package gocbcore

import (
	"encoding/binary"
)

type memdClient struct {
	conn        memdConn
	opList      memdOpMap
	errorMap    *kvErrorMap
	closeNotify chan bool
	dcpAckSize  int
	dcpFlowRecv int
}

func newMemdClient(conn memdConn) *memdClient {
	client := memdClient{
		conn:        conn,
		closeNotify: make(chan bool),
	}
	client.run()
	return &client
}

func (client *memdClient) EnableDcpBufferAck(bufferAckSize int) {
	client.dcpAckSize = bufferAckSize
}

func (client *memdClient) maybeSendDcpBufferAck(packet *memdPacket) {
	packetLen := 24 + len(packet.Extras) + len(packet.Key) + len(packet.Value)

	client.dcpFlowRecv += packetLen
	if client.dcpFlowRecv < client.dcpAckSize {
		return
	}

	ackAmt := client.dcpFlowRecv

	extrasBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extrasBuf, uint32(ackAmt))

	err := client.conn.WritePacket(&memdPacket{
		Magic:  reqMagic,
		Opcode: cmdDcpBufferAck,
		Extras: extrasBuf,
	})
	if err != nil {
		logWarnf("Failed to dispatch DCP buffer ack: %s", err)
	}

	client.dcpFlowRecv -= ackAmt
}

func (client *memdClient) SetErrorMap(errorMap *kvErrorMap) {
	client.errorMap = errorMap
}

func (client *memdClient) Address() string {
	return client.conn.RemoteAddr()
}

func (client *memdClient) CloseNotify() chan bool {
	return client.closeNotify
}

func (client *memdClient) SendRequest(req *memdQRequest) error {
	client.opList.Add(req)

	err := client.conn.WritePacket(&req.memdPacket)
	if err != nil {
		logDebugf("memdClient write failure: %v", err)
		client.opList.Remove(req)
		return err
	}

	return nil
}

func (client *memdClient) resolveRequest(resp *memdQResponse) {
	opIndex := resp.Opaque

	// Find the request that goes with this response
	req := client.opList.FindAndMaybeRemove(opIndex)

	if req == nil {
		// There is no known request that goes with this response.  Ignore it.
		logDebugf("Received response with no corresponding request.")
		return
	}

	if req.RoutingCallback != nil {
		if req.RoutingCallback(resp, req) {
			logSchedf("Routing callback intercepted response")
			return
		}
	}

	// Grab an error object if one needs to exist.
	var err error
	if resp.Magic == resMagic {
		err = getMemdError(resp.Status, client.errorMap)
	}

	// Call the requests callback handler...
	logSchedf("Dispatching response callback. OP=0x%x. Opaque=%d", resp.Opcode, resp.Opaque)
	req.tryCallback(resp, err)
}

func (client *memdClient) run() {
	dcpBufferQ := make(chan *memdQResponse)
	killSwitch := make(chan bool, 1)
	go func() {
		for {
			select {
			case resp, more := <-dcpBufferQ:
				if !more {
					return
				}

				logSchedf("Resolving response OP=0x%x. Opaque=%d", resp.Opcode, resp.Opaque)
				client.resolveRequest(resp)

				if client.dcpAckSize > 0 {
					client.maybeSendDcpBufferAck(&resp.memdPacket)
				}
			case <-killSwitch:
				close(dcpBufferQ)
			}
		}
	}()

	go func() {
		for {
			resp := &memdQResponse{
				sourceAddr: client.conn.RemoteAddr(),
			}

			err := client.conn.ReadPacket(&resp.memdPacket)
			if err != nil {
				logErrorf("memdClient read failure: %v", err)
				break
			}

			// We handle DCP no-op's directly here so we can reply immediately.
			if resp.memdPacket.Opcode == cmdDcpNoop {
				err := client.conn.WritePacket(&memdPacket{
					Magic:  resMagic,
					Opcode: cmdDcpNoop,
				})
				if err != nil {
					logWarnf("Failed to dispatch DCP noop reply: %s", err)
				}
				continue
			}

			switch resp.memdPacket.Opcode {
			case cmdDcpDeletion:
				fallthrough
			case cmdDcpExpiration:
				fallthrough
			case cmdDcpMutation:
				fallthrough
			case cmdDcpSnapshotMarker:
				fallthrough
			case cmdDcpStreamEnd:
				fallthrough
			case cmdDcpStreamReq:
				dcpBufferQ <- resp
				continue
			default:
				logSchedf("Resolving response OP=0x%x. Opaque=%d", resp.Opcode, resp.Opaque)
				client.resolveRequest(resp)
			}
		}

		err := client.conn.Close()
		if err != nil {
			// Lets log an error, as this is non-fatal
			logErrorf("Failed to shut down client connection (%s)", err)
		}

		client.opList.Drain(func(req *memdQRequest) {
			req.tryCallback(nil, ErrNetwork)
		})

		killSwitch <- true
		client.closeNotify <- true
	}()
}

func (client *memdClient) Close() error {
	return client.conn.Close()
}
