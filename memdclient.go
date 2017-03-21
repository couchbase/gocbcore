package gocbcore

type memdClient struct {
	conn        memdConn
	opList      memdOpMap
	closeNotify chan bool
}

func newMemdClient(conn memdConn) *memdClient {
	client := memdClient{
		conn:        conn,
		closeNotify: make(chan bool),
	}
	client.run()
	return &client
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
		err = getMemdError(resp.Status)
	}

	// Call the requests callback handler...
	logSchedf("Dispatching response callback. OP=0x%x. Opaque=%d", resp.Opcode, resp.Opaque)
	req.tryCallback(resp, err)
}

func (client *memdClient) run() {
	go func() {
		for {
			resp := memdQResponse{
				sourceAddr: client.conn.RemoteAddr(),
			}

			err := client.conn.ReadPacket(&resp.memdPacket)
			if err != nil {
				logErrorf("memdClient read failure: %v", err)
				break
			}

			logSchedf("Resolving response OP=0x%x. Opaque=%d", resp.Opcode, resp.Opaque)
			client.resolveRequest(&resp)
		}

		err := client.conn.Close()
		if err != nil {
			// Lets log an error, as this is non-fatal
			logErrorf("Failed to shut down client connection (%s)", err)
		}

		client.opList.Drain(func(req *memdQRequest) {
			req.tryCallback(nil, ErrNetwork)
		})

		client.closeNotify <- true
	}()
}

func (client *memdClient) Close() error {
	return client.conn.Close()
}
