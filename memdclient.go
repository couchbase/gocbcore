package gocbcore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/gocbcore/v10/memd"

	"github.com/golang/snappy"
)

func isCompressibleOp(command memd.CmdCode) bool {
	switch command {
	case memd.CmdSet:
		fallthrough
	case memd.CmdAdd:
		fallthrough
	case memd.CmdReplace:
		fallthrough
	case memd.CmdAppend:
		fallthrough
	case memd.CmdPrepend:
		return true
	}
	return false
}

type postCompleteErrorHandler func(resp *memdQResponse, req *memdQRequest, err error) (bool, error)
type serverRequestHandler func(pak *memd.Packet)

type memdClient struct {
	lastActivity          int64
	dcpAckSize            int
	dcpFlowRecv           int
	closeNotify           chan bool
	connReleaseNotify     chan struct{}
	connReleasedNotify    chan struct{}
	connID                string
	closed                bool
	conn                  memdConn
	opList                *memdOpMap
	opTombstones          *memdOpTombstoneStore
	features              []memd.HelloFeature
	lock                  sync.Mutex
	streamEndNotSupported bool
	breaker               circuitBreaker
	postErrHandler        postCompleteErrorHandler
	serverRequestHandler  serverRequestHandler
	tracer                *tracerComponent
	zombieLogger          *zombieLoggerComponent
	telemetry             *telemetryComponent

	dcpQueueSize int

	// When a close request comes in, we need to immediately stop processing all requests.  This
	// includes immediately stopping the DCP queue rather than waiting for the application to
	// flush that queue.  This means that we lose packets that were read but not processed, but
	// this is not fundamentally different to if we had just not read them at all.  As a side
	// effect of this, we need to use a separate kill signal on top of closing the queue.
	// We need this to be owned by the client because we only use it when the client is closed,
	// when the connection is closed from an external actor (e.g. server) we want to flush the queue.
	shutdownDCP uint32

	compressionMinSize   int
	compressionMinRatio  float64
	disableDecompression bool

	telemetryAttrs atomic.Pointer[memdClientTelemetryAttrs]

	gracefulCloseTriggered uint32
}

type memdClientTelemetryAttrs struct {
	nodeUUID string
	node     string
	altNode  string
}

type dcpBuffer struct {
	resp       *memdQResponse
	packetLen  int
	isInternal bool
}

type memdClientProps struct {
	ClientID         string
	CanonicalAddress string
	NodeUUID         string

	DCPQueueSize         int
	CompressionMinSize   int
	CompressionMinRatio  float64
	DisableDecompression bool
}

func newMemdClient(props memdClientProps, conn memdConn, breakerCfg CircuitBreakerConfig, postErrHandler postCompleteErrorHandler,
	tracer *tracerComponent, zombieLogger *zombieLoggerComponent, telemetry *telemetryComponent, serverRequestHandler serverRequestHandler) *memdClient {
	client := memdClient{
		closeNotify:          make(chan bool),
		connReleaseNotify:    make(chan struct{}),
		connReleasedNotify:   make(chan struct{}),
		connID:               props.ClientID + "/" + formatCbUID(randomCbUID()),
		postErrHandler:       postErrHandler,
		serverRequestHandler: serverRequestHandler,
		tracer:               tracer,
		zombieLogger:         zombieLogger,
		telemetry:            telemetry,
		conn:                 conn,
		opList:               newMemdOpMap(),

		dcpQueueSize:         props.DCPQueueSize,
		compressionMinRatio:  props.CompressionMinRatio,
		compressionMinSize:   props.CompressionMinSize,
		disableDecompression: props.DisableDecompression,
	}

	client.UpdateTelemetryAttributes(props.NodeUUID, props.CanonicalAddress)

	if client.zombieLogger != nil {
		client.opTombstones = newMemdOpTombstoneStore()
	}

	if breakerCfg.Enabled {
		client.breaker = newLazyCircuitBreaker(breakerCfg, client.sendCanary)
	} else {
		client.breaker = newNoopCircuitBreaker()
	}

	client.run()
	return &client
}

func (client *memdClient) SupportsFeature(feature memd.HelloFeature) bool {
	return checkSupportsFeature(client.features, feature)
}

// Features must be set from a context where no racey behaviours can occur, i.e. during bootstrap.
func (client *memdClient) Features(features []memd.HelloFeature) {
	client.features = features

	for _, feature := range features {
		client.conn.EnableFeature(feature)
	}
}

func (client *memdClient) EnableDcpBufferAck(bufferAckSize int) {
	client.dcpAckSize = bufferAckSize
}

func (client *memdClient) maybeSendDcpBufferAck(packetLen int) {
	client.dcpFlowRecv += packetLen
	if client.dcpFlowRecv < client.dcpAckSize {
		return
	}

	ackAmt := client.dcpFlowRecv

	extrasBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extrasBuf, uint32(ackAmt))

	err := client.conn.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicReq,
		Command: memd.CmdDcpBufferAck,
		Extras:  extrasBuf,
	})
	if err != nil {
		logWarnf("%p memdclient failed to dispatch DCP buffer ack: %s", client, err)
	}

	client.dcpFlowRecv -= ackAmt
}

func (client *memdClient) Address() string {
	return client.conn.RemoteAddr()
}

func (client *memdClient) ConnID() string {
	return client.connID
}

func (client *memdClient) CloseNotify() chan bool {
	return client.closeNotify
}

func (client *memdClient) UpdateTelemetryAttributes(nodeUUID, canonicalAddress string) {
	var node, altNode string
	if canonicalAddress != "" && canonicalAddress != client.Address() {
		var err error
		node, err = hostFromHostPort(canonicalAddress)
		if err != nil {
			node = canonicalAddress
		}
		altNode, err = hostFromHostPort(client.Address())
		if err != nil {
			altNode = client.Address()
		}
	} else {
		var err error
		node, err = hostFromHostPort(client.Address())
		if err != nil {
			node = client.Address()
		}
	}
	client.telemetryAttrs.Store(&memdClientTelemetryAttrs{
		nodeUUID: nodeUUID,
		node:     node,
		altNode:  altNode,
	})
}

func (client *memdClient) takeRequestOwnership(req *memdQRequest) error {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.closed {
		logDebugf("%s memdclient attempted to put dispatched op OP=0x%x, Opaque=%d in drained opmap", client.loggerID(), req.Command, req.Opaque)
		return errMemdClientClosed
	}

	if atomic.LoadUint32(&client.gracefulCloseTriggered) == 1 {
		logDebugf("%s memdclient attempted to dispatch op OP=0x%x, Opaque=%d from gracefully closing memdclient", client.loggerID(), req.Command, req.Opaque)
		return errMemdClientClosed
	}

	if !atomic.CompareAndSwapPointer(&req.waitingIn, nil, unsafe.Pointer(client)) {
		logDebugf("%s memdclient attempted to put dispatched op OP=0x%x, Opaque=%d in new opmap", client.loggerID(), req.Command, req.Opaque)
		return errRequestAlreadyDispatched
	}

	if req.isCancelled() {
		atomic.CompareAndSwapPointer(&req.waitingIn, unsafe.Pointer(client), nil)
		return errRequestCanceled
	}

	req.SetConnectionInfo(memdQRequestConnInfo{
		lastDispatchedTo:   client.Address(),
		lastDispatchedFrom: client.conn.LocalAddr(),
		lastConnectionID:   client.connID,
		lastDispatchedAt:   time.Now(),
	})

	client.opList.Add(req)
	return nil
}

func (client *memdClient) CancelRequest(req *memdQRequest, err error) bool {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.closed {
		logDebugf("%s memdclient attempted to remove op OP=0x%x, Opaque=%d from drained opmap", client.loggerID(), req.Command, req.Opaque)
		return false
	}

	removed := client.opList.Remove(req)
	if removed {
		atomic.CompareAndSwapPointer(&req.waitingIn, unsafe.Pointer(client), nil)
	}

	if client.opTombstones != nil {
		connInfo := req.ConnectionInfo()

		tombstoneEvicted := client.opTombstones.Add(req.Opaque, &memdOpTombstone{
			totalServerDuration: req.totalServerDuration,
			dispatchTime:        req.dispatchTime,
			lastAttemptTime:     connInfo.lastDispatchedAt,
			isDurable:           req.DurabilityLevelFrame != nil && req.DurabilityLevelFrame.DurabilityLevel != memd.DurabilityLevel(0),
			command:             req.Command,
		})

		if tombstoneEvicted && client.zombieLogger != nil {
			client.zombieLogger.RecordTombstoneEviction()
		}
	}

	client.recordTelemetry(req, err)

	if client.breaker.CompletionCallback(err) {
		client.breaker.MarkSuccessful()
	} else {
		client.breaker.MarkFailure()
	}

	return removed
}

func (client *memdClient) SendRequest(req *memdQRequest) error {
	if !client.breaker.AllowsRequest() {
		logSchedf("Circuit breaker interrupting request. %s to %s OP=0x%x. Opaque=%d", client.conn.LocalAddr(), client.Address(), req.Command, req.Opaque)

		req.cancelWithCallback(errCircuitBreakerOpen)

		return nil
	}

	return client.internalSendRequest(req)
}

func (client *memdClient) internalSendRequest(req *memdQRequest) error {
	if err := client.takeRequestOwnership(req); err != nil {
		return err
	}

	packet := &req.Packet
	if client.SupportsFeature(memd.FeatureSnappy) {
		isCompressed := (packet.Datatype & uint8(memd.DatatypeFlagCompressed)) != 0
		packetSize := len(packet.Value)
		if !isCompressed && packetSize > client.compressionMinSize && isCompressibleOp(packet.Command) {
			compressedValue := snappy.Encode(nil, packet.Value)
			if float64(len(compressedValue))/float64(packetSize) <= client.compressionMinRatio {
				newPacket := *packet
				newPacket.Value = compressedValue
				newPacket.Datatype = newPacket.Datatype | uint8(memd.DatatypeFlagCompressed)
				packet = &newPacket
			}
		}
	}

	logSchedf("Writing request. %s to %s OP=0x%x. Opaque=%d. Vbid=%d", client.conn.LocalAddr(), client.loggerID(), req.Command, req.Opaque, req.Vbucket)

	client.tracer.StartNetTrace(req)

	err := client.conn.WritePacket(packet)
	if err != nil {
		logDebugf(" %s memdclient write failure: %v", client.loggerID(), err)
		return err
	}

	return nil
}

func (client *memdClient) classifyResponseStatusClass(status memd.StatusCode) statusClass {
	switch status {
	case memd.StatusSuccess:
		return statusClassOK
	case memd.StatusRangeScanMore:
		return statusClassOK
	case memd.StatusRangeScanComplete:
		return statusClassOK
	default:
		return statusClassError
	}
}

func (client *memdClient) resolveRequest(resp *memdQResponse) {
	defer memd.ReleasePacket(resp.Packet)

	if resp.Magic == memd.CmdMagicServerReq {
		logSchedf("Handling server request data on %s. OP=0x%x", client.loggerID(), resp.Command)
		client.serverRequestHandler(resp.Packet)
		return
	}

	logSchedf("Handling response data on %s. OP=0x%x. Opaque=%d. Status:%d", client.loggerID(), resp.Command, resp.Opaque, resp.Status)

	stClass := client.classifyResponseStatusClass(resp.Status)

	client.lock.Lock()
	// Find the request that goes with this response, don't check if the client is
	// closed so that we can handle orphaned responses.
	req := client.opList.FindAndMaybeRemove(resp.Opaque, stClass == statusClassError)
	client.lock.Unlock()

	if atomic.LoadUint32(&client.gracefulCloseTriggered) == 1 {
		client.lock.Lock()
		size := client.opList.Size()
		client.lock.Unlock()

		if size == 0 {
			// Let's make sure that we don't somehow slow down returning to the user here.
			go func() {
				// We use the Close function rather than closeConn to ensure that we don't try to close the
				// connection/client if someone else has already closed it.
				err := client.Close()
				if err != nil {
					logDebugf("Failed to shutdown memdclient (%s) during graceful close: %s", client.loggerID(), err)
				}
			}()
		}
	}

	if req == nil {
		// There is no known request that goes with this response.  Ignore it.
		logDebugf("%s memdclient received response with no corresponding request. OP=0x%x. Opaque=%d. Status:%d", client.loggerID(), resp.Command, resp.Opaque, resp.Status)
		if client.opTombstones == nil {
			return
		}
		client.lock.Lock()
		tombstone := client.opTombstones.FindAndMaybeRemove(resp.Opaque)
		if tombstone == nil {
			client.lock.Unlock()
			logDebugf("%s memdclient received unexpected orphaned response with no corresponding cancellation metadata. OP=0x%x. Opaque=%d. Status:%d", client.loggerID(), resp.Command, resp.Opaque, resp.Status)
			return
		}
		client.lock.Unlock()
		if client.zombieLogger != nil {
			client.zombieLogger.RecordZombieResponse(tombstone, resp, client.connID, client.LocalAddress(), client.Address())
		}
		client.recordTelemetryForOrphan(tombstone)
		return
	}

	if !req.Persistent || stClass == statusClassError {
		atomic.CompareAndSwapPointer(&req.waitingIn, unsafe.Pointer(client), nil)
	}

	req.processingLock.Lock()

	req.AddResourceUnits(resp.ReadUnitsFrame, resp.WriteUnitsFrame)

	if !req.Persistent {
		stopNetTraceLocked(req, resp, client.conn.LocalAddr(), client.conn.RemoteAddr())

		if resp.ServerDurationFrame != nil {
			req.recordServerDurationLocked(resp.ServerDurationFrame.ServerDuration)
		}

		client.recordTelemetry(req, nil)
	}

	isCompressed := (resp.Datatype & uint8(memd.DatatypeFlagCompressed)) != 0
	// We always want to decompress cluster configs if they've been compressed.
	alwaysDecompress := req.Command == memd.CmdGetClusterConfig || resp.Status == memd.StatusNotMyVBucket
	if isCompressed && (!client.disableDecompression || alwaysDecompress) {
		newValue, err := snappy.Decode(nil, resp.Value)
		if err != nil {
			req.processingLock.Unlock()
			logDebugf("%s memdclient failed to decompress value from the server for key `%s`.", client.loggerID(), req.Key)
			return
		}

		resp.Value = newValue
		resp.Datatype = resp.Datatype & ^uint8(memd.DatatypeFlagCompressed)
	}

	// Give the agent an opportunity to intercept the response first
	var err error
	if resp.Magic == memd.CmdMagicRes && stClass == statusClassError {
		err = getKvStatusCodeError(resp.Status)
	}

	if client.breaker.CompletionCallback(err) {
		client.breaker.MarkSuccessful()
	} else {
		client.breaker.MarkFailure()
	}

	if !req.Persistent {
		stopCmdTraceLocked(req)
	}

	req.processingLock.Unlock()

	if err != nil {
		shortCircuited, routeErr := client.postErrHandler(resp, req, err)
		if shortCircuited {
			logSchedf("Routing callback intercepted response")
			return
		}
		err = routeErr
	}

	// Call the requests callback handler...
	logSchedf("Dispatching response callback. OP=0x%x. Opaque=%d", resp.Command, resp.Opaque)
	req.tryCallback(resp, err)
}

func (client *memdClient) run() {
	var (
		// A queue for DCP commands so we can execute them out-of-band from packet receiving.  This
		// is integral to allow the higher level application to back-pressure against the DCP packet
		// processing without interfeering with the SDKs control commands (like config fetch).
		dcpBufferQ = make(chan *dcpBuffer, client.dcpQueueSize)

		// After we signal that DCP processing should stop, we need a notification so we know when
		// it has been completed, we do this to prevent leaving the goroutine around, and we need to
		// ensure that the application has finished with the last packet it received before we stop.
		dcpProcDoneCh = make(chan struct{})
	)

	go func() {
		defer close(dcpProcDoneCh)

		for {
			// If the client has been told to close then we need to finish ASAP, otherwise if the dcpBufferQ has been
			// closed then we'll flush the queue first.
			q, stillOpen := <-dcpBufferQ
			if !stillOpen || atomic.LoadUint32(&client.shutdownDCP) != 0 {
				return
			}

			logSchedf("Resolving response OP=0x%x. Opaque=%d", q.resp.Command, q.resp.Opaque)
			client.resolveRequest(q.resp)

			// See below for information on MB-26363 for why this is here.
			if !q.isInternal && client.dcpAckSize > 0 {
				client.maybeSendDcpBufferAck(q.packetLen)
			}
		}
	}()

	go func() {
		for {
			packet, n, err := client.conn.ReadPacket()
			if err != nil {
				client.lock.Lock()
				if !client.closed {
					logWarnf("%p memdClient read failure on conn `%v` : %v", client, client.connID, err)
				}
				client.lock.Unlock()
				break
			}

			resp := &memdQResponse{
				remoteAddr:   client.conn.LocalAddr(),
				sourceAddr:   client.conn.RemoteAddr(),
				sourceConnID: client.connID,
				Packet:       packet,
			}

			atomic.StoreInt64(&client.lastActivity, time.Now().UnixNano())

			// We handle DCP no-op's directly here so we can reply immediately.
			if resp.Packet.Command == memd.CmdDcpNoop {
				err := client.conn.WritePacket(&memd.Packet{
					Magic:   memd.CmdMagicRes,
					Command: memd.CmdDcpNoop,
					Opaque:  resp.Opaque,
				})
				if err != nil {
					logWarnf("%p memdclient failed to dispatch DCP noop reply: %s", client, err)
				}
				continue
			}

			// This is a fix for a bug in the server DCP implementation (MB-26363).  This
			// bug causes the server to fail to send a stream-end notification.  The server
			// does however synchronously stop the stream, and thus we can assume no more
			// packets will be received following the close response.
			if resp.Magic == memd.CmdMagicRes && resp.Command == memd.CmdDcpCloseStream && client.streamEndNotSupported {
				closeReq := client.opList.Find(resp.Opaque)
				if closeReq != nil {
					vbID := closeReq.Vbucket
					streamReq := client.opList.FindOpenStream(vbID)
					if streamReq != nil {
						endExtras := make([]byte, 4)
						binary.BigEndian.PutUint32(endExtras, uint32(memd.StreamEndClosed))
						endResp := &memdQResponse{
							Packet: &memd.Packet{
								Magic:   memd.CmdMagicReq,
								Command: memd.CmdDcpStreamEnd,
								Vbucket: vbID,
								Opaque:  streamReq.Opaque,
								Extras:  endExtras,
							},
						}
						dcpBufferQ <- &dcpBuffer{
							resp:       endResp,
							packetLen:  n,
							isInternal: true,
						}
					}
				}
			}

			switch resp.Packet.Command {
			case memd.CmdDcpDeletion, memd.CmdDcpExpiration, memd.CmdDcpMutation, memd.CmdDcpSnapshotMarker,
				memd.CmdDcpEvent, memd.CmdDcpOsoSnapshot, memd.CmdDcpSeqNoAdvanced, memd.CmdDcpStreamEnd:
				dcpBufferQ <- &dcpBuffer{
					resp:      resp,
					packetLen: n,
				}
			default:
				logSchedf("%s memdclient resolving response OP=0x%x. Opaque=%d", client.loggerID(), resp.Command, resp.Opaque)
				client.resolveRequest(resp)
			}
		}

		client.lock.Lock()
		if !client.closed {
			client.closed = true
			client.lock.Unlock()

			err := client.closeConn(true)
			if err != nil {
				// Lets log a warning, as this is non-fatal
				logWarnf("Failed to shut down client (%p) connection (%s)", client, err)
			}
		} else {
			client.lock.Unlock()
		}

		// We close the buffer channel to wake the processor if its asleep (queue was empty).
		// We then wait to ensure it is finished with whatever packet (or packets if the connection was closed by the
		// server) was being processed.
		close(dcpBufferQ)
		<-dcpProcDoneCh

		close(client.connReleaseNotify)

		client.opList.Drain(func(req *memdQRequest) {
			if !atomic.CompareAndSwapPointer(&req.waitingIn, unsafe.Pointer(client), nil) {
				logWarnf("Encountered an unowned request in a client (%p) opMap", client)
			}

			shortCircuited, routeErr := client.postErrHandler(nil, req, io.EOF)
			if shortCircuited {
				return
			}

			req.tryCallback(nil, routeErr)
		})

		<-client.connReleasedNotify

		close(client.closeNotify)
	}()
}

func (client *memdClient) LocalAddress() string {
	return client.conn.LocalAddr()
}

func (client *memdClient) GracefulClose(err error) {
	if atomic.CompareAndSwapUint32(&client.gracefulCloseTriggered, 0, 1) {
		client.lock.Lock()
		if client.closed {
			client.lock.Unlock()
			return
		}
		persistentReqs := client.opList.FindAndRemoveAllPersistent()
		client.lock.Unlock()

		if err == nil {
			err = io.EOF
		}

		for _, req := range persistentReqs {
			req.cancelWithCallback(err)
		}

		// Close down the DCP worker, there can't be any future DCP messages. We don't
		// strictly need to do this, as connection close will trigger it to close anyway.
		atomic.StoreUint32(&client.shutdownDCP, 1)

		client.lock.Lock()
		size := client.opList.Size()
		if size > 0 {
			// If there are items in the op list then we need to go into graceful shutdown mode, so don't close anything
			// yet.
			client.lock.Unlock()
			return
		}

		// If there are no items in the oplist then it's safe to close down the client and connection now.
		if client.closed {
			client.lock.Unlock()
			return
		}
		client.closed = true
		client.lock.Unlock()

		err := client.closeConn(false)
		if err != nil {
			// Lets log a warning, as this is non-fatal
			logWarnf("Failed to shut down client (%p) connection (%s)", client, err)
		}

	}
}

func (client *memdClient) closeConn(internalTrigger bool) error {
	logDebugf("%s memdclient closing connection, internal close: %t", client.loggerID(), internalTrigger)
	err := client.conn.Close()
	if err != nil {
		logDebugf("Failed to close memdconn: %v on memdclient %s", err, client.loggerID())
	}

	// If this has been triggered by the read side failing a read before the client is closed then we
	// can be certain that we aren't going to attempt a read, and it's safe to release the connection.
	// Otherwise, we need to wait for the connection close to propagate through the read side and to be told
	// that reading has stopped so we can safely release.
	if !internalTrigger {
		<-client.connReleaseNotify
	}

	client.conn.Release()
	close(client.connReleasedNotify)
	return err
}

func (client *memdClient) Close() error {
	// We mark that we are shutting down to stop the DCP processor from running any
	// additional packets up to the application. We do this before the closed check to
	// force stop flushing. Rebalance etc... uses GracefulClose so if we received this Close
	// then we do need to shutdown in a timely manner.
	atomic.StoreUint32(&client.shutdownDCP, 1)

	client.lock.Lock()
	if client.closed {
		client.lock.Unlock()
		return nil
	}
	client.closed = true
	client.lock.Unlock()

	return client.closeConn(false)
}

func (client *memdClient) sendCanary() {
	errChan := make(chan error)
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		errChan <- err
	}

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdNoop,
			Datatype: 0,
			Cas:      0,
			Key:      nil,
			Value:    nil,
		},
		Callback:      handler,
		RetryStrategy: newFailFastRetryStrategy(),
	}

	logDebugf("Sending NOOP request for %s", client.loggerID())
	err := client.internalSendRequest(req)
	if err != nil {
		client.breaker.MarkFailure()
	}

	timer := AcquireTimer(client.breaker.CanaryTimeout())
	select {
	case <-timer.C:
		if !req.internalCancel(errRequestCanceled) {
			err := <-errChan
			if err == nil {
				logDebugf("NOOP request successful for %s", client.loggerID())
				client.breaker.MarkSuccessful()
			} else {
				logDebugf("NOOP request failed for %s", client.loggerID())
				client.breaker.MarkFailure()
			}
		}
		client.breaker.MarkFailure()
	case err := <-errChan:
		if err == nil {
			client.breaker.MarkSuccessful()
		} else {
			client.breaker.MarkFailure()
		}
	}
}

func (client *memdClient) recordTelemetry(req *memdQRequest, err error) {
	if !client.telemetry.TelemetryEnabled() {
		return
	}

	category := req.Command.Category()
	if category == memd.CmdCategoryUnknown {
		return
	}
	outcome := telemetryOutcomeSuccess
	if err != nil {
		if errors.Is(err, ErrRequestCanceled) {
			outcome = telemetryOutcomeCanceled
		} else if errors.Is(err, ErrTimeout) {
			outcome = telemetryOutcomeTimedout
		} else {
			outcome = telemetryOutcomeError
		}
	}

	clientAttrs := client.telemetryAttrs.Load()
	connInfo := req.ConnectionInfo()

	client.telemetry.RecordOp(outcome, telemetryOperationAttributes{
		duration: time.Since(connInfo.lastDispatchedAt),
		nodeUUID: clientAttrs.nodeUUID,
		node:     clientAttrs.node,
		altNode:  clientAttrs.altNode,
		service:  MemdService,
		durable:  req.DurabilityLevelFrame != nil && req.DurabilityLevelFrame.DurabilityLevel != memd.DurabilityLevel(0),
		mutation: category == memd.CmdCategoryMutation,
	})
}

func (client *memdClient) recordTelemetryForOrphan(tombstone *memdOpTombstone) {
	if !client.telemetry.TelemetryEnabled() {
		return
	}

	category := tombstone.command.Category()
	if category == memd.CmdCategoryUnknown {
		return
	}

	clientAttrs := client.telemetryAttrs.Load()

	client.telemetry.RecordOrphanedResponse(telemetryOperationAttributes{
		duration: time.Since(tombstone.lastAttemptTime),
		nodeUUID: clientAttrs.nodeUUID,
		node:     clientAttrs.node,
		altNode:  clientAttrs.altNode,
		service:  MemdService,
		durable:  tombstone.isDurable,
		mutation: category == memd.CmdCategoryMutation,
	})
}

func (client *memdClient) loggerID() string {
	return fmt.Sprintf("%s/%p", client.Address(), client)
}
