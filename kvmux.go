package gocbcore

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/gocbcore/v10/memd"
)

type bucketCapabilityVerifier interface {
	HasBucketCapabilityStatus(cap BucketCapability, status BucketCapabilityStatus) bool
}

type dispatcher interface {
	DispatchDirect(req *memdQRequest) (PendingOp, error)
	RequeueDirect(req *memdQRequest, isRetry bool)
	DispatchDirectToAddress(req *memdQRequest, pipeline *memdPipeline) (PendingOp, error)
	CollectionsEnabled() bool
	SupportsCollections() bool
	SetPostCompleteErrorHandler(handler postCompleteErrorHandler)
	PipelineSnapshot() (*pipelineSnapshot, error)
}

type kvMux struct {
	muxPtr unsafe.Pointer

	collectionsEnabled bool
	queueSize          int
	poolSize           int
	cfgMgr             *configManagementComponent
	errMapMgr          *errMapComponent

	tracer *tracerComponent
	dialer *memdClientDialerComponent

	postCompleteErrHandler postCompleteErrorHandler

	// muxStateWriteLock is necessary for functions which update the muxPtr, due to the scenario where ForceReconnect and
	// OnNewRouteConfig could race. ForceReconnect must succeed and cannot fail because OnNewRouteConfig has updated
	// the mux state whilst force is attempting to update it. We could also end up in a situation where a full reconnect
	// is occurring at the same time as a pipeline takeover and scenarios like that, including missing a config update because
	// ForceReconnect has won the race.
	// There is no need for read side locks as we are locking around an atomic and it is only the write sides that present
	// a potential issue.
	muxStateWriteLock sync.Mutex

	shutdownSig   chan struct{}
	clientCloseWg sync.WaitGroup

	noTLSSeedNode bool

	hasSeenConfigCh chan struct{}
}

type kvMuxProps struct {
	CollectionsEnabled bool
	QueueSize          int
	PoolSize           int
	NoTLSSeedNode      bool
}

func newKVMux(props kvMuxProps, cfgMgr *configManagementComponent, errMapMgr *errMapComponent, tracer *tracerComponent,
	dialer *memdClientDialerComponent, muxState *kvMuxState) *kvMux {
	mux := &kvMux{
		queueSize:          props.QueueSize,
		poolSize:           props.PoolSize,
		collectionsEnabled: props.CollectionsEnabled,
		cfgMgr:             cfgMgr,
		errMapMgr:          errMapMgr,
		tracer:             tracer,
		dialer:             dialer,
		shutdownSig:        make(chan struct{}),
		noTLSSeedNode:      props.NoTLSSeedNode,
		muxPtr:             unsafe.Pointer(muxState),
		hasSeenConfigCh:    make(chan struct{}),
	}

	cfgMgr.AddConfigWatcher(mux)

	return mux
}

func (mux *kvMux) getState() *kvMuxState {
	muxPtr := atomic.LoadPointer(&mux.muxPtr)
	if muxPtr == nil {
		return nil
	}

	return (*kvMuxState)(muxPtr)
}

func (mux *kvMux) updateState(old, new *kvMuxState) bool {
	if new == nil {
		logErrorf("Attempted to update to nil kvMuxState")
		return false
	}

	if old != nil {
		return atomic.CompareAndSwapPointer(&mux.muxPtr, unsafe.Pointer(old), unsafe.Pointer(new))
	}

	if atomic.SwapPointer(&mux.muxPtr, unsafe.Pointer(new)) != nil {
		logErrorf("Updated from nil attempted on initialized kvMuxState")
		return false
	}

	return true
}

func (mux *kvMux) clear() *kvMuxState {
	mux.muxStateWriteLock.Lock()
	val := atomic.SwapPointer(&mux.muxPtr, nil)
	mux.muxStateWriteLock.Unlock()
	return (*kvMuxState)(val)
}

func (mux *kvMux) OnNewRouteConfig(cfg *routeConfig) {
	mux.muxStateWriteLock.Lock()
	defer mux.muxStateWriteLock.Unlock()
	oldMuxState := mux.getState()
	newMuxState := mux.newKVMuxState(cfg, oldMuxState.tlsConfig, oldMuxState.authMechanisms, oldMuxState.auth)

	// Attempt to atomically update the routing data
	if !mux.updateState(oldMuxState, newMuxState) {
		logWarnf("Someone preempted the config update, skipping update")
		return
	}

	if oldMuxState.RevID() == -1 && newMuxState.RevID() > -1 {
		if mux.collectionsEnabled && !newMuxState.collectionsSupported {
			logDebugf("Collections disabled as unsupported")
		}

		close(mux.hasSeenConfigCh)
	}

	if !mux.collectionsEnabled {
		// If collections just aren't enabled then we never need to refresh the connections because collections
		// have come online.
		mux.pipelineTakeover(oldMuxState, newMuxState)
	} else if oldMuxState.RevID() == -1 || oldMuxState.collectionsSupported == newMuxState.collectionsSupported {
		// Get the new muxer to takeover the pipelines from the older one
		mux.pipelineTakeover(oldMuxState, newMuxState)
	} else {
		// Collections support has changed so we need to reconnect all connections in order to support the new
		// state.
		mux.reconnectPipelines(oldMuxState, newMuxState, true)
	}

	mux.requeueRequests(oldMuxState)
}

func (mux *kvMux) SetPostCompleteErrorHandler(handler postCompleteErrorHandler) {
	mux.postCompleteErrHandler = handler
}

func (mux *kvMux) ConfigRev() (int64, error) {
	clientMux := mux.getState()
	if clientMux == nil {
		return 0, errShutdown
	}
	return clientMux.RevID(), nil
}

func (mux *kvMux) ConfigUUID() string {
	clientMux := mux.getState()
	if clientMux == nil {
		return ""
	}
	return clientMux.UUID()
}

func (mux *kvMux) KeyToVbucket(key []byte) (uint16, error) {
	clientMux := mux.getState()
	if clientMux == nil || clientMux.VBMap() == nil {
		return 0, errShutdown
	}

	return clientMux.VBMap().VbucketByKey(key), nil
}

func (mux *kvMux) NumReplicas() int {
	clientMux := mux.getState()
	if clientMux == nil {
		return 0
	}

	if clientMux.VBMap() == nil {
		return 0
	}

	return clientMux.VBMap().NumReplicas()
}

func (mux *kvMux) BucketType() bucketType {
	clientMux := mux.getState()
	if clientMux == nil {
		return bktTypeInvalid
	}

	return clientMux.BucketType()
}

func (mux *kvMux) SupportsGCCCP() bool {
	clientMux := mux.getState()
	if clientMux == nil {
		return false
	}

	return clientMux.BucketType() == bktTypeNone
}

func (mux *kvMux) NumPipelines() int {
	clientMux := mux.getState()
	if clientMux == nil {
		return 0
	}

	return clientMux.NumPipelines()
}

// CollectionsEnaled returns whether or not the kv mux was created with collections enabled.
func (mux *kvMux) CollectionsEnabled() bool {
	return mux.collectionsEnabled
}

func (mux *kvMux) IsSecure() bool {
	return mux.getState().tlsConfig != nil
}

// SupportsCollections returns whether or not collections are enabled AND supported by the server.
func (mux *kvMux) SupportsCollections() bool {
	if !mux.collectionsEnabled {
		return false
	}

	clientMux := mux.getState()
	if clientMux == nil {
		return false
	}

	return clientMux.collectionsSupported
}

func (mux *kvMux) HasBucketCapabilityStatus(cap BucketCapability, status BucketCapabilityStatus) bool {
	clientMux := mux.getState()
	if clientMux == nil {
		return status == BucketCapabilityStatusUnknown
	}

	return clientMux.HasBucketCapabilityStatus(cap, status)
}

func (mux *kvMux) BucketCapabilityStatus(cap BucketCapability) BucketCapabilityStatus {
	clientMux := mux.getState()
	if clientMux == nil || clientMux.RevID() == -1 {
		return BucketCapabilityStatusUnknown
	}

	return clientMux.BucketCapabilityStatus(cap)
}

func (mux *kvMux) RouteRequest(req *memdQRequest) (*memdPipeline, error) {
	clientMux := mux.getState()
	if clientMux == nil {
		return nil, errShutdown
	}

	// We haven't seen a valid config yet so put this in the dead pipeline so
	// it'll get requeued once we do get a config.
	if clientMux.RevID() == -1 {
		return clientMux.deadPipe, nil
	}

	var srvIdx int
	repIdx := req.ReplicaIdx

	// Route to specific server
	if repIdx < 0 {
		srvIdx = -repIdx - 1
	} else {
		var err error

		bktType := clientMux.BucketType()
		if bktType == bktTypeCouchbase {
			if req.Key != nil {
				req.Vbucket = clientMux.VBMap().VbucketByKey(req.Key)
			}

			srvIdx, err = clientMux.VBMap().NodeByVbucket(req.Vbucket, uint32(repIdx))

			if err != nil {
				return nil, err
			}
		} else if bktType == bktTypeMemcached {
			if repIdx > 0 {
				// Error. Memcached buckets don't understand replicas!
				return nil, errInvalidReplica
			}

			if len(req.Key) == 0 {
				// Non-broadcast keyless Memcached bucket request
				return nil, errInvalidArgument
			}

			srvIdx, err = clientMux.KetamaMap().NodeByKey(req.Key)
			if err != nil {
				return nil, err
			}
		} else if bktType == bktTypeNone {
			// This means that we're using GCCCP and not connected to a bucket
			return nil, errGCCCPInUse
		}
	}

	return clientMux.GetPipeline(srvIdx), nil
}

func (mux *kvMux) DispatchDirect(req *memdQRequest) (PendingOp, error) {
	mux.tracer.StartCmdTrace(req)
	req.dispatchTime = time.Now()

	for {
		pipeline, err := mux.RouteRequest(req)
		if err != nil {
			return nil, err
		}

		err = pipeline.SendRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err != nil {
			if err == errPipelineFull {
				err = errOverload
			}

			shortCircuit, routeErr := mux.handleOpRoutingResp(nil, req, err)
			if shortCircuit {
				return req, nil
			}

			return nil, routeErr
		}

		break
	}

	return req, nil
}

func (mux *kvMux) RequeueDirect(req *memdQRequest, isRetry bool) {
	mux.tracer.StartCmdTrace(req)

	handleError := func(err error) {
		// We only want to log an error on retries if the error isn't cancelled.
		if !isRetry || (isRetry && !errors.Is(err, ErrRequestCanceled)) {
			logErrorf("Reschedule failed, failing request (%s)", err)
		}

		req.tryCallback(nil, err)
	}

	logDebugf("Request being requeued, Opaque=%d, Opcode=0x%x", req.Opaque, req.Command)

	for {
		pipeline, err := mux.RouteRequest(req)
		if err != nil {
			handleError(err)
			return
		}

		err = pipeline.RequeueRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err != nil {
			handleError(err)
			return
		}

		break
	}
}

func (mux *kvMux) DispatchDirectToAddress(req *memdQRequest, pipeline *memdPipeline) (PendingOp, error) {
	mux.tracer.StartCmdTrace(req)
	req.dispatchTime = time.Now()

	// We set the ReplicaIdx to a negative number to ensure it is not redispatched
	// and we check that it was 0 to begin with to ensure it wasn't miss-used.
	if req.ReplicaIdx != 0 {
		return nil, errInvalidReplica
	}
	req.ReplicaIdx = -999999999

	for {
		err := pipeline.SendRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err != nil {
			if err == errPipelineFull {
				err = errOverload
			}

			shortCircuit, routeErr := mux.handleOpRoutingResp(nil, req, err)
			if shortCircuit {
				return req, nil
			}

			return nil, routeErr
		}

		break
	}

	return req, nil
}

func (mux *kvMux) Close() error {
	mux.cfgMgr.RemoveConfigWatcher(mux)
	clientMux := mux.clear()

	if clientMux == nil {
		return errShutdown
	}

	// Trigger any memdclients that are in graceful close to forcibly close.
	close(mux.shutdownSig)

	var muxErr error
	// Shut down the client multiplexer which will close all its queues
	// effectively causing all the clients to shut down.
	for _, pipeline := range clientMux.pipelines {
		err := pipeline.Close()
		if err != nil {
			logErrorf("failed to shut down pipeline: %s", err)
			muxErr = errCliInternalError
		}
	}

	if clientMux.deadPipe != nil {
		err := clientMux.deadPipe.Close()
		if err != nil {
			logErrorf("failed to shut down deadpipe: %s", err)
			muxErr = errCliInternalError
		}
	}

	// Drain all the pipelines and error their requests, then
	//  drain the dead queue and error those requests.
	cb := func(req *memdQRequest) {
		req.tryCallback(nil, errShutdown)
	}

	mux.drainPipelines(clientMux, cb)

	mux.clientCloseWg.Wait()

	return muxErr
}

func (mux *kvMux) ForceReconnect(tlsConfig *dynTLSConfig, authMechanisms []AuthMechanism, auth AuthProvider,
	reconnectLocal bool) {
	logDebugf("Forcing reconnect of all connections")
	mux.muxStateWriteLock.Lock()
	muxState := mux.getState()
	newMuxState := mux.newKVMuxState(muxState.RouteConfig(), tlsConfig, authMechanisms, auth)

	atomic.SwapPointer(&mux.muxPtr, unsafe.Pointer(newMuxState))

	mux.reconnectPipelines(muxState, newMuxState, reconnectLocal)
	mux.muxStateWriteLock.Unlock()
}

func (mux *kvMux) PipelineSnapshot() (*pipelineSnapshot, error) {
	clientMux := mux.getState()
	if clientMux == nil {
		return nil, errShutdown
	}

	return &pipelineSnapshot{
		state: clientMux,
	}, nil
}

type waitForConfigSnapshotOp struct {
	cancelCh chan struct{}
}

func (w *waitForConfigSnapshotOp) Cancel() {
	close(w.cancelCh)
}

func (mux *kvMux) WaitForConfigSnapshot(deadline time.Time, cb WaitForConfigSnapshotCallback) (PendingOp, error) {
	// No point in doing anything if we're shutdown.
	clientMux := mux.getState()
	if clientMux == nil {
		return nil, errShutdown
	}

	op := &waitForConfigSnapshotOp{
		cancelCh: make(chan struct{}),
	}

	start := time.Now()
	go func() {
		select {
		case <-mux.shutdownSig:
			cb(nil, errShutdown)
		case <-op.cancelCh:
			cb(nil, errRequestCanceled)
		case <-time.After(time.Until(deadline)):
			cb(nil, &TimeoutError{
				InnerError:   errUnambiguousTimeout,
				OperationID:  "WaitForConfigSnapshot",
				TimeObserved: time.Since(start),
			})
		case <-mux.hasSeenConfigCh:
			// Just in case.
			clientMux := mux.getState()
			if clientMux == nil {
				cb(nil, errShutdown)
				return
			}

			cb(&WaitForConfigSnapshotResult{
				Snapshot: &ConfigSnapshot{
					state: clientMux,
				},
			}, nil)
		}
	}()

	return op, nil
}

func (mux *kvMux) ConfigSnapshot() (*ConfigSnapshot, error) {
	clientMux := mux.getState()
	if clientMux == nil {
		return nil, errShutdown
	}

	return &ConfigSnapshot{
		state: clientMux,
	}, nil
}

func (mux *kvMux) handleOpRoutingResp(resp *memdQResponse, req *memdQRequest, originalErr error) (bool, error) {
	// If there is no error, we should return immediately
	if originalErr == nil {
		return false, nil
	}

	// If this operation has been cancelled, we just fail immediately.
	if errors.Is(originalErr, ErrRequestCanceled) || errors.Is(originalErr, ErrTimeout) {
		return false, originalErr
	}

	err := translateMemdError(originalErr, req)

	if err == originalErr {
		if errors.Is(err, io.EOF) && !mux.closed() {
			// The connection has gone away.
			if req.Command == memd.CmdGetClusterConfig {
				return false, err
			}

			// If the request is idempotent or not written yet then we should retry.
			if req.Idempotent() || req.ConnectionInfo().lastDispatchedTo == "" {
				if mux.waitAndRetryOperation(req, SocketNotAvailableRetryReason) {
					return true, nil
				}
			}
		} else if errors.Is(err, ErrMemdClientClosed) && !mux.closed() {
			if req.Command == memd.CmdGetClusterConfig {
				return false, err
			}

			// The request can't have been dispatched yet.
			if mux.waitAndRetryOperation(req, SocketNotAvailableRetryReason) {
				return true, nil
			}
		} else if errors.Is(err, io.ErrShortWrite) {
			// This is a special case where the write has failed on the underlying connection and not all the bytes
			// were written to the network.
			if mux.waitAndRetryOperation(req, MemdWriteFailure) {
				return true, nil
			}
		} else if resp != nil && resp.Magic == memd.CmdMagicRes {
			// We don't know anything about this error so send it to the error map
			shouldRetry := mux.errMapMgr.ShouldRetry(resp.Status)
			if shouldRetry {
				if mux.waitAndRetryOperation(req, KVErrMapRetryReason) {
					return true, nil
				}
			}
		}
	} else {
		// Handle potentially retrying the operation
		if errors.Is(err, ErrNotMyVBucket) {
			if mux.handleNotMyVbucket(resp, req) {
				return true, nil
			}
		} else if errors.Is(err, ErrDocumentLocked) {
			if mux.waitAndRetryOperation(req, KVLockedRetryReason) {
				return true, nil
			}
		} else if errors.Is(err, ErrTemporaryFailure) {
			if mux.waitAndRetryOperation(req, KVTemporaryFailureRetryReason) {
				return true, nil
			}
		} else if errors.Is(err, ErrDurableWriteInProgress) {
			if mux.waitAndRetryOperation(req, KVSyncWriteInProgressRetryReason) {
				return true, nil
			}
		} else if errors.Is(err, ErrDurableWriteReCommitInProgress) {
			if mux.waitAndRetryOperation(req, KVSyncWriteRecommitInProgressRetryReason) {
				return true, nil
			}
		}
		// If an error isn't in this list then we know what this error is but we don't support retries for it.
	}

	err = mux.errMapMgr.EnhanceKvError(err, resp, req)

	if mux.postCompleteErrHandler == nil {
		return false, err
	}

	return mux.postCompleteErrHandler(resp, req, err)
}

func (mux *kvMux) closed() bool {
	return mux.getState() == nil
}

func (mux *kvMux) waitAndRetryOperation(req *memdQRequest, reason RetryReason) bool {
	shouldRetry, retryTime := retryOrchMaybeRetry(req, reason)
	if shouldRetry {
		go func() {
			time.Sleep(time.Until(retryTime))
			mux.RequeueDirect(req, true)
		}()
		return true
	}

	return false
}

func (mux *kvMux) handleNotMyVbucket(resp *memdQResponse, req *memdQRequest) bool {
	// Grab just the hostname from the source address
	sourceHost, err := hostFromHostPort(resp.sourceAddr)
	if err != nil {
		logErrorf("NMV response source address was invalid, skipping config update")
	} else {
		// Try to parse the value as a bucket configuration
		bk, err := parseConfig(resp.Value, sourceHost)
		if err == nil {
			// We need to push this upstream which will then update us with a new config.
			mux.cfgMgr.OnNewConfig(bk)
		}
	}

	if req.Command == memd.CmdRangeScanContinue {
		// For range scan continue we never want to retry, the range scan is now invalid.
		return false
	}

	// Redirect it!  This may actually come back to this server, but I won't tell
	//   if you don't ;)
	return mux.waitAndRetryOperation(req, KVNotMyVBucketRetryReason)
}

func (mux *kvMux) drainPipelines(clientMux *kvMuxState, cb func(req *memdQRequest)) {
	for _, pipeline := range clientMux.pipelines {
		logDebugf("Draining queue %+v", pipeline)
		pipeline.Drain(cb)
	}
	if clientMux.deadPipe != nil {
		clientMux.deadPipe.Drain(cb)
	}
}

func (mux *kvMux) newKVMuxState(cfg *routeConfig, tlsConfig *dynTLSConfig, authMechanisms []AuthMechanism,
	auth AuthProvider) *kvMuxState {
	poolSize := 1
	if !cfg.IsGCCCPConfig() {
		poolSize = mux.poolSize
	}

	useTls := tlsConfig != nil

	var kvServerList []routeEndpoint
	if mux.noTLSSeedNode {
		// The order of the kv server list matters, so we need to maintain the same order and just replace the seed
		// node.
		if useTls {
			kvServerList = make([]routeEndpoint, len(cfg.kvServerList.SSLEndpoints))
			copy(kvServerList, cfg.kvServerList.SSLEndpoints)

			for i, ep := range cfg.kvServerList.NonSSLEndpoints {
				if ep.IsSeedNode {
					kvServerList[i] = ep
				}
			}
		} else {
			kvServerList = cfg.kvServerList.NonSSLEndpoints
		}
	} else {
		if useTls {
			kvServerList = cfg.kvServerList.SSLEndpoints
		} else {
			kvServerList = cfg.kvServerList.NonSSLEndpoints
		}
	}

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintln("KV muxer applying endpoints:"))
	buffer.WriteString(fmt.Sprintf("Bucket: %s\n", cfg.name))
	for _, ep := range kvServerList {
		buffer.WriteString(fmt.Sprintf("  - %s\n", ep.Address))
	}

	logDebugf(buffer.String())

	pipelines := make([]*memdPipeline, len(kvServerList))
	for i, hostPort := range kvServerList {
		trimmedHostPort := routeEndpoint{
			Address:    trimSchemePrefix(hostPort.Address),
			IsSeedNode: hostPort.IsSeedNode,
		}

		getCurClientFn := func(cancelSig <-chan struct{}) (*memdClient, error) {
			return mux.dialer.SlowDialMemdClient(cancelSig, trimmedHostPort, tlsConfig, auth, authMechanisms, mux.handleOpRoutingResp)
		}
		pipeline := newPipeline(trimmedHostPort, poolSize, mux.queueSize, getCurClientFn)

		pipelines[i] = pipeline
	}

	return newKVMuxState(cfg, kvServerList, tlsConfig, authMechanisms, auth, pipelines,
		newDeadPipeline(mux.queueSize))
}

func (mux *kvMux) reconnectPipelines(oldMuxState *kvMuxState, newMuxState *kvMuxState, reconnectSeed bool) {
	oldPipelines := list.New()

	for _, pipeline := range oldMuxState.pipelines {
		oldPipelines.PushBack(pipeline)
	}

	for _, pipeline := range newMuxState.pipelines {
		// If we aren't reconnecting the seed node then we need to take its clients and make sure we don't
		// end up closing it down.
		if pipeline.isSeedNode && !reconnectSeed {
			oldPipeline := mux.stealPipeline(pipeline.Address(), oldPipelines)

			if oldPipeline != nil {
				pipeline.Takeover(oldPipeline)
			}
		}

		pipeline.StartClients()
	}

	for e := oldPipelines.Front(); e != nil; e = e.Next() {
		pipeline, ok := e.Value.(*memdPipeline)
		if !ok {
			logErrorf("Failed to cast old pipeline")
			continue
		}

		clients := pipeline.GracefulClose()

		for _, client := range clients {
			mux.closeMemdClient(client, errForcedReconnect)
		}
	}
}

func (mux *kvMux) requeueRequests(oldMuxState *kvMuxState) {
	// Gather all the requests from all the old pipelines and then
	//  sort and redispatch them (which will use the new pipelines)
	var requestList []*memdQRequest
	mux.drainPipelines(oldMuxState, func(req *memdQRequest) {
		requestList = append(requestList, req)
	})

	sort.Sort(memdQRequestSorter(requestList))

	for _, req := range requestList {
		stopCmdTrace(req)
		mux.RequeueDirect(req, false)
	}
}

// closeMemdClient will gracefully close the memdclient, spinning up a goroutine to watch for when the client
// shuts down. The error provided is the error sent to any callback handlers for persistent operations which are
// currently live in the client.
func (mux *kvMux) closeMemdClient(client *memdClient, err error) {
	mux.clientCloseWg.Add(1)
	client.GracefulClose(err)
	go func(client *memdClient) {
		select {
		case <-client.CloseNotify():
			logDebugf("Memdclient %s/%p completed graceful shutdown", client.Address(), client)
		case <-mux.shutdownSig:
			logDebugf("Memdclient %s/%p being forcibly shutdown", client.Address(), client)
			// Force the client to close even if there are requests in flight.
			err := client.Close()
			if err != nil {
				logErrorf("failed to shutdown memdclient: %s", err)
			}
			<-client.CloseNotify()
			logDebugf("Memdclient %s/%p completed shutdown", client.Address(), client)
		}
		mux.clientCloseWg.Done()
	}(client)
}

func (mux *kvMux) stealPipeline(address string, oldPipelines *list.List) *memdPipeline {
	for e := oldPipelines.Front(); e != nil; e = e.Next() {
		pipeline, ok := e.Value.(*memdPipeline)
		if !ok {
			logErrorf("Failed to cast old pipeline")
			continue
		}

		if pipeline.Address() == address {
			oldPipelines.Remove(e)
			return pipeline
		}
	}

	return nil
}

func (mux *kvMux) pipelineTakeover(oldMux, newMux *kvMuxState) {
	oldPipelines := list.New()

	// Gather all our old pipelines up for takeover and what not
	if oldMux != nil {
		for _, pipeline := range oldMux.pipelines {
			oldPipelines.PushBack(pipeline)
		}
	}

	// Initialize new pipelines (possibly with a takeover)
	for _, pipeline := range newMux.pipelines {
		oldPipeline := mux.stealPipeline(pipeline.Address(), oldPipelines)
		if oldPipeline != nil {
			pipeline.Takeover(oldPipeline)
		}

		pipeline.StartClients()
	}

	// Shut down any pipelines that were not taken over
	for e := oldPipelines.Front(); e != nil; e = e.Next() {
		pipeline, ok := e.Value.(*memdPipeline)
		if !ok {
			logErrorf("Failed to cast old pipeline")
			continue
		}

		clients := pipeline.GracefulClose()
		for _, client := range clients {
			mux.closeMemdClient(client, nil)
		}
	}

	if oldMux != nil && oldMux.deadPipe != nil {
		err := oldMux.deadPipe.Close()
		if err != nil {
			logErrorf("Failed to properly close abandoned dead pipe (%s)", err)
		}
	}
}
