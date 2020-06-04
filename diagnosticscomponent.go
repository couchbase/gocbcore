package gocbcore

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v9/memd"
)

type diagnosticsComponent struct {
	kvMux         *kvMux
	httpMux       *httpMux
	httpComponent *httpComponent
	bucket        string
}

func newDiagnosticsComponent(kvMux *kvMux, httpMux *httpMux, httpComponent *httpComponent, bucket string) *diagnosticsComponent {
	return &diagnosticsComponent{
		kvMux:         kvMux,
		httpMux:       httpMux,
		bucket:        bucket,
		httpComponent: httpComponent,
	}
}

func (dc *diagnosticsComponent) pingKV(ctx context.Context, interval time.Duration, deadline time.Time,
	retryStrat RetryStrategy, op *pingOp) {

	if !deadline.IsZero() {
		// We have to setup a new child context with its own deadline because services have their own timeout values.
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	for {
		iter, err := dc.kvMux.PipelineSnapshot()
		if err != nil {
			logErrorf("failed to get pipeline snapshot")

			select {
			case <-ctx.Done():
				ctxErr := ctx.Err()
				var cancelReason error
				if errors.Is(ctxErr, context.Canceled) {
					cancelReason = ctxErr
				} else {
					cancelReason = errUnambiguousTimeout
				}

				op.results[MemdService] = append(op.results[MemdService], EndpointPingResult{
					Error: cancelReason,
					Scope: op.bucketName,
					ID:    uuid.New().String(),
					State: PingStateTimeout,
				})
				op.handledOneLocked(iter.RevID())
				return
			case <-time.After(interval):
				continue
			}
		}

		if iter.RevID() > -1 {
			var wg sync.WaitGroup
			iter.Iterate(0, func(p *memdPipeline) bool {
				wg.Add(1)
				go func(pipeline *memdPipeline) {
					serverAddress := pipeline.Address()

					startTime := time.Now()
					handler := func(resp *memdQResponse, req *memdQRequest, err error) {
						pingLatency := time.Now().Sub(startTime)

						state := PingStateOK
						if err != nil {
							if errors.Is(err, ErrTimeout) {
								state = PingStateTimeout
							} else {
								state = PingStateError
							}
						}

						op.lock.Lock()
						op.results[MemdService] = append(op.results[MemdService], EndpointPingResult{
							Endpoint: serverAddress,
							Error:    err,
							Latency:  pingLatency,
							Scope:    op.bucketName,
							ID:       fmt.Sprintf("%p", pipeline),
							State:    state,
						})
						op.lock.Unlock()
						wg.Done()
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
						RetryStrategy: retryStrat,
					}

					curOp, err := dc.kvMux.DispatchDirectToAddress(req, pipeline)
					if err != nil {
						op.lock.Lock()
						op.results[MemdService] = append(op.results[MemdService], EndpointPingResult{
							Endpoint: redactSystemData(serverAddress),
							Error:    err,
							Latency:  0,
							Scope:    op.bucketName,
						})
						op.lock.Unlock()
						wg.Done()
						return
					}

					if !deadline.IsZero() {
						start := time.Now()
						req.SetTimer(time.AfterFunc(deadline.Sub(start), func() {
							connInfo := req.ConnectionInfo()
							count, reasons := req.Retries()
							req.cancelWithCallback(&TimeoutError{
								InnerError:         errUnambiguousTimeout,
								OperationID:        "PingKV",
								Opaque:             req.Identifier(),
								TimeObserved:       time.Now().Sub(start),
								RetryReasons:       reasons,
								RetryAttempts:      count,
								LastDispatchedTo:   connInfo.lastDispatchedTo,
								LastDispatchedFrom: connInfo.lastDispatchedFrom,
								LastConnectionID:   connInfo.lastConnectionID,
							})
						}))
					}

					op.lock.Lock()
					op.subops = append(op.subops, pingSubOp{
						endpoint: serverAddress,
						op:       curOp,
					})
					op.lock.Unlock()
				}(p)

				// We iterate through all pipelines
				return false
			})

			wg.Wait()
			op.lock.Lock()
			op.handledOneLocked(iter.RevID())
			op.lock.Unlock()
			return
		}

		select {
		case <-ctx.Done():
			ctxErr := ctx.Err()
			var cancelReason error
			if errors.Is(ctxErr, context.Canceled) {
				cancelReason = ctxErr
			} else {
				cancelReason = errUnambiguousTimeout
			}

			op.lock.Lock()
			op.results[MemdService] = append(op.results[MemdService], EndpointPingResult{
				Error: cancelReason,
				Scope: op.bucketName,
				ID:    uuid.New().String(),
				State: PingStateTimeout,
			})
			op.handledOneLocked(iter.RevID())
			op.lock.Unlock()
			return
		case <-time.After(interval):
		}
	}
}

func (dc *diagnosticsComponent) pingHTTP(ctx context.Context, service ServiceType,
	interval time.Duration, deadline time.Time, retryStrat RetryStrategy, op *pingOp, ignoreMissingServices bool) {

	if !deadline.IsZero() {
		// We have to setup a new child context with its own deadline because services have their own timeout values.
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	muxer := dc.httpMux

	var path string
	switch service {
	case N1qlService:
		path = "/admin/ping"
	case CbasService:
		path = "/admin/ping"
	case FtsService:
		path = "/api/ping"
	case CapiService:
		path = "/"
	}

	for {
		clientMux := muxer.Get()
		if clientMux.revID > -1 {
			var epList []string
			switch service {
			case N1qlService:
				epList = clientMux.n1qlEpList
			case CbasService:
				epList = clientMux.cbasEpList
			case FtsService:
				epList = clientMux.ftsEpList
			case MgmtService:
				epList = clientMux.mgmtEpList
			case CapiService:
				epList = dc.endpointsFromCapiList(clientMux.capiEpList)
			}

			if len(epList) == 0 {
				op.lock.Lock()
				if !ignoreMissingServices {
					op.results[service] = append(op.results[service], EndpointPingResult{
						Error: errServiceNotAvailable,
						Scope: op.bucketName,
						ID:    uuid.New().String(),
					})
				}
				op.handledOneLocked(clientMux.revID)
				op.lock.Unlock()
				return
			}

			var wg sync.WaitGroup
			for _, ep := range epList {
				wg.Add(1)
				go func(ep string) {
					defer wg.Done()
					req := &httpRequest{
						Service:       service,
						Method:        "GET",
						Path:          path,
						Endpoint:      ep,
						IsIdempotent:  true,
						RetryStrategy: retryStrat,
						Context:       ctx,
						UniqueID:      uuid.New().String(),
					}
					start := time.Now()
					resp, err := dc.httpComponent.DoInternalHTTPRequest(req, false)
					pingLatency := time.Now().Sub(start)
					state := PingStateOK
					if err != nil {
						if errors.Is(err, ErrTimeout) {
							state = PingStateTimeout
						} else {
							state = PingStateError
						}
					} else {
						if resp.StatusCode > 200 {
							state = PingStateError
							b, pErr := ioutil.ReadAll(resp.Body)
							if pErr != nil {
								logDebugf("Failed to read response body for ping: %v", pErr)
							}

							err = errors.New(string(b))
						}
					}
					op.lock.Lock()
					op.results[service] = append(op.results[service], EndpointPingResult{
						Endpoint: ep,
						Error:    err,
						Latency:  pingLatency,
						Scope:    op.bucketName,
						ID:       uuid.New().String(),
						State:    state,
					})
					op.lock.Unlock()
				}(ep)
			}

			wg.Wait()
			op.lock.Lock()
			op.handledOneLocked(clientMux.revID)
			op.lock.Unlock()
			return
		}

		select {
		case <-ctx.Done():
			ctxErr := ctx.Err()
			var cancelReason error
			if errors.Is(ctxErr, context.Canceled) {
				cancelReason = ctxErr
			} else {
				cancelReason = errUnambiguousTimeout
			}

			op.lock.Lock()
			op.results[service] = append(op.results[service], EndpointPingResult{
				Error: cancelReason,
				Scope: op.bucketName,
				ID:    uuid.New().String(),
				State: PingStateTimeout,
			})
			op.handledOneLocked(clientMux.revID)
			op.lock.Unlock()
			return
		case <-time.After(interval):
		}
	}
}

func (dc *diagnosticsComponent) Ping(opts PingOptions, cb PingCallback) (PendingOp, error) {
	bucketName := ""
	if dc.bucket != "" {
		bucketName = redactMetaData(dc.bucket)
	}

	ignoreMissingServices := false
	serviceTypes := opts.ServiceTypes
	if len(serviceTypes) == 0 {
		// We're defaulting to pinging what we can so don't ping anything that isn't in the cluster config
		ignoreMissingServices = true
		serviceTypes = []ServiceType{MemdService, CapiService, N1qlService, FtsService, CbasService, MgmtService}
	}

	ignoreMissingServices = ignoreMissingServices || opts.ignoreMissingServices

	ctx, cancelFunc := context.WithCancel(context.Background())

	op := &pingOp{
		callback:   cb,
		remaining:  int32(len(serviceTypes)),
		results:    make(map[ServiceType][]EndpointPingResult),
		bucketName: bucketName,
		httpCancel: cancelFunc,
	}

	retryStrat := newFailFastRetryStrategy()

	// interval is how long to wait between checking if we've seen a cluster config
	interval := 10 * time.Millisecond

	for _, serviceType := range serviceTypes {
		switch serviceType {
		case MemdService:
			go dc.pingKV(ctx, interval, opts.KVDeadline, retryStrat, op)
		case CapiService:
			go dc.pingHTTP(ctx, CapiService, interval, opts.CapiDeadline, retryStrat, op, ignoreMissingServices)
		case N1qlService:
			go dc.pingHTTP(ctx, N1qlService, interval, opts.N1QLDeadline, retryStrat, op, ignoreMissingServices)
		case FtsService:
			go dc.pingHTTP(ctx, FtsService, interval, opts.FtsDeadline, retryStrat, op, ignoreMissingServices)
		case CbasService:
			go dc.pingHTTP(ctx, CbasService, interval, opts.CbasDeadline, retryStrat, op, ignoreMissingServices)
		case MgmtService:
			go dc.pingHTTP(ctx, MgmtService, interval, opts.MgmtDeadline, retryStrat, op, ignoreMissingServices)
		}
	}

	return op, nil
}

func (dc *diagnosticsComponent) endpointsFromCapiList(capiEpList []string) []string {
	var epList []string
	for _, ep := range capiEpList {
		epList = append(epList, strings.TrimRight(ep, "/"+dc.bucket))
	}

	return epList
}

// Diagnostics returns diagnostics information about the client.
// Mainly containing a list of open connections and their current
// states.
func (dc *diagnosticsComponent) Diagnostics(opts DiagnosticsOptions) (*DiagnosticInfo, error) {
	for {
		iter, err := dc.kvMux.PipelineSnapshot()
		if err != nil {
			return nil, err
		}

		var conns []MemdConnInfo

		iter.Iterate(0, func(pipeline *memdPipeline) bool {
			pipeline.clientsLock.Lock()
			for _, pipecli := range pipeline.clients {
				localAddr := ""
				remoteAddr := ""
				var lastActivity time.Time

				pipecli.lock.Lock()
				if pipecli.client != nil {
					localAddr = pipecli.client.LocalAddress()
					remoteAddr = pipecli.client.Address()
					lastActivityUs := atomic.LoadInt64(&pipecli.client.lastActivity)
					if lastActivityUs != 0 {
						lastActivity = time.Unix(0, lastActivityUs)
					}
				}
				pipecli.lock.Unlock()

				conn := MemdConnInfo{
					LocalAddr:    localAddr,
					RemoteAddr:   remoteAddr,
					LastActivity: lastActivity,
					ID:           fmt.Sprintf("%p", pipecli),
					State:        pipecli.State(),
				}
				if dc.bucket != "" {
					conn.Scope = redactMetaData(dc.bucket)
				}
				conns = append(conns, conn)
			}
			pipeline.clientsLock.Unlock()
			return false
		})

		expected := len(conns)
		connected := 0
		for _, conn := range conns {
			if conn.State == EndpointStateConnected {
				connected++
			}
		}

		state := ClusterStateOffline
		if connected == expected {
			state = ClusterStateOnline
		} else if connected > 1 {
			state = ClusterStateDegraded
		}

		endIter, err := dc.kvMux.PipelineSnapshot()
		if err != nil {
			return nil, err
		}
		if iter.RevID() == endIter.RevID() {
			return &DiagnosticInfo{
				ConfigRev: iter.RevID(),
				MemdConns: conns,
				State:     state,
			}, nil
		}
	}
}

func (dc *diagnosticsComponent) checkKVReady(interval time.Duration, desiredState ClusterState,
	op *waitUntilOp) {
	for {
		iter, err := dc.kvMux.PipelineSnapshot()
		if err != nil {
			logErrorf("failed to get pipeline snapshot")

			select {
			case <-op.stopCh:
				return
			case <-time.After(interval):
				continue
			}
		}

		if iter.RevID() > -1 {
			expected := 0
			connected := 0
			iter.Iterate(0, func(pipeline *memdPipeline) bool {
				pipeline.clientsLock.Lock()
				defer pipeline.clientsLock.Unlock()
				expected += pipeline.maxClients
				for _, cli := range pipeline.clients {
					state := cli.State()
					if state == EndpointStateConnected {
						connected++
						if desiredState == ClusterStateDegraded {
							// If we're after degraded state then we can just bail early as we've already fulfilled that.
							return true
						}
					} else if desiredState == ClusterStateOnline {
						// If we're after online state then we can just bail early as we've already failed to fulfill that.
						return true
					}
				}

				return false
			})

			switch desiredState {
			case ClusterStateDegraded:
				if connected > 0 {
					op.lock.Lock()
					op.handledOneLocked()
					op.lock.Unlock()

					return
				}
			case ClusterStateOnline:
				if connected == expected {
					op.lock.Lock()
					op.handledOneLocked()
					op.lock.Unlock()

					return
				}
			default:
				// How we got here no-one does know
				// But round and round we must go
			}
		}

		select {
		case <-op.stopCh:
			return
		case <-time.After(interval):
		}
	}
}

func (dc *diagnosticsComponent) checkHTTPReady(ctx context.Context, service ServiceType,
	interval time.Duration, desiredState ClusterState, op *waitUntilOp) {
	retryStrat := &failFastRetryStrategy{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	muxer := dc.httpMux

	var path string
	switch service {
	case N1qlService:
		path = "/admin/ping"
	case CbasService:
		path = "/admin/ping"
	case FtsService:
		path = "/api/ping"
	case CapiService:
		path = "/"
	}

	for {
		clientMux := muxer.Get()
		if clientMux.revID > -1 {
			var epList []string
			switch service {
			case N1qlService:
				epList = clientMux.n1qlEpList
			case CbasService:
				epList = clientMux.cbasEpList
			case FtsService:
				epList = clientMux.ftsEpList
			case CapiService:
				epList = dc.endpointsFromCapiList(clientMux.capiEpList)
			}

			connected := uint32(0)
			var wg sync.WaitGroup
			for _, ep := range epList {
				wg.Add(1)
				go func(ep string) {
					defer wg.Done()
					req := &httpRequest{
						Service:       service,
						Method:        "GET",
						Path:          path,
						RetryStrategy: retryStrat,
						Endpoint:      ep,
						IsIdempotent:  true,
						Context:       ctx,
						UniqueID:      uuid.New().String(),
					}
					resp, err := dc.httpComponent.DoInternalHTTPRequest(req, false)
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}

						if desiredState == ClusterStateOnline {
							// Cancel this run entirely, we can't satisfy the requirements
							cancel()
						}
						return
					}
					if resp.StatusCode != 200 {
						if desiredState == ClusterStateOnline {
							// Cancel this run entirely, we can't satisfy the requirements
							cancel()
						}
						return
					}
					atomic.AddUint32(&connected, 1)
					if desiredState == ClusterStateDegraded {
						// Cancel this run entirely, we've successfully satisfied the requirements
						cancel()
					}
				}(ep)
			}

			wg.Wait()

			switch desiredState {
			case ClusterStateDegraded:
				if atomic.LoadUint32(&connected) > 0 {
					op.lock.Lock()
					op.handledOneLocked()
					op.lock.Unlock()

					return
				}
			case ClusterStateOnline:
				if atomic.LoadUint32(&connected) == uint32(len(epList)) {
					op.lock.Lock()
					op.handledOneLocked()
					op.lock.Unlock()

					return
				}
			default:
				// How we got here no-one does know
				// But round and round we must go
			}
		}

		select {
		case <-op.stopCh:
			return
		case <-time.After(interval):
		}
	}
}

func (dc *diagnosticsComponent) WaitUntilReady(deadline time.Time, opts WaitUntilReadyOptions,
	cb WaitUntilReadyCallback) (PendingOp, error) {
	desiredState := opts.DesiredState
	if desiredState == ClusterStateOffline {
		return nil, wrapError(errInvalidArgument, "cannot use offline as a desired state")
	}

	if desiredState == 0 {
		desiredState = ClusterStateOnline
	}

	serviceTypes := opts.ServiceTypes
	if len(serviceTypes) == 0 {
		serviceTypes = []ServiceType{MemdService}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	op := &waitUntilOp{
		remaining:  int32(len(serviceTypes)),
		stopCh:     make(chan struct{}),
		callback:   cb,
		httpCancel: cancelFunc,
	}

	op.lock.Lock()
	start := time.Now()
	op.timer = time.AfterFunc(deadline.Sub(start), func() {
		op.cancel(&TimeoutError{
			InnerError:   errUnambiguousTimeout,
			OperationID:  "WaitUntilReady",
			TimeObserved: time.Now().Sub(start),
		})
	})
	op.lock.Unlock()

	interval := 10 * time.Millisecond

	for _, serviceType := range serviceTypes {
		switch serviceType {
		case MemdService:
			go dc.checkKVReady(interval, desiredState, op)
		case CapiService:
			go dc.checkHTTPReady(ctx, CapiService, interval, desiredState, op)
		case N1qlService:
			go dc.checkHTTPReady(ctx, N1qlService, interval, desiredState, op)
		case FtsService:
			go dc.checkHTTPReady(ctx, FtsService, interval, desiredState, op)
		case CbasService:
			go dc.checkHTTPReady(ctx, CbasService, interval, desiredState, op)
		}
	}

	return op, nil
}
