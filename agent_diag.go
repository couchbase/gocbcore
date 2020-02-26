package gocbcore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PingResult contains the results of a ping to a single server.
type PingResult struct {
	Endpoint string
	Error    error
	Latency  time.Duration
	ID       string
	Scope    string
}

type pingSubOp struct {
	op       PendingOp
	endpoint string
}

type pingOp struct {
	lock      sync.Mutex
	subops    []pingSubOp
	remaining int32
	results   []PingResult
	callback  PingKvExCallback
	configRev int64
}

func (pop *pingOp) Cancel(err error) {
	for _, subop := range pop.subops {
		subop.op.Cancel(err)
	}
}

func (pop *pingOp) handledOneLocked() {
	remaining := atomic.AddInt32(&pop.remaining, -1)
	if remaining == 0 {
		pop.callback(&PingKvResult{
			ConfigRev: pop.configRev,
			Services:  pop.results,
		}, nil)
	}
}

// PingKvOptions encapsulates the parameters for a PingKvEx operation.
type PingKvOptions struct {
	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// PingKvResult encapsulates the result of a PingKvEx operation.
type PingKvResult struct {
	ConfigRev int64
	Services  []PingResult
}

// PingKvExCallback is invoked upon completion of a PingKvEx operation.
type PingKvExCallback func(*PingKvResult, error)

// PingKvEx pings all of the servers we are connected to and returns
// a report regarding the pings that were performed.
func (agent *Agent) PingKvEx(opts PingKvOptions, cb PingKvExCallback) (PendingOp, error) {
	clientMux := agent.kvMux.GetState()
	if clientMux == nil {
		return nil, errShutdown
	}

	op := &pingOp{
		callback:  cb,
		remaining: 1,
		configRev: clientMux.revID,
	}

	pingStartTime := time.Now()

	bucketName := ""
	if agent.bucketName != "" {
		bucketName = redactMetaData(agent.bucketName)
	}

	addrToID := make(map[string]string)

	kvHandler := func(resp *memdQResponse, req *memdQRequest, err error) {
		serverAddress := resp.sourceAddr

		pingLatency := time.Now().Sub(pingStartTime)

		op.lock.Lock()
		id := addrToID[serverAddress]
		op.results = append(op.results, PingResult{
			Endpoint: serverAddress,
			Error:    err,
			Latency:  pingLatency,
			Scope:    bucketName,
			ID:       id,
		})
		op.handledOneLocked()
		op.lock.Unlock()
	}

	retryStrat := newFailFastRetryStrategy()

	for serverIdx := 0; serverIdx < clientMux.NumPipelines(); serverIdx++ {
		pipeline := clientMux.GetPipeline(serverIdx)
		serverAddress := pipeline.Address()

		req := &memdQRequest{
			memdPacket: memdPacket{
				Magic:    reqMagic,
				Opcode:   cmdNoop,
				Datatype: 0,
				Cas:      0,
				Key:      nil,
				Value:    nil,
			},
			Callback:      kvHandler,
			RetryStrategy: retryStrat,
		}

		curOp, err := agent.dispatchOpToAddress(req, serverAddress)
		if err != nil {
			op.lock.Lock()
			op.results = append(op.results, PingResult{
				Endpoint: redactSystemData(serverAddress),
				Error:    err,
				Latency:  0,
				Scope:    bucketName,
			})
			op.lock.Unlock()
			continue
		}

		op.lock.Lock()
		op.subops = append(op.subops, pingSubOp{
			endpoint: serverAddress,
			op:       curOp,
		})
		atomic.AddInt32(&op.remaining, 1)
		addrToID[serverAddress] = fmt.Sprintf("%p", pipeline)
		op.lock.Unlock()
	}

	// We initialized remaining to one to ensure that the callback is not
	// invoked until all of the operations have been dispatched first.  This
	// final handling is to indicate that all operations were dispatched.
	op.lock.Lock()
	op.handledOneLocked()
	op.lock.Unlock()

	return op, nil
}

// MemdConnInfo represents information we know about a particular
// memcached connection reported in a diagnostics report.
type MemdConnInfo struct {
	LocalAddr    string
	RemoteAddr   string
	LastActivity time.Time
	Scope        string
	ID           string
}

// DiagnosticInfo is returned by the Diagnostics method and includes
// information about the overall health of the clients connections.
type DiagnosticInfo struct {
	ConfigRev int64
	MemdConns []MemdConnInfo
}

// Diagnostics returns diagnostics information about the client.
// Mainly containing a list of open connections and their current
// states.
func (agent *Agent) Diagnostics() (*DiagnosticInfo, error) {
	for {
		clientMux := agent.kvMux.GetState()
		if clientMux == nil {
			return nil, errShutdown
		}

		var conns []MemdConnInfo

		for _, pipeline := range clientMux.pipelines {
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
				}
				if agent.bucketName != "" {
					conn.Scope = redactMetaData(agent.bucketName)
				}
				conns = append(conns, conn)
			}
			pipeline.clientsLock.Unlock()
		}

		endMux := agent.kvMux.GetState()
		if endMux == clientMux {
			return &DiagnosticInfo{
				ConfigRev: clientMux.revID,
				MemdConns: conns,
			}, nil
		}
	}
}
