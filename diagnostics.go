package gocbcore

import (
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
