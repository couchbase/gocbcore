package gocbcore

import (
	"sync"
	"sync/atomic"
)

// PendingOp represents an outstanding operation within the client.
// This can be used to cancel an operation before it completes.
// This can also be used to Get information about the operation once
// it has completed (cancelled or successful).
type PendingOp interface {
	Cancel()
}

type multiPendingOp struct {
	ops          []PendingOp
	completedOps uint32
	isIdempotent bool
	cancelled    bool
	lock         sync.Mutex
}

func (mp *multiPendingOp) Len() int {
	mp.lock.Lock()
	defer mp.lock.Unlock()
	return len(mp.ops)
}

func (mp *multiPendingOp) AddOp(op PendingOp) {
	mp.lock.Lock()
	if mp.cancelled {
		mp.lock.Unlock()
		op.Cancel()
		return
	}

	mp.ops = append(mp.ops, op)
	mp.lock.Unlock()
}

func (mp *multiPendingOp) Cancel() {
	mp.lock.Lock()
	mp.cancelled = true
	var ops []PendingOp
	ops = append(ops, mp.ops...)
	mp.lock.Unlock()

	for _, op := range ops {
		op.Cancel()
	}
}

func (mp *multiPendingOp) CompletedOps() uint32 {
	return atomic.LoadUint32(&mp.completedOps)
}

func (mp *multiPendingOp) IncrementCompletedOps() uint32 {
	return atomic.AddUint32(&mp.completedOps, 1)
}
