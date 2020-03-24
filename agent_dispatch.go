package gocbcore

import (
	"sync/atomic"
)

// Cas represents a unique revision of a document.  This can be used
// to perform optimistic locking.
type Cas uint64

// VbUUID represents a unique identifier for a particular vbucket history.
type VbUUID uint64

// SeqNo is a sequential mutation number indicating the order and precise
// position of a write that has occurred.
type SeqNo uint64

// MutationToken represents a particular mutation within the cluster.
type MutationToken struct {
	VbID   uint16
	VbUUID VbUUID
	SeqNo  SeqNo
}

// PendingOp represents an outstanding operation within the client.
// This can be used to cancel an operation before it completes.
// This can also be used to Get information about the operation once
// it has completed (cancelled or successful).
type PendingOp interface {
	Cancel(err error)
}

type multiPendingOp struct {
	ops          []PendingOp
	completedOps uint32
	isIdempotent bool
}

func (mp *multiPendingOp) Cancel(err error) {
	for _, op := range mp.ops {
		op.Cancel(err)
	}
}

func (mp *multiPendingOp) CompletedOps() uint32 {
	return atomic.LoadUint32(&mp.completedOps)
}

func (mp *multiPendingOp) IncrementCompletedOps() uint32 {
	return atomic.AddUint32(&mp.completedOps, 1)
}
