package gocbcore

import (
	"sync/atomic"
	"unsafe"
)

// The data for a request that can be queued with a memdqueueconn,
//   and can potentially be rerouted to multiple servers due to
//   configuration changes.
type memdQRequest struct {
	memdPacket

	// Static routing properties
	ReplicaIdx int
	Callback   Callback
	Persistent bool

	// This stores a pointer to the server that currently own
	//   this request.  When a request is resolved or cancelled,
	//   this is nulled out.  This property allows the request to
	//   lookup who owns it during cancelling as well as prevents
	//   callback after cancel, or cancel after callback.
	queuedWith unsafe.Pointer

	// Holds the next item in the opList, this is used by the
	//   memdOpQueue to avoid extra GC for a discreet list
	//   element structure.
	queueNext *memdQRequest
}

func (req *memdQRequest) QueueOwner() *memdQueue {
	return (*memdQueue)(atomic.LoadPointer(&req.queuedWith))
}

func (req *memdQRequest) Cancel() bool {
	queue := (*memdQueue)(atomic.SwapPointer(&req.queuedWith, nil))
	if queue == nil {
		return false
	}
	return true
}
