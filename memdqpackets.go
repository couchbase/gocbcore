package gocbcore

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// The data for a response from a server.  This includes the
// packets data along with some useful meta-data related to
// the response.
type memdQResponse struct {
	memdPacket

	sourceAddr string
}

type callback func(*memdQResponse, *memdQRequest, error)
type routingCallback func(*memdQResponse, *memdQRequest) (bool, error)

// The data for a request that can be queued with a memdqueueconn,
// and can potentially be rerouted to multiple servers due to
// configuration changes.
type memdQRequest struct {
	memdPacket

	// Static routing properties
	ReplicaIdx      int
	Callback        callback
	Persistent      bool
	RoutingCallback routingCallback

	// This tracks when the request was dispatched so that we can
	//  properly prioritize older requests to try and meet timeout
	//  requirements.
	dispatchTime time.Time

	// This stores a pointer to the server that currently own
	//   this request.  This allows us to remove it from that list
	//   whenever the request is cancelled.
	queuedWith unsafe.Pointer

	// This stores a pointer to the opList that currently is holding
	//  this request.  This allows us to remove it form that list
	//  whenever the request is cancelled
	waitingIn unsafe.Pointer

	// This keeps track of whether the request has been 'completed'
	//  which is synonymous with the callback having been invoked.
	//  This is an integer to allow us to atomically control it.
	isCompleted uint32

	// This stores the number of times that the item has been
	// retried, and is used for various non-linear retry
	// algorithms.
	retryCount uint32
}

func (req *memdQRequest) tryCallback(resp *memdQResponse, err error) bool {
	if req.Persistent {
		if atomic.LoadUint32(&req.isCompleted) == 0 {
			req.Callback(resp, req, err)
			return true
		}
	} else {
		if atomic.SwapUint32(&req.isCompleted, 1) == 0 {
			req.Callback(resp, req, err)
			return true
		}
	}

	return false
}

func (req *memdQRequest) isCancelled() bool {
	return atomic.LoadUint32(&req.isCompleted) != 0
}

func (req *memdQRequest) Cancel() bool {
	if atomic.SwapUint32(&req.isCompleted, 1) != 0 {
		// Someone already completed this request
		return false
	}

	queuedWith := (*memdOpQueue)(atomic.LoadPointer(&req.queuedWith))
	if queuedWith != nil {
		queuedWith.Remove(req)
	}

	waitingIn := (*memdOpMap)(atomic.LoadPointer(&req.waitingIn))
	if waitingIn != nil {
		waitingIn.Remove(req)
	}

	return true
}
