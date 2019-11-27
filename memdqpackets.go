package gocbcore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// The data for a response from a server.  This includes the
// packets data along with some useful meta-data related to
// the response.
type memdQResponse struct {
	memdPacket

	sourceAddr   string
	sourceConnID string
	isInternal   bool
}

type callback func(*memdQResponse, *memdQRequest, error)

// The data for a request that can be queued with a memdqueueconn,
// and can potentially be rerouted to multiple servers due to
// configuration changes.
type memdQRequest struct {
	memdPacket

	// Static routing properties
	ReplicaIdx int
	Callback   callback
	Persistent bool

	// Owner represents the agent which created and currently owns
	// this request.  This is used for specialized routing such as
	// not-my-vbucket and enhanced errors.
	owner *Agent

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

	// This is used to lock access to the request when processing
	// a timeout, a response or spans
	processingLock sync.Mutex

	// This stores the number of times that the item has been
	// retried. It is used for various non-linear retry
	// algorithms.
	retryCount uint32

	// This is used to determine what, if any, retry strategy to use
	// when deciding whether to retry the request and calculating
	// any back-off time period.
	RetryStrategy RetryStrategy

	// This is the set of reasons why this request has been retried.
	retryReasons []RetryReason

	lastDispatchedTo   string
	lastDispatchedFrom string
	lastConnectionID   string

	// If the request is in the process of being retried then this is the function
	// to call to stop the retry wait for this request.
	cancelRetryTimerFunc func() bool
	cancelTimerLock      sync.Mutex

	RootTraceContext RequestSpanContext
	cmdTraceSpan     RequestSpan
	netTraceSpan     RequestSpan

	CollectionName string
	ScopeName      string

	onCompletion func(err error)
}

func (req *memdQRequest) RetryAttempts() uint32 {
	return atomic.LoadUint32(&req.retryCount)
}

func (req *memdQRequest) incrementRetryAttempts() {
	atomic.AddUint32(&req.retryCount, 1)
}

func (req *memdQRequest) Identifier() string {
	return fmt.Sprintf("0x%x", req.Opaque)
}

func (req *memdQRequest) Idempotent() bool {
	_, ok := idempotentOps[req.Opcode]
	return ok
}

func (req *memdQRequest) RetryReasons() []RetryReason {
	return req.retryReasons
}

func (req *memdQRequest) LocalEndpoint() string {
	return req.lastDispatchedFrom
}

func (req *memdQRequest) RemoteEndpoint() string {
	return req.lastDispatchedTo
}

func (req *memdQRequest) ConnectionID() string {
	return req.lastConnectionID
}

func (req *memdQRequest) setCancelRetry(cancelFunc func() bool) {
	req.cancelTimerLock.Lock()
	req.cancelRetryTimerFunc = cancelFunc
	req.cancelTimerLock.Unlock()
}

func (req *memdQRequest) addRetryReason(retryReason RetryReason) {
	found := false
	for i := 0; i < len(req.retryReasons); i++ {
		if req.retryReasons[i] == retryReason {
			found = true
			break
		}
	}

	// if idx is out of the range of retryReasons then it wasn't found.
	if !found {
		req.retryReasons = append(req.retryReasons, retryReason)
	}
}

func (req *memdQRequest) cloneNew() *memdQRequest {
	return &memdQRequest{
		memdPacket:       req.memdPacket,
		ReplicaIdx:       req.ReplicaIdx,
		Callback:         req.Callback,
		Persistent:       req.Persistent,
		owner:            req.owner,
		RootTraceContext: req.RootTraceContext,
	}
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
	req.processingLock.Lock()

	if atomic.SwapUint32(&req.isCompleted, 1) != 0 {
		// Someone already completed this request
		req.processingLock.Unlock()
		return false
	}

	req.cancelTimerLock.Lock()
	if req.cancelRetryTimerFunc != nil {
		// It doesn't really matter if this succeeds or fails but we should try to do it anyway.
		// At worst the request will be requeued and then rejected.
		req.cancelRetryTimerFunc()
	}
	req.cancelTimerLock.Unlock()

	queuedWith := (*memdOpQueue)(atomic.LoadPointer(&req.queuedWith))
	if queuedWith != nil {
		queuedWith.Remove(req)
	}

	waitingIn := (*memdClient)(atomic.LoadPointer(&req.waitingIn))
	if waitingIn != nil {
		waitingIn.CancelRequest(req)
	}

	if req.onCompletion != nil {
		req.onCompletion(ErrCancelled)
	}

	req.owner.cancelReqTrace(req, ErrCancelled)
	req.processingLock.Unlock()
	return true
}
