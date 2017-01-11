package gocbcore

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type drainedReqCallback func(*memdQRequest)

type memdQueue struct {
	lock      sync.RWMutex
	isDrained bool
	reqsCh    chan *memdQRequest
}

func createMemdQueue() *memdQueue {
	return &memdQueue{
		reqsCh: make(chan *memdQRequest, 5000),
	}
}

func (s *memdQueue) QueueRequest(req *memdQRequest) bool {
	s.lock.RLock()
	if s.isDrained {
		s.lock.RUnlock()
		return false
	}

	if !atomic.CompareAndSwapPointer(&req.queuedWith, nil, unsafe.Pointer(s)) {
		panic("Request was dispatched while already queued somewhere.")
	}

	logSchedf("Writing request to queue!")

	// Try to write the request to the queue, if the queue is full,
	//   we immediately fail the request with a queueOverflow error.
	select {
	case s.reqsCh <- req:
		s.lock.RUnlock()
		return true

	default:
		s.lock.RUnlock()
		// As long as we have not lost ownership, dispatch a queue overflow error.
		if atomic.CompareAndSwapPointer(&req.queuedWith, unsafe.Pointer(s), nil) {
			req.Callback(nil, nil, ErrOverload)
		}
		return true
	}
}

func (queue *memdQueue) UnqueueRequest(req *memdQRequest) bool {
	return atomic.CompareAndSwapPointer(&req.queuedWith, unsafe.Pointer(queue), nil)
}

func (queue *memdQueue) drainTillEmpty(reqCb drainedReqCallback) {
	for {
		select {
		case req := <-queue.reqsCh:
			if queue.UnqueueRequest(req) {
				reqCb(req)
			}
		default:
			return
		}
	}
}

func (queue *memdQueue) drainTillSignalAndEmpty(reqCb drainedReqCallback, signal chan bool) {
	for {
		select {
		case req := <-queue.reqsCh:
			if queue.UnqueueRequest(req) {
				reqCb(req)
			}
		case <-signal:
			queue.drainTillEmpty(reqCb)
			return
		}
	}
}

// Drains all the requests out of the queue.  This will mark the queue as drained
//   (further attempts to send it requests will fail), and call the specified
//   callback for each request that was still queued.
func (queue *memdQueue) Drain(reqCb drainedReqCallback, readersDoneSig chan bool) {
	// Set up a signal for making this method synchronous in spite
	//   of us internally running a goroutine.
	finishedSig := make(chan bool)

	// Start up our drainer goroutine.  This will ensure that queue is constantly
	//   being drained while we perform the shutdown of the queue, without this,
	//   we may deadlock between trying to write to a full queue, and trying to
	//   get the lock to mark it as draining.
	closedSig := make(chan bool)
	go func() {
		queue.drainTillSignalAndEmpty(reqCb, closedSig)
		finishedSig <- true
	}()

	// First we mark this queue as draining, this will prevent further requests
	//   from being dispatched from any external sources.
	queue.lock.Lock()
	queue.isDrained = true
	queue.lock.Unlock()

	// If there is anyone actively processing data off this queue, we need to wait
	//   till they've stopped before we can clear this queue, this is because of
	//   the fact that its possible that the processor might need to put a request
	//   back in the queue if it fails to handle it and we need to make sure the
	//   queue is emptying so there is room for the processor to put it in.
	if readersDoneSig != nil {
		<-readersDoneSig
	}

	// Signal our drain goroutine that it can stop now (once its emptied the queue).
	closedSig <- true

	// Wait until the drainer goroutine finishes draining everything
	<-finishedSig
}
