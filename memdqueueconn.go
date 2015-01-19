package gocouchbaseio

import (
	"encoding/binary"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Interface used by memdQueueConn to connect to a memcache server.
type Dialer interface {
	// Dials an address and returns a ReadWriteCloser for that connection.
	Dial(address string, useSsl bool) (io.ReadWriteCloser, error)
}

type memdInitFunc func(*memdQueueConn) error

type CloseHandler func(*memdQueueConn)
type BadRouteHandler func(*memdQueueConn, *memdRequest, *memdResponse)

// Callback invoked by a successfully executed request
type Callback func(*memdResponse, error)

// The data for a request that can be queued with a memdqueueconn,
//   and can potentially be rerouted to multiple servers due to
//   configuration changes.
type memdRequest struct {
	// These properties are not modified once dispatched
	Magic      CommandMagic
	Opcode     CommandCode
	Datatype   uint8
	Cas        uint64
	Vbucket    uint16
	Key        []byte
	Extras     []byte
	Value      []byte
	ReplicaIdx int
	Callback   Callback
	Persistent bool

	// The unique lookup id assigned to this request for mapping
	//   responses from the server back to their requests.
	opaque uint32

	// This stores a pointer to the server that currently own
	//   this request.  When a request is resolved or cancelled,
	//   this is nulled out.  This property allows the request to
	//   lookup who owns it during cancelling as well as prevents
	//   callback after cancel, or cancel after callback.
	queuedWith unsafe.Pointer

	// Holds the next item in the opList, this is used by the
	//   memdOpQueue to avoid extra GC for a discreet list
	//   element structure.
	queueNext *memdRequest
}

// The data returned from the server in relation to an executed
//   request.
type memdResponse struct {
	Magic    CommandMagic
	Opcode   CommandCode
	Datatype uint8
	Status   StatusCode
	Cas      uint64
	Key      []byte
	Extras   []byte
	Value    []byte

	opaque uint32
}

// This is used to store operations while they are pending
//   a response from the server to allow mapping of a response
//   opaque back to the originating request.  This queue takes
//   advantage of the monotonic nature of the opaque values
//   and synchronous responses from the server to nearly always
//   return the request without needing to iterate at all.
type memdOpQueue struct {
	first *memdRequest
	last  *memdRequest
}

// Add a new request to the bottom of the op queue.
func (q *memdOpQueue) Add(r *memdRequest) {
	if q.last == nil {
		q.first = r
		q.last = r
	} else {
		q.last.queueNext = r
		q.last = r
	}
}

// Removes a request from the op queue.  Expects to be passed
//   the request to remove, along with the request that
//   immediately preceeds it in the queue.
func (q *memdOpQueue) remove(prev *memdRequest, req *memdRequest) {
	if prev == nil {
		q.first = req.queueNext
		if q.first == nil {
			q.last = nil
		}
		return
	}
	prev.queueNext = req.queueNext
	if prev.queueNext == nil {
		q.last = prev
	}
}

// Removes a specific request from the op queue.
func (q *memdOpQueue) Remove(req *memdRequest) bool {
	var cur *memdRequest = q.first
	var prev *memdRequest
	for cur != nil {
		if cur == req {
			q.remove(prev, cur)
			return true
		}
		prev = cur
		cur = cur.queueNext
	}
	return false
}

// Locates a request (searching FIFO-style) in the op queue using
//   the opaque value that was assigned to it when it was dispatched.
//   It then removes the request from the queue if it is not persistent.
func (q *memdOpQueue) FindAndMaybeRemove(opaque uint32) *memdRequest {
	var cur *memdRequest = q.first
	var prev *memdRequest
	for cur != nil {
		if cur.opaque == opaque {
			if !cur.Persistent {
				q.remove(prev, cur)
			}
			return cur
		}
		prev = cur
		cur = cur.queueNext
	}
	return nil
}

// Clears the queue of all requests and calls the passed function
//   once for each request found in the queue.
func (q *memdOpQueue) Drain(cb func(*memdRequest)) {
	for cur := q.first; cur != nil; cur = cur.queueNext {
		cb(cur)
	}
	q.first = nil
	q.last = nil
}

type memdQueueConn struct {
	address string
	useSsl  bool
	dialer  Dialer

	conn    io.ReadWriteCloser
	recvBuf []byte

	lock           sync.RWMutex
	isClosed       bool
	isDrained      bool
	initReqsCh     chan *memdRequest
	reqsCh         chan *memdRequest
	handleBadRoute BadRouteHandler
	handleDeath    CloseHandler

	isDead  bool
	mapLock sync.Mutex
	opIndex uint32
	opList  memdOpQueue

	ioDoneCh chan bool
}

func (s *memdQueueConn) Address() string {
	return s.address
}

func (s *memdQueueConn) Hostname() string {
	return strings.Split(s.address, ":")[0]
}

func (s *memdQueueConn) IsClosed() bool {
	return s.isClosed
}

func createMemdQueueConn(addr string, useSsl bool, dialer Dialer) *memdQueueConn {
	return &memdQueueConn{
		address:    addr,
		useSsl:     useSsl,
		dialer:     dialer,
		initReqsCh: make(chan *memdRequest, 10),
		reqsCh:     make(chan *memdRequest, 100),
		ioDoneCh:   make(chan bool, 1),
	}
}

// Dials and Authenticates a memdQueueConn object to the cluster.
func (s *memdQueueConn) Connect(initFn memdInitFunc) error {
	conn, err := s.dialer.Dial(s.address, s.useSsl)
	if err != nil {
		return err
	}

	s.conn = conn
	go s.runIoHandlers()

	err = initFn(s)
	if err != nil {
		// We errored, close the connection!
		s.conn.Close()
		return err
	}

	close(s.initReqsCh)

	return nil
}

func (s *memdQueueConn) SetHandlers(badRouteFn BadRouteHandler, deathFn CloseHandler) error {
	s.lock.Lock()
	if s.isClosed {
		// We died between authentication and here, no deathFn was set,
		//   so we need to notify through the return of this function.
		s.lock.Unlock()
		return networkError{}
	}
	s.handleBadRoute = badRouteFn
	s.handleDeath = deathFn
	s.lock.Unlock()
	return nil
}

func (s *memdQueueConn) writeRequest(req *memdRequest) error {
	extLen := len(req.Extras)
	keyLen := len(req.Key)
	valLen := len(req.Value)

	// Go appears to do some clever things in regards to writing data
	//   to the kernel for network dispatch.  Having a write buffer
	//   per-server that is re-used actually hinders performance...
	// For now, we will simply create a new buffer and let it be GC'd.
	buffer := make([]byte, 24+keyLen+extLen+valLen)

	buffer[0] = uint8(req.Magic)
	buffer[1] = uint8(req.Opcode)
	binary.BigEndian.PutUint16(buffer[2:], uint16(keyLen))
	buffer[4] = byte(extLen)
	buffer[5] = req.Datatype
	binary.BigEndian.PutUint16(buffer[6:], uint16(req.Vbucket))
	binary.BigEndian.PutUint32(buffer[8:], uint32(len(buffer)-24))
	binary.BigEndian.PutUint32(buffer[12:], req.opaque)
	binary.BigEndian.PutUint64(buffer[16:], req.Cas)

	copy(buffer[24:], req.Extras)
	copy(buffer[24+extLen:], req.Key)
	copy(buffer[24+extLen+keyLen:], req.Value)

	_, err := s.conn.Write(buffer)
	return err
}

func (s *memdQueueConn) readBuffered(n int) ([]byte, error) {
	// Make sure our buffer is big enough to hold all our data
	if len(s.recvBuf) < n {
		neededSize := 4096
		if neededSize < n {
			neededSize = n
		}
		newBuf := make([]byte, neededSize)
		copy(newBuf[0:], s.recvBuf[0:])
		s.recvBuf = newBuf[0:len(s.recvBuf)]
	}

	// Loop till we encounter an error or have enough data...
	for {
		// Check if we already have enough data buffered
		if n <= len(s.recvBuf) {
			buf := s.recvBuf[0:n]
			s.recvBuf = s.recvBuf[n:]
			return buf, nil
		}

		// Read data up to the capacity
		recvTgt := s.recvBuf[len(s.recvBuf):cap(s.recvBuf)]
		n, err := s.conn.Read(recvTgt)
		if n <= 0 {
			return nil, err
		}

		// Update the len of our slice to encompass our new data
		s.recvBuf = s.recvBuf[:len(s.recvBuf)+n]
	}
}

func (s *memdQueueConn) readResponse() (*memdResponse, error) {
	hdrBuf, err := s.readBuffered(24)
	if err != nil {
		return nil, err
	}

	bodyLen := int(binary.BigEndian.Uint32(hdrBuf[8:]))
	bodyBuf, err := s.readBuffered(bodyLen)
	if err != nil {
		return nil, err
	}

	keyLen := int(binary.BigEndian.Uint16(hdrBuf[2:]))
	extLen := int(hdrBuf[4])

	return &memdResponse{
		Magic:    CommandMagic(hdrBuf[0]),
		Opcode:   CommandCode(hdrBuf[1]),
		Datatype: hdrBuf[5],
		Status:   StatusCode(binary.BigEndian.Uint16(hdrBuf[6:])),
		opaque:   binary.BigEndian.Uint32(hdrBuf[12:]),
		Cas:      binary.BigEndian.Uint64(hdrBuf[16:]),
		Extras:   bodyBuf[:extLen],
		Key:      bodyBuf[extLen : extLen+keyLen],
		Value:    bodyBuf[extLen+keyLen:],
	}, nil
}

func (s *memdQueueConn) queueRequest(req *memdRequest, forInit bool) bool {
	s.lock.RLock()
	if s.isDrained {
		s.lock.RUnlock()
		return false
	}

	oldSP := atomic.SwapPointer(&req.queuedWith, unsafe.Pointer(s))
	if oldSP != nil {
		panic("Request was dispatched while already queued somewhere.")
	}

	if !forInit {
		s.reqsCh <- req
	} else {
		s.initReqsCh <- req
	}

	s.lock.RUnlock()
	return true
}

// Dispatch a request to this server instance, this operation will
//   fail if the server you're dispatching to has been drained.
func (s *memdQueueConn) QueueRequest(req *memdRequest) bool {
	return s.queueRequest(req, false)
}

func (r *memdRequest) Cancel() bool {
	server := (*memdQueueConn)(atomic.SwapPointer(&r.queuedWith, nil))
	if server == nil {
		return false
	}
	server.mapLock.Lock()
	server.opList.Remove(r)
	//delete(server.opMap, r.opaque)
	server.mapLock.Unlock()
	return true
}

func (s *memdQueueConn) execRequest(req *memdRequest, forInit bool) (respOut *memdResponse, errOut error) {
	if req.Callback != nil {
		panic("Tried to synchronously dispatch an operation with an async handler.")
	}

	signal := make(chan bool)

	req.Callback = func(resp *memdResponse, err error) {
		respOut = resp
		errOut = err
		signal <- true
	}

	if !s.queueRequest(req, forInit) {
		return nil, &generalError{"Failed to dispatch operation."}
	}

	select {
	case <-signal:
		return
	case <-time.After(2500 * time.Millisecond):
		req.Cancel()
		return nil, &generalError{"Operation timed out."}
	}
}

func (s *memdQueueConn) execInitRequest(req *memdRequest) (respOut *memdResponse, errOut error) {
	return s.execRequest(req, true)
}

func (s *memdQueueConn) ExecRequest(req *memdRequest) (respOut *memdResponse, errOut error) {
	return s.execRequest(req, false)
}

func (s *memdQueueConn) resolveRequest(resp *memdResponse) {
	opIndex := resp.opaque

	// Find the request that goes with this response
	s.mapLock.Lock()
	req := s.opList.FindAndMaybeRemove(opIndex)
	s.mapLock.Unlock()

	if req == nil {
		// There is no known request that goes with this response.  Ignore it.
		return
	}

	if !req.Persistent {
		if !atomic.CompareAndSwapPointer(&req.queuedWith, unsafe.Pointer(s), nil) {
			// While we found a valid request, the request does not appear to be queued
			//   with this server anymore, this probably means that it has been cancelled.
			return
		}
	}

	if resp.Status == StatusNotMyVBucket {
		// If possible, lets backchannel our NMV back to the Agent of this memdQueueConn
		//   instance.  This is primarily meant to enhance performance, and allow the
		//   agent to be instantly notified upon a new configuration arriving.  If the
		//   backchannel isn't available, we just Callback with the NMV error.
		s.lock.RLock()
		badRouteFn := s.handleBadRoute
		s.lock.RUnlock()
		if badRouteFn != nil {
			badRouteFn(s, req, resp)
			return
		}
	}

	// Call the requests callback handler...  Ignore Status field for incoming requests.
	if resp.Magic == ReqMagic || resp.Status == StatusSuccess {
		req.Callback(resp, nil)
	} else {
		req.Callback(nil, &memdError{resp.Status})
	}
}

func (s *memdQueueConn) dispatchRequest(req *memdRequest) error {
	// We do a cursory check of the server to avoid dispatching operations on the network
	//   that have already knowingly been cancelled.  This doesn't guarentee a cancelled
	//   operation from being sent, but it does reduce network IO when possible.
	server := (*memdQueueConn)(atomic.LoadPointer(&req.queuedWith))
	if server != s {
		// Even though we failed to dispatch, this is not actually an error,
		//   we just consume the operation since its already been handled elsewhere
		return nil
	}

	s.mapLock.Lock()
	s.opIndex++
	req.opaque = s.opIndex
	s.opList.Add(req)
	s.mapLock.Unlock()

	err := s.writeRequest(req)
	if err != nil {
		s.mapLock.Lock()
		s.opList.Remove(req)
		s.mapLock.Unlock()

		return err
	}

	return nil
}

func (s *memdQueueConn) runIoHandlers() {
	killSig := make(chan bool)

	// Reading
	go func() {
		for {
			resp, err := s.readResponse()
			if err != nil {
				killSig <- true
				break
			}

			s.resolveRequest(resp)
		}
	}()

	// Writing
InitWriterLoop:
	for {
		select {
		case req, ok := <-s.initReqsCh:
			if ok {
				err := s.dispatchRequest(req)
				if err != nil {
					// We can assume that the server is not fully drained yet, as the drainer blocks
					//   waiting for the IO goroutines to finish first.
					s.initReqsCh <- req

					// We must wait for the receive goroutine to die as well before we can continue.
					<-killSig

					break InitWriterLoop
				}
			} else {
				// The initReqsCh was closed, which means we are done initializing
				// We set the init channel to nil now that we have been signalled that
				//   its no longer in use to ensure nobody else tries to read from it!
				s.initReqsCh = nil
			WriterLoop:
				for {
					select {
					case req := <-s.reqsCh:
						err := s.dispatchRequest(req)
						if err != nil {
							// We can assume that the server is not fully drained yet, as the drainer blocks
							//   waiting for the IO goroutines to finish first.
							s.reqsCh <- req

							// We must wait for the receive goroutine to die as well before we can continue.
							<-killSig

							break WriterLoop
						}
					case <-killSig:
						break WriterLoop
					}
				}
				break InitWriterLoop
			}
		case <-killSig:
			break InitWriterLoop
		}
	}

	// Now we must signal the drainer that we are done!
	s.ioDoneCh <- true

	// Signal the creator that we died :(
	s.lock.Lock()
	s.isClosed = true
	deathFn := s.handleDeath
	s.lock.Unlock()
	if deathFn != nil {
		deathFn(s)
	} else {
		s.CloseAndDrain(func(r *memdRequest) {
			r.Callback(nil, networkError{})
		})
	}
}

func (s *memdQueueConn) Close() {
	s.conn.Close()
}

type drainedReqCallback func(*memdRequest)

// Drains all the requests out of the queue for this server.  This will mark
//   the server as drained (further attempts to send it requests will fail),
//   and call the specified callback for each request that was still queued.
func (s *memdQueueConn) CloseAndDrain(reqCb drainedReqCallback) {
	signal := make(chan bool)
	go func() {
		// We need to drain the queue while also waiting for the signal, as the
		//   lock required before signalling can be held by other goroutines who
		//   are trying to send to the queue which could be full.
		for {
			select {
			case req := <-s.initReqsCh:
				req.Callback(nil, networkError{})
			case req := <-s.reqsCh:
				reqCb(req)
			case <-signal:
				// Signal means no more requests will be added to the queue, but we still
				//  need to drain what was there.
				for {
					select {
					case req := <-s.initReqsCh:
						req.Callback(nil, networkError{})
					case req := <-s.reqsCh:
						reqCb(req)
					default:
						return
					}
				}
			}
		}
	}()

	// First we mark this connection as being drained, this will prevent further
	//   requests from being dispatched to its queues from external sources.
	s.lock.Lock()
	s.isDrained = true
	s.lock.Unlock()

	// We close the connection to signal the IO goroutines that they need
	//   to finish.  Note that the receive goroutine will die first, after
	//   receiving all the remaining data on the socket, and signal the
	//   writer to stop too.
	s.conn.Close()

	// We then wait for the IO goroutines to shut down cleanely, this is an
	//   essential step to ensure we don't stop draining the queues until
	//   the other goroutines stop since if it reads a request out, but fails
	//   to write it, it might need to put it back into the queue.
	<-s.ioDoneCh

	// Scan through our list of pending ops (which doesn't need to be locked
	//   since the IO goroutine is stopped, and isDrained is set so no more
	//   requests will be dispatched.
	s.opList.Drain(func(r *memdRequest) {
		r.Callback(nil, networkError{})
	})

	// Signal our drain coroutine that it can stop now.
	signal <- true
}

func (s *memdQueueConn) ExecCccpRequest() ([]byte, error) {
	resp, err := s.ExecRequest(&memdRequest{
		Magic:    ReqMagic,
		Opcode:   CmdGetClusterConfig,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      nil,
		Value:    nil,
	})
	if err != nil {
		return nil, err
	}

	return resp.Value, nil
}

func (s *memdQueueConn) doBasicInitOp(cmd CommandCode, k, v []byte) ([]byte, error) {
	resp, err := s.execInitRequest(&memdRequest{
		Magic:  ReqMagic,
		Opcode: cmd,
		Key:    k,
		Value:  v,
	})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}
func (s *memdQueueConn) ExecSaslListMechs() ([]string, error) {
	bytes, err := s.doBasicInitOp(CmdSASLListMechs, nil, nil)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(bytes), " "), nil
}
func (s *memdQueueConn) ExecSaslAuth(k, v []byte) ([]byte, error) {
	return s.doBasicInitOp(CmdSASLAuth, k, v)
}
func (s *memdQueueConn) ExecSaslStep(k, v []byte) ([]byte, error) {
	return s.doBasicInitOp(CmdSASLStep, k, v)
}
func (s *memdQueueConn) ExecSelectBucket(b []byte) error {
	_, err := s.doBasicInitOp(CmdSelectBucket, nil, b)
	return err
}

func (s *memdQueueConn) OpenDcpChannel(streamName string) error {
	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], 1)

	_, err := s.execInitRequest(&memdRequest{
		Magic:    ReqMagic,
		Opcode:   CmdDcpOpenConnection,
		Datatype: 0,
		Cas:      0,
		Extras:   extraBuf,
		Key:      []byte(streamName),
		Value:    nil,
	})
	if err != nil {
		return err
	}
	return nil
}
