package gocouchbaseio

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type memdResponse struct {
	Magic    commandMagic
	Opcode   commandCode
	Datatype uint8
	Status   statusCode
	Cas      uint64
	Key      []byte
	Extras   []byte
	Value    []byte

	// Controlled internally by Agent
	opaque uint32
}
type memdCallback func(*memdResponse, error)
type memdRequest struct {
	// These properties are not modified once dispatched
	Magic      commandMagic
	Opcode     commandCode
	Datatype   uint8
	Cas        uint64
	Key        []byte
	Extras     []byte
	Value      []byte
	ReplicaIdx int
	Callback   memdCallback

	// The unique lookup id assigned to this request for mapping
	//   responses from the server back to their requests.
	opaque uint32

	// The target vBucket id for this request.  This is usually
	//   updated by the routing function when a request is routed.
	vBucket uint16

	// This stores a pointer to the server that currently own
	//   this request.  When a request is resolved or cancelled,
	//   this is nulled out.  This property allows the request to
	//   lookup who owns it during cancelling as well as prevents
	//   callback after cancel, or cancel after callback.
	queuedWith unsafe.Pointer
}

type memdServer struct {
	address string
	useSsl  bool

	conn      io.ReadWriteCloser
	isDead    bool
	isDrained bool
	lock      sync.RWMutex
	reqsCh    chan *memdRequest

	opIndex uint32
	opMap   map[uint32]*memdRequest
	mapLock sync.Mutex

	recvHdrBuf []byte
}

func (s *memdServer) hostname() string {
	return strings.Split(s.address, ":")[0]
}

type MemdAuthClient interface {
	Address() string

	SaslListMechs() ([]string, error)
	SaslAuth(k, v []byte) ([]byte, error)
	SaslStep(k, v []byte) ([]byte, error)
	SelectBucket(b []byte) error
}

type memdAuthClient struct {
	srv *memdServer
}

func (s *memdAuthClient) Address() string {
	return s.srv.address
}
func (s *memdAuthClient) SaslListMechs() ([]string, error) {
	return s.srv.DoSaslListMechs()
}
func (s *memdAuthClient) SaslAuth(k, v []byte) ([]byte, error) {
	return s.srv.DoSaslAuth(k, v)
}
func (s *memdAuthClient) SaslStep(k, v []byte) ([]byte, error) {
	return s.srv.DoSaslStep(k, v)
}
func (s *memdAuthClient) SelectBucket(b []byte) error {
	return s.srv.DoSelectBucket(b)
}

type routerFunc func(*memdRequest) *memdServer
type routeData struct {
	revId      uint
	servers    []*memdServer
	routeFn    routerFunc
	capiEpList []string
	mgmtEpList []string

	source *cfgBucket
}

type AuthFunc func(MemdAuthClient) error

type PendingOp interface {
	Cancel() bool
}

type memdPendingOp struct {
	agent *Agent
	req   *memdRequest
}

func (op memdPendingOp) Cancel() bool {
	return op.agent.dequeueRequest(op.req)
}

// This class represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	useSsl bool
	authFn AuthFunc

	routingInfo unsafe.Pointer

	configCh     chan *cfgBucket
	configWaitCh chan *memdRequest
	deadServerCh chan *memdServer

	Stats struct {
		NumConfigUpdate  uint64
		NumServerConnect uint64
		NumServerLost    uint64
		NumServerRemoved uint64
		NumOpRelocated   uint64
		NumOp            uint64
		NumOpResp        uint64
		NumOpTimeout     uint64
	}
}

// Creates a new memdServer object for the specified address.
func (c *Agent) createServer(addr string) *memdServer {
	return &memdServer{
		address:    addr,
		useSsl:     c.useSsl,
		reqsCh:     make(chan *memdRequest),
		opMap:      make(map[uint32]*memdRequest),
		recvHdrBuf: make([]byte, 24),
	}
}

// Creates a new memdRequest object to perform PLAIN authentication to a server.
func makePlainAuthRequest(bucket, password string) *memdRequest {
	userBuf := []byte(bucket)
	passBuf := []byte(password)

	authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
	authData[0] = 0
	copy(authData[1:], userBuf)
	authData[1+len(userBuf)] = 0
	copy(authData[1+len(userBuf)+1:], passBuf)

	authMech := []byte("PLAIN")

	return &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdSASLAuth,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      authMech,
		Value:    authData,
	}
}

// Creates a new memdRequest object to perform a configuration request.
func makeCccpRequest() *memdRequest {
	return &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdGetClusterConfig,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      nil,
		Value:    nil,
	}
}

// Dials and Authenticates a memdServer object to the cluster.
func (s *memdServer) connect(authFn AuthFunc) error {
	fmt.Printf("Dialing %s\n", s.address)

	if !s.useSsl {
		conn, err := net.Dial("tcp", s.address)
		if err != nil {
			fmt.Printf("Dial Error %v\n", err)
			return err
		}
		fmt.Printf("Dial Complete\n")

		s.conn = conn
	} else {
		conn, err := tls.Dial("tcp", s.address, &tls.Config{
			InsecureSkipVerify: true,
		})
		if err != nil {
			fmt.Printf("SSL Dial Error %v\n", err)
			return err
		}
		fmt.Printf("SSL Dial Complete\n")

		s.conn = conn
	}

	err := authFn(&memdAuthClient{s})
	if err != nil {
		fmt.Printf("Server authentication failed!\n")
		return agentError{"Authentication failure."}
	}

	fmt.Printf("Server connect success (%s)!\n", s.address)
	return nil
}

func (s *memdServer) doRequest(req *memdRequest) (*memdResponse, error) {
	err := s.writeRequest(req)
	if err != nil {
		return nil, err
	}

	return s.readResponse()
}

func (s *memdServer) DoCccpRequest() ([]byte, error) {
	resp, err := s.doRequest(makeCccpRequest())
	if err != nil {
		return nil, err
	}

	return resp.Value, nil
}

func (s *memdServer) doBasicOp(cmd commandCode, k, v []byte) ([]byte, error) {
	resp, err := s.doRequest(&memdRequest{
		Magic:  reqMagic,
		Opcode: cmd,
		Key:    k,
		Value:  v,
	})
	if err != nil {
		return nil, err
	}

	// BUG(brett19): Need to convert the resp.Status to an error
	//  here, otherwise errors arn't propagated.

	return resp.Value, nil
}
func (s *memdServer) DoSaslListMechs() ([]string, error) {
	bytes, err := s.doBasicOp(cmdSASLListMechs, nil, nil)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(bytes), " "), nil
}
func (s *memdServer) DoSaslAuth(k, v []byte) ([]byte, error) {
	return s.doBasicOp(cmdSASLAuth, k, v)
}
func (s *memdServer) DoSaslStep(k, v []byte) ([]byte, error) {
	return s.doBasicOp(cmdSASLStep, k, v)
}
func (s *memdServer) DoSelectBucket(b []byte) error {
	_, err := s.doBasicOp(cmdSelectBucket, nil, b)
	return err
}

func (s *memdServer) readResponse() (*memdResponse, error) {
	hdrBuf := s.recvHdrBuf

	_, err := io.ReadFull(s.conn, hdrBuf)
	if err != nil {
		return nil, err
	}

	bodyLen := int(binary.BigEndian.Uint32(hdrBuf[8:]))

	bodyBuf := make([]byte, bodyLen)
	_, err = io.ReadFull(s.conn, bodyBuf)
	if err != nil {
		return nil, err
	}

	keyLen := int(binary.BigEndian.Uint16(hdrBuf[2:]))
	extLen := int(hdrBuf[4])

	return &memdResponse{
		Magic:    commandMagic(hdrBuf[0]),
		Opcode:   commandCode(hdrBuf[1]),
		Datatype: hdrBuf[5],
		Status:   statusCode(binary.BigEndian.Uint16(hdrBuf[6:])),
		opaque:   binary.BigEndian.Uint32(hdrBuf[12:]),
		Cas:      binary.BigEndian.Uint64(hdrBuf[16:]),
		Extras:   bodyBuf[:extLen],
		Key:      bodyBuf[extLen : extLen+keyLen],
		Value:    bodyBuf[extLen+keyLen:],
	}, nil
}

func (s *memdServer) writeRequest(req *memdRequest) error {
	extLen := len(req.Extras)
	keyLen := len(req.Key)
	valLen := len(req.Value)

	buffer := make([]byte, 24+keyLen+extLen+valLen)

	buffer[0] = uint8(req.Magic)
	buffer[1] = uint8(req.Opcode)
	binary.BigEndian.PutUint16(buffer[2:], uint16(keyLen))
	buffer[4] = byte(extLen)
	buffer[5] = req.Datatype
	binary.BigEndian.PutUint16(buffer[6:], uint16(req.vBucket))
	binary.BigEndian.PutUint32(buffer[8:], uint32(len(buffer)-24))
	binary.BigEndian.PutUint32(buffer[12:], req.opaque)
	binary.BigEndian.PutUint64(buffer[16:], req.Cas)

	copy(buffer[24:], req.Extras)
	copy(buffer[24+extLen:], req.Key)
	copy(buffer[24+extLen+keyLen:], req.Value)

	_, err := s.conn.Write(buffer)
	return err
}

func (c *Agent) serverConnectRun(s *memdServer, authFn AuthFunc) {
	atomic.AddUint64(&c.Stats.NumServerConnect, 1)
	err := s.connect(authFn)
	if err != nil {
		c.deadServerCh <- s
		return
	}

	c.serverRun(s)
}

func (c *Agent) serverRun(s *memdServer) {
	killSig := make(chan bool)

	// Reading
	go func() {
		for {
			resp, err := s.readResponse()
			if err != nil {
				c.deadServerCh <- s
				killSig <- true
				break
			}

			c.resolveRequest(s, resp)
		}
	}()

	// Writing
	for {
		select {
		case req := <-s.reqsCh:
			// We do a cursory check of the server to avoid dispatching operations on the network
			//   that have already knowingly been cancelled.  This doesn't prevent a cancelled
			//   operation from being sent, but it does reduce network IO when possible.
			server := (*memdServer)(atomic.LoadPointer(&req.queuedWith))
			if server != s {
				continue
			}

			err := s.writeRequest(req)
			if err != nil {
				fmt.Printf("Write failure occured for %v\n", req)

				// Lock the server to write to it's queue, as used in dispatchRequest.
				//   If the server has already been drained, this indicates that the routing
				//   information is update, the servers opMap is thrown out and we are safe
				//   to redispatch this operation like the drainer would.
				s.lock.RLock()
				if s.isDrained {
					s.lock.RUnlock()
					c.redispatchDirect(s, req)
				} else {
					s.reqsCh <- req
					s.lock.RUnlock()
				}
				break
			}
		case <-killSig:
			break
		}
	}
}

func (c *Agent) shotgunCccp() []byte {
	var writtenReqs []*memdRequest
	writtenOps := 0
	respCh := make(chan *memdResponse)

	for {
		routingInfo := (*routeData)(atomic.LoadPointer(&c.routingInfo))

		for _, srv := range routingInfo.servers {
			req := makeCccpRequest()
			req.Callback = func(resp *memdResponse, err error) {
				if err == nil {
					respCh <- resp
				} else {
					respCh <- nil
				}
			}
			if !c.dispatchTo(srv, req) {
				if writtenOps > 0 {
					// If we've already written ops, we can't try again, so just keep
					//   going as if this one didn't fail.
					continue
				} else {
					// If no ops were written yet, then encountering a drained server
					//   means we can just refresh our routing info and try again.
					writtenOps = -1
					break
				}
			}
			writtenReqs = append(writtenReqs, req)
			writtenOps++
		}
		if writtenOps == -1 {
			continue
		}

		// Shotgun Success!
		break
	}

	configCh := make(chan []byte)
	cancelSig := make(chan bool)
	go func() {
		recvdOps := 0
		for {
			select {
			case resp := <-respCh:
				recvdOps++

				if resp.Status != success {
					if recvdOps == writtenOps {
						// This is the last of the requests, and we are still
						//   looping, so lets fail fast and let the reader know
						//   that everyone is sucking...
						configCh <- nil
						break
					} else {
						// If there are requests left, just silently ignore.
						continue
					}
				}

				// Send back the received configuration!
				configCh <- resp.Value
				break
			case <-cancelSig:
				break
			}
		}

		// Cancel the requests in case any are still pending, this is really
		//   just an optimization as most of the requests probably were already
		//   dispatched across the network...
		for _, req := range writtenReqs {
			c.dequeueRequest(req)
		}
	}()

	select {
	case configStr := <-configCh:
		return configStr
	case <-time.After(5 * time.Second):
		cancelSig <- true
		return nil
	}
}

func (c *Agent) configRunner() {
	// HTTP Polling
	//  - run http polling
	// CCCP Polling
	//  c.globalRequestCh <- &memdRequest{
	//    ReplicaIdx: -1 // No Specific Server
	//  }

	confSig := make(chan bool)
	configCh := make(chan *cfgBucket)
	lastConfig := time.Time{}
	configMinWait := 1 * time.Second
	configMaxWait := 120 * time.Second
	//lastCccpSrvIdx := 0

	go func() {
		for {
			// Wait for an explicit configuration request, or for our explicit
			//   max wait time to trip.
			select {
			case <-confSig:
			case <-time.After(configMaxWait):
			}

			// Make sure if we receive multiple configuration requests quickly
			//  that we don't spam the server with requests
			configWait := time.Now().Sub(lastConfig)
			if configWait < configMinWait {
				continue
			}

			req := makeCccpRequest()
			req.Callback = func(resp *memdResponse, err error) {

			}

			configCh <- nil
		}
	}()

	confSig <- true

}

func (c *Agent) globalHandler() {
	fmt.Printf("Global Handler Running\n")
	for {
		fmt.Printf("Global Handler Loop\n")

		select {
		case deadSrv := <-c.deadServerCh:
			// Mark the server as dead
			deadSrv.isDead = true

			// Refresh the routing data with the existing configuration, this has
			//   the effect of attempting to rebuild the dead server.
			c.updateConfig(nil)

			// TODO(brett19): We probably should actually try other ways of resolving
			//  the issue, like requesting a new configuration.

		case config := <-c.configCh:
			c.updateConfig(config)

		}
	}
}

// Accepts a cfgBucket object representing a cluster configuration and rebuilds the server list
//  along with any routing information for the Client.  Passing no config will refresh the existing one.
//  This method MUST NEVER BLOCK due to its use from various contention points.
func (c *Agent) updateConfig(bk *cfgBucket) {
	atomic.AddUint64(&c.Stats.NumConfigUpdate, 1)

	if bk == nil {
		oldRouting := (*routeData)(atomic.LoadPointer(&c.routingInfo))
		bk = oldRouting.source
	}

	var kvServerList []string
	var capiEpList []string
	var mgmtEpList []string

	if bk.NodesExt != nil {
		var kvPort uint16
		for _, node := range bk.NodesExt {
			if !c.useSsl {
				kvPort = node.Services.Kv
			} else {
				kvPort = node.Services.KvSsl
			}
			kvServerList = append(kvServerList, fmt.Sprintf("%s:%d", node.Hostname, kvPort))

			if !c.useSsl {
				capiEpList = append(capiEpList, fmt.Sprintf("http://%s:%d/%s", node.Hostname, node.Services.Capi, bk.Name))
				mgmtEpList = append(mgmtEpList, fmt.Sprintf("http://%s:%d", node.Hostname, node.Services.Mgmt))
			} else {
				capiEpList = append(capiEpList, fmt.Sprintf("https://%s:%d/%s", node.Hostname, node.Services.CapiSsl, bk.Name))
				mgmtEpList = append(mgmtEpList, fmt.Sprintf("https://%s:%d", node.Hostname, node.Services.MgmtSsl))
			}
		}
	} else {
		if c.useSsl {
			panic("Received config without nodesExt while SSL is enabled.")
		}

		kvServerList = bk.VBucketServerMap.ServerList

		for _, node := range bk.Nodes {
			capiEpList = append(capiEpList, node.CouchAPIBase)
			mgmtEpList = append(mgmtEpList, fmt.Sprintf("http://%s", node.Hostname))
		}
	}

	fmt.Printf("Gogo Server List: %v\n", kvServerList)

	var newRouting *routeData
	var oldServers []*memdServer
	var addServers []*memdServer
	var newServers []*memdServer
	for {
		oldRouting := (*routeData)(atomic.LoadPointer(&c.routingInfo))

		// BUG(brett19): Need to do revision comparison here to make sure that
		//   another config update has not preempted us to a higher revision!

		oldServers = oldRouting.servers
		newServers = []*memdServer{}

		for _, hostPort := range kvServerList {
			var newServer *memdServer

			// See if this server exists in the old routing data and is still alive
			for _, oldServer := range oldServers {
				if !oldServer.isDead && oldServer.address == hostPort {
					newServer = oldServer
					break
				}
			}

			// If we did not find the server in our old routing data, we need to build
			//   a new connection instead.  This simply creates the object, we don't "launch"
			//   the servers operation until later once we know we've successfully CAS'd our
			//   new routing data so we don't have to kill them if the CAS fails due to
			//   another goroutine also updating the config.
			if newServer == nil {
				newServer = c.createServer(hostPort)
				addServers = append(addServers, newServer)
			}

			newServers = append(newServers, newServer)
		}

		// Build a new routing object
		rt := func(req *memdRequest) *memdServer {
			repId := req.ReplicaIdx
			if repId < 0 {
				repId = 0
			}
			vbId := cbCrc(req.Key) % uint32(len(bk.VBucketServerMap.VBucketMap))
			req.vBucket = uint16(vbId)
			srvIdx := bk.VBucketServerMap.VBucketMap[vbId][repId]

			return newServers[srvIdx]
		}
		newRouting = &routeData{
			revId:      0,
			servers:    newServers,
			capiEpList: capiEpList,
			mgmtEpList: mgmtEpList,
			routeFn:    rt,
			source:     bk,
		}

		// Attempt to atomically update the routing data
		if !atomic.CompareAndSwapPointer(&c.routingInfo, unsafe.Pointer(oldRouting), unsafe.Pointer(newRouting)) {
			// Someone preempted us, let's restart and try again...
			continue
		}

		// We've successfully swapped to the new config, lets finish building the
		//   new routing data's connections and destroy/draining old connections.
		break
	}

	// Launch all the new servers
	for _, addServer := range addServers {
		fmt.Printf("Launching server %s\n", addServer.address)
		go c.serverConnectRun(addServer, c.authFn)
	}

	// Identify all the dead servers and drain their requests
	for _, oldServer := range oldServers {
		found := false
		for _, newServer := range newServers {
			if newServer == oldServer {
				found = true
				break
			}
		}
		if !found {
			fmt.Printf("Draining server %s\n", oldServer.address)
			go c.drainServer(oldServer)
		}
	}
}

func CreateAgent(memdAddrs, httpAddrs []string, useSsl bool, authFn AuthFunc) (*Agent, error) {
	fmt.Printf("ADDRs: %v\n", memdAddrs)

	c := &Agent{
		useSsl:       useSsl,
		authFn:       authFn,
		configCh:     make(chan *cfgBucket, 0),
		configWaitCh: make(chan *memdRequest, 1),
		deadServerCh: make(chan *memdServer, 1),
	}

	var firstConfig *cfgBucket
	for _, thisHostPort := range memdAddrs {
		srv := c.createServer(thisHostPort)

		fmt.Printf("Time to connect new server %s\n", thisHostPort)
		atomic.AddUint64(&c.Stats.NumServerConnect, 1)
		err := srv.connect(authFn)

		fmt.Printf("MConnect: %v, %v\n", srv, err)
		if err != nil {
			fmt.Printf("Connection to %s failed, continueing\n", thisHostPort)
			continue
		}

		resp, err := srv.doRequest(makeCccpRequest())
		if err != nil {
			fmt.Printf("CCCP failed on %s, continueing\n", thisHostPort)
			continue
		}

		fmt.Printf("Request processed %v\n", resp)

		configStr := strings.Replace(string(resp.Value), "$HOST", srv.hostname(), -1)
		bk := new(cfgBucket)
		err = json.Unmarshal([]byte(configStr), bk)
		fmt.Printf("DATA: %v %v\n", err, bk)

		go c.serverRun(srv)

		// Build some fake routing data, this is used to essentially 'pass' the
		//   server connection we already have over to the config update function.
		//   It also gives it something to CAS against, note that we do not return
		//   from this function until after the config update happens, meaning this
		//   temporary routing data should not be able to be be used for any routing.
		c.routingInfo = unsafe.Pointer(&routeData{
			servers: []*memdServer{srv},
		})

		firstConfig = bk

		break
	}

	if firstConfig == nil {
		fmt.Printf("Failed to get CCCP config, trying HTTP")

		panic("HTTP configurations not yet supported")

		//go httpConfigHandler()
		// Need to select here for timeouts
		firstConfig := <-c.configCh

		if firstConfig == nil {
			panic("Failed to retrieve first good configuration.")
		}
	}

	c.updateConfig(firstConfig)
	go c.globalHandler()

	return c, nil
}

// Drains all the requests out of the queue for this server.  This must be
//   invoked only once this server no longer exists in the routing data or an
//   infinite loop will likely occur.
func (c *Agent) drainServer(s *memdServer) {
	signal := make(chan bool)

	go func() {
		for {
			select {
			case req := <-s.reqsCh:
				c.redispatchDirect(s, req)
			case <-signal:
				break
			}
		}
		// Signal means no more requests will be added to the queue, but we still
		//  need to drain what was there.
		for {
			select {
			case req := <-s.reqsCh:
				c.redispatchDirect(s, req)
			default:
				break
			}
		}
	}()

	s.lock.Lock()
	s.isDrained = true
	s.lock.Unlock()

	signal <- true
}

// Dispatch a request to a specific server instance, this operation will
//   fail if the server you're dispatching to has been drained.  If this
//   occurs, you should refresh your view of the routing information.
func (c *Agent) dispatchTo(server *memdServer, req *memdRequest) bool {
	server.lock.RLock()
	if server.isDrained {
		server.lock.RUnlock()
		return false
	}

	server.mapLock.Lock()
	server.opIndex++
	req.opaque = server.opIndex
	server.opMap[req.opaque] = req
	atomic.StorePointer(&req.queuedWith, unsafe.Pointer(server))
	server.mapLock.Unlock()

	server.reqsCh <- req
	server.lock.RUnlock()
	return true
}

// This immediately dispatches a request to the appropriate server based on the
//  currently applicable routing data.
func (c *Agent) dispatchDirect(req *memdRequest) error {
	if req.queuedWith != nil {
		panic("Attempted to double-queue request.")
	}

	for {
		routingInfo := *(*routeData)(atomic.LoadPointer(&c.routingInfo))
		server := routingInfo.routeFn(req)

		if !c.dispatchTo(server, req) {
			continue
		}

		break
	}
	return nil
}

// This is used to dispatch a request to a server after being drained from another.
// This function assumes the opMap of the source server will be destroyed and does
//   not need to be cleaned up.
func (c *Agent) redispatchDirect(origServer *memdServer, req *memdRequest) {
	if atomic.CompareAndSwapPointer(&req.queuedWith, unsafe.Pointer(origServer), nil) {
		if req.ReplicaIdx >= 0 {
			// Reschedule the operation
			c.dispatchDirect(req)
		} else {
			// Callback advising that a network failure caused this operation to
			//   not be processed, nothing outside the agent should really see this.
			req.Callback(nil, agentError{"Network failure"})
		}
	}
}

// This resolves a request using a response and its Opaque value along with the server
//  which received the request.
func (c *Agent) resolveRequest(s *memdServer, resp *memdResponse) {
	opIndex := resp.opaque

	// Find the request that goes with this response
	s.mapLock.Lock()
	req := s.opMap[opIndex]
	if req != nil {
		delete(s.opMap, opIndex)
	}
	s.mapLock.Unlock()

	if req != nil {
		// If this is a Not-My-VBucket, redispatch the request again after a config update.
		if resp.Status == notMyVBucket {
			// Try to parse the value as a bucket configuration
			bk := new(cfgBucket)
			err := json.Unmarshal(resp.Value, bk)
			if err == nil {
				c.updateConfig(bk)
			}

			fmt.Printf("NMV Redispatch!\n")

			// Redirect it!  This may actually come back to this server, but I won't tell
			//   if you don't ;)
			atomic.AddUint64(&c.Stats.NumOpRelocated, 1)
			c.redispatchDirect(s, req)
		} else {
			// Check that the request is still linked to us, unlink it and dispatch its callback
			if atomic.CompareAndSwapPointer(&req.queuedWith, unsafe.Pointer(s), nil) {
				if resp.Status == success {
					req.Callback(resp, nil)
				} else {
					req.Callback(nil, &memdError{resp.Status})
				}
			}
		}
	}
}

// This will dequeue a request that was dispatched and stop its callback from being
//   invoked, should be used primarily for 'cancelling' a request, note that the operation
//   may have been dispatched across the network regardless of cancellation.
func (c *Agent) dequeueRequest(req *memdRequest) bool {
	server := (*memdServer)(atomic.SwapPointer(&req.queuedWith, nil))
	if server == nil {
		return false
	}
	server.mapLock.Lock()
	delete(server.opMap, req.opaque)
	server.mapLock.Unlock()
	return true
}

type GetCallback func([]byte, uint32, uint64, error)
type UnlockCallback func(uint64, error)
type TouchCallback func(uint64, error)
type RemoveCallback func(uint64, error)
type StoreCallback func(uint64, error)
type CounterCallback func(uint64, uint64, error)

func (c *Agent) dispatchOp(req *memdRequest) (PendingOp, error) {
	err := c.dispatchDirect(req)
	if err != nil {
		return nil, err
	}
	return memdPendingOp{c, req}, nil
}

func (c *Agent) Get(key []byte, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, resp.Cas, nil)
	}
	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdGet,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      key,
		Value:    nil,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetAndTouch(key []byte, expiry uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, resp.Cas, nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf, expiry)

	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdGAT,
		Datatype: 0,
		Cas:      0,
		Extras:   extraBuf,
		Key:      key,
		Value:    nil,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetAndLock(key []byte, lockTime uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, resp.Cas, nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf, lockTime)

	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdGetLocked,
		Datatype: 0,
		Cas:      0,
		Extras:   extraBuf,
		Key:      key,
		Value:    nil,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetReplica(key []byte, replicaIdx int, cb GetCallback) (PendingOp, error) {
	if replicaIdx <= 0 {
		panic("Replica number must be greater than 0")
	}

	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, resp.Cas, nil)
	}

	req := &memdRequest{
		Magic:      reqMagic,
		Opcode:     cmdGetReplica,
		Datatype:   0,
		Cas:        0,
		Extras:     nil,
		Key:        key,
		Value:      nil,
		Callback:   handler,
		ReplicaIdx: replicaIdx,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Touch(key []byte, expiry uint32, cb TouchCallback) (PendingOp, error) {
	// This seems odd, but this is how it's done in Node.js
	return c.GetAndTouch(key, expiry, func(value []byte, flags uint32, cas uint64, err error) {
		cb(cas, err)
	})
}

func (c *Agent) Unlock(key []byte, cas uint64, cb UnlockCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
	}

	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdUnlockKey,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      key,
		Value:    nil,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Remove(key []byte, cas uint64, cb RemoveCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
	}

	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   cmdDelete,
		Datatype: 0,
		Cas:      cas,
		Extras:   nil,
		Key:      key,
		Value:    nil,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) store(opcode commandCode, key, value []byte, flags uint32, cas uint64, expiry uint32, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf, flags)
	binary.BigEndian.PutUint32(extraBuf, expiry)
	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   opcode,
		Datatype: 0,
		Cas:      cas,
		Extras:   extraBuf,
		Key:      key,
		Value:    value,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Add(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return c.store(cmdAdd, key, value, flags, 0, expiry, cb)
}

func (c *Agent) Set(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return c.store(cmdSet, key, value, flags, 0, expiry, cb)
}

func (c *Agent) Replace(key, value []byte, flags uint32, cas uint64, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return c.store(cmdReplace, key, value, flags, cas, expiry, cb)
}

func (c *Agent) adjoin(opcode commandCode, key, value []byte, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
	}

	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   opcode,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      key,
		Value:    value,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Append(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return c.adjoin(cmdAppend, key, value, cb)
}

func (c *Agent) Prepend(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return c.adjoin(cmdPrepend, key, value, cb)
}

func (c *Agent) counter(opcode commandCode, key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, 0, err)
			return
		}

		intVal, perr := strconv.ParseUint(string(resp.Value), 10, 64)
		if perr != nil {
			cb(0, 0, agentError{"Failed to parse returned value"})
			return
		}

		cb(intVal, resp.Cas, nil)
	}

	extraBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extraBuf[0:], delta)
	binary.BigEndian.PutUint64(extraBuf[8:], initial)
	binary.BigEndian.PutUint32(extraBuf[16:], expiry)

	req := &memdRequest{
		Magic:    reqMagic,
		Opcode:   opcode,
		Datatype: 0,
		Cas:      0,
		Extras:   extraBuf,
		Key:      key,
		Value:    nil,
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Increment(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return c.counter(cmdIncrement, key, delta, initial, expiry, cb)
}

func (c *Agent) Decrement(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return c.counter(cmdDecrement, key, delta, initial, expiry, cb)
}

func (c *Agent) GetCapiEps() []string {
	routingInfo := *(*routeData)(atomic.LoadPointer(&c.routingInfo))
	return routingInfo.capiEpList
}

func (c *Agent) GetMgmtEps() []string {
	routingInfo := *(*routeData)(atomic.LoadPointer(&c.routingInfo))
	return routingInfo.mgmtEpList
}
