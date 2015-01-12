package gocouchbaseio

import "fmt"
import "io"
import "encoding/binary"
import "net"
import "crypto/tls"
import "strings"
import "net/http"
import "sync/atomic"
import "encoding/json"
import "time"
import "unsafe"
import "sync"
import "math/rand"
import "strconv"

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
	Magic    commandMagic
	Opcode   commandCode
	Datatype uint8
	Cas      uint64
	Key      []byte
	Extras   []byte
	Value    []byte

	Callback memdCallback

	// Controlled internally by Agent
	opaque     uint32
	vBucket    uint16
	queuedWith unsafe.Pointer
}

type MemdServer struct {
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

func (s *MemdServer) Address() string {
	return s.address
}

func (s *MemdServer) Hostname() string {
	return strings.Split(s.address, ":")[0]
}

type routerFunc func(*memdRequest) *MemdServer
type routeData struct {
	revId      uint
	servers    []*MemdServer
	routeFn    routerFunc
	capiEpList []string
	mgmtEpList []string

	source *cfgBucket
}

type AuthFunc func(*MemdServer) error

// This class represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	useSsl bool
	authFn AuthFunc

	routingInfo unsafe.Pointer

	httpCli *http.Client

	configCh     chan *cfgBucket
	configWaitCh chan *memdRequest
	deadServerCh chan *MemdServer

	operationTimeout time.Duration

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

func (c *Agent) KillTest() {
	routingInfo := (*routeData)(atomic.LoadPointer(&c.routingInfo))
	server := routingInfo.servers[rand.Intn(len(routingInfo.servers))]
	fmt.Printf("Killing server %s\n", server.address)
	server.conn.Close()
}

// Creates a new MemdServer object for the specified address.
func (c *Agent) createServer(addr string) *MemdServer {
	return &MemdServer{
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

// Dials and Authenticates a MemdServer object to the cluster.
func (s *MemdServer) connect(authFn AuthFunc) error {
	fmt.Printf("Dialing %s\n", s.address)

	if !s.useSsl {
		conn, err := net.Dial("tcp", s.address)
		if err != nil {
			return err
		}
		fmt.Printf("Dial Complete\n")

		s.conn = conn
	} else {
		conn, err := tls.Dial("tcp", s.address, &tls.Config{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return err
		}
		fmt.Printf("SSL Dial Complete\n")

		s.conn = conn
	}

	err := authFn(s)
	if err != nil {
		fmt.Printf("Server authentication failed!\n")
		return agentError{"Authentication failure."}
	}

	fmt.Printf("Server connect success (%s)!\n", s.address)
	return nil
}

func (s *MemdServer) doRequest(req *memdRequest) (*memdResponse, error) {
	err := s.writeRequest(req)
	if err != nil {
		return nil, err
	}

	return s.readResponse()
}

func (s *MemdServer) readResponse() (*memdResponse, error) {
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

func (s *MemdServer) writeRequest(req *memdRequest) error {
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

func (c *Agent) serverConnectRun(s *MemdServer, authFn AuthFunc) {
	atomic.AddUint64(&c.Stats.NumServerConnect, 1)
	err := s.connect(authFn)
	if err != nil {
		c.deadServerCh <- s
		return
	}

	c.serverRun(s)
}

func (c *Agent) serverRun(s *MemdServer) {
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

func (c *Agent) configRunner() {
	// HTTP Polling
	//  - run http polling
	// CCCP Polling
	//  c.globalRequestCh <- &memdRequest{
	//    ReplicaIdx: -1 // No Specific Server
	//  }
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
			go c.updateConfig(nil)

			// TODO(brett19): We probably should actually try other ways of resolving
			//  the issue, like requesting a new configuration.

		case config := <-c.configCh:
			c.updateConfig(config)

		}
	}
}

// Accepts a cfgBucket object representing a cluster configuration and rebuilds the server list
//  along with any routing information for the Client.  Passing no config will refresh the existing one.
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
	var oldServers []*MemdServer
	var addServers []*MemdServer
	var newServers []*MemdServer
	for {
		oldRouting := (*routeData)(atomic.LoadPointer(&c.routingInfo))

		// BUG(brett19): Need to do revision comparison here to make sure that
		//   another config update has not preempted us to a higher revision!

		oldServers = oldRouting.servers
		newServers = []*MemdServer{}

		for _, hostPort := range kvServerList {
			var newServer *MemdServer

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
		rt := func(req *memdRequest) *MemdServer {
			vbId := cbCrc(req.Key) % uint32(len(bk.VBucketServerMap.VBucketMap))
			req.vBucket = uint16(vbId)
			srvIdx := bk.VBucketServerMap.VBucketMap[vbId][0]

			rand.Seed(time.Now().UnixNano())
			if rand.Int31n(2) == 1 {
				fmt.Printf("Randomly causing NMV for fun..\n")
				srvIdx = int(rand.Int31n(int32(len(newServers))))
			}

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
			c.drainServer(oldServer)
		}
	}
}

func CreateAgent(memdAddrs, httpAddrs []string, useSsl bool, authFn AuthFunc) (*Agent, error) {
	fmt.Printf("ADDRs: %v\n", memdAddrs)

	c := &Agent{
		useSsl:           useSsl,
		authFn:           authFn,
		httpCli:          &http.Client{},
		configCh:         make(chan *cfgBucket, 0),
		configWaitCh:     make(chan *memdRequest, 1),
		deadServerCh:     make(chan *MemdServer, 1),
		operationTimeout: 2500 * time.Millisecond,
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

		configStr := strings.Replace(string(resp.Value), "$HOST", srv.Hostname(), -1)
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
			servers: []*MemdServer{srv},
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
func (c *Agent) drainServer(s *MemdServer) {
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

// This immediately dispatches a request to the appropriate server based on the
//  currently applicable routing data.
func (c *Agent) dispatchDirect(req *memdRequest) error {
	if req.queuedWith != nil {
		panic("Attempted to double-queue request.")
	}

	for {
		routingInfo := *(*routeData)(atomic.LoadPointer(&c.routingInfo))
		server := routingInfo.routeFn(req)

		server.lock.RLock()
		if server.isDrained {
			server.lock.RUnlock()
			continue
		}

		server.mapLock.Lock()
		server.opIndex++
		req.opaque = server.opIndex
		server.opMap[req.opaque] = req
		atomic.StorePointer(&req.queuedWith, unsafe.Pointer(server))
		server.mapLock.Unlock()

		server.reqsCh <- req
		server.lock.RUnlock()
		break
	}
	return nil
}

// This is used to dispatch a request to a server after being drained from another.
// This function assumes the opMap of the source server will be destroyed and does
//   not need to be cleaned up.
func (c *Agent) redispatchDirect(origServer *MemdServer, req *memdRequest) {
	if atomic.CompareAndSwapPointer(&req.queuedWith, unsafe.Pointer(origServer), nil) {
		c.dispatchDirect(req)
	}
}

// This resolves a request using a response and its Opaque value along with the server
//  which received the request.
func (c *Agent) resolveRequest(s *MemdServer, resp *memdResponse) {
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
				req.Callback(resp, nil)
			}
		}
	}
}

// This will dequeue a request that was dispatched and stop its callback from being
//   invoked, should be used primarily for 'cancelling' a request, note that the operation
//   may have been dispatched across the network regardless of cancellation.
func (c *Agent) dequeueRequest(req *memdRequest) bool {
	server := (*MemdServer)(atomic.SwapPointer(&req.queuedWith, nil))
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

func (c *Agent) Get(key []byte, cb GetCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) GetAndTouch(key []byte, expiry uint32, cb GetCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) GetAndLock(key []byte, lockTime uint32, cb GetCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) GetReplica(key []byte, replicaIdx int, cb GetCallback) error {
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
		Opcode:   cmdGetReplica,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      key,
		Value:    nil,
		Callback: handler,
	}
	return c.dispatchDirect(req)
}

func (c *Agent) Touch(key []byte, expiry uint32, cb TouchCallback) error {
	// This seems odd, but this is how it's done in Node.js
	return c.GetAndTouch(key, expiry, func(value []byte, flags uint32, cas uint64, err error) {
		cb(cas, err)
	})
}

func (c *Agent) Unlock(key []byte, cas uint64, cb UnlockCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) Remove(key []byte, cas uint64, cb RemoveCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) store(opcode commandCode, key, value []byte, flags uint32, cas uint64, expiry uint32, cb StoreCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) Add(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) error {
	return c.store(cmdAdd, key, value, flags, 0, expiry, cb)
}

func (c *Agent) Set(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) error {
	return c.store(cmdSet, key, value, flags, 0, expiry, cb)
}

func (c *Agent) Replace(key, value []byte, flags uint32, cas uint64, expiry uint32, cb StoreCallback) error {
	return c.store(cmdReplace, key, value, flags, cas, expiry, cb)
}

func (c *Agent) adjoin(opcode commandCode, key, value []byte, cb StoreCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) Append(key, value []byte, cb StoreCallback) error {
	return c.adjoin(cmdAppend, key, value, cb)
}

func (c *Agent) Prepend(key, value []byte, cb StoreCallback) error {
	return c.adjoin(cmdPrepend, key, value, cb)
}

func (c *Agent) counter(opcode commandCode, key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) error {
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
	return c.dispatchDirect(req)
}

func (c *Agent) Increment(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) error {
	return c.counter(cmdIncrement, key, delta, initial, expiry, cb)
}

func (c *Agent) Decrement(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) error {
	return c.counter(cmdDecrement, key, delta, initial, expiry, cb)
}

/*
func (c *Agent) GetCapiEp() string {
	usCapiEps := atomic.LoadPointer(&c.capiEps)
	capiEps := *(*[]string)(usCapiEps)
	return capiEps[rand.Intn(len(capiEps))]
}

func (c *Agent) GetMgmtEp() string {
	usMgmtEps := atomic.LoadPointer(&c.mgmtEps)
	mgmtEps := *(*[]string)(usMgmtEps)
	return mgmtEps[rand.Intn(len(mgmtEps))]
}
*/

func (c *Agent) SetOperationTimeout(val time.Duration) {
	c.operationTimeout = val
}
func (c *Agent) GetOperationTimeout() time.Duration {
	return c.operationTimeout
}
