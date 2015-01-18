package gocouchbaseio

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"unsafe"
)

type routerFunc func(*memdRequest) *memdQueueConn
type routeData struct {
	revId      uint
	servers    []*memdQueueConn
	vbMap      [][]int
	capiEpList []string
	mgmtEpList []string

	source *cfgBucket
}

type PendingOp interface {
	Cancel() bool
}

// This class represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	useSsl     bool
	memdDialer Dialer
	authFn     AuthFunc

	routingInfo unsafe.Pointer

	configCh     chan *cfgBucket
	configWaitCh chan *memdRequest
	deadServerCh chan *memdQueueConn

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

func parseConfig(source *memdQueueConn, config []byte) (*cfgBucket, error) {
	configStr := strings.Replace(string(config), "$HOST", source.Hostname(), -1)

	bk := new(cfgBucket)
	err := json.Unmarshal([]byte(configStr), bk)
	if err != nil {
		return nil, err
	}

	bk.SourceHostname = source.Hostname()
	return bk, nil
}

func (c *Agent) globalHandler() {
	for {
		select {
		case <-c.deadServerCh:
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

func (c *Agent) handleServerNmv(s *memdQueueConn, req *memdRequest, resp *memdResponse) {
	// Try to parse the value as a bucket configuration
	bk, err := parseConfig(s, resp.Value)
	if err == nil {
		c.updateConfig(bk)
	}

	// Redirect it!  This may actually come back to this server, but I won't tell
	//   if you don't ;)
	atomic.AddUint64(&c.Stats.NumOpRelocated, 1)
	c.redispatchDirect(req)
}

func (c *Agent) handleServerDeath(s *memdQueueConn) {
	c.deadServerCh <- s
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

			// Hostname blank means to use the same one as was connected to
			if node.Hostname == "" {
				node.Hostname = bk.SourceHostname
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

	var newRouting *routeData
	var oldServers []*memdQueueConn
	var addServers []*memdQueueConn
	var newServers []*memdQueueConn
	for {
		oldRouting := (*routeData)(atomic.LoadPointer(&c.routingInfo))

		// BUG(brett19): Need to do revision comparison here to make sure that
		//   another config update has not preempted us to a higher revision!

		oldServers = oldRouting.servers
		newServers = []*memdQueueConn{}

		for _, hostPort := range kvServerList {
			var newServer *memdQueueConn

			// See if this server exists in the old routing data and is still alive
			for _, oldServer := range oldServers {
				if oldServer.Address() == hostPort && !oldServer.IsClosed() {
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
				newServer = createMemdQueueConn(hostPort, c.useSsl, c.memdDialer)
				newServer.SetHandlers(c.handleServerNmv, c.handleServerDeath)
				addServers = append(addServers, newServer)
			}

			newServers = append(newServers, newServer)
		}

		// Build a new routing object
		newRouting = &routeData{
			revId:      0,
			servers:    newServers,
			capiEpList: capiEpList,
			mgmtEpList: mgmtEpList,
			vbMap:      bk.VBucketServerMap.VBucketMap,
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
		addServer := addServer
		go func() {
			err := addServer.Connect(c.authFn)
			if err != nil {
				c.handleServerDeath(addServer)
			}
		}()
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
			go c.drainServer(oldServer)
		}
	}
}

/*
func CreateDcpAgent(memdAddrs, httpAddrs []string, useSsl bool, memdDialer Dialer, authFn AuthFunc, dcpStreamName string) (*Agent, error) {
	if memdDialer == nil {
		memdDialer = &DefaultDialer{}
	}
	dcpAuthWrap := func(c AuthClient) error {
		if err := authFn(c); err != nil {
			return err
		}
		return c.OpenDcpStream(dcpStreamName)
	}
	c := &Agent{
		useSsl:       useSsl,
		authFn:       dcpAuthWrap,
		configCh:     make(chan *cfgBucket, 5),
		configWaitCh: make(chan *memdRequest, 5),
		deadServerCh: make(chan *memdQueueConn, 5),
	}
	if err := c.connect(memdAddrs, httpAddrs); err != nil {
		return nil, err
	}
	return c, nil
}
*/

func CreateAgent(memdAddrs, httpAddrs []string, useSsl bool, memdDialer Dialer, authFn AuthFunc) (*Agent, error) {
	if memdDialer == nil {
		memdDialer = &DefaultDialer{}
	}
	c := &Agent{
		useSsl:       useSsl,
		authFn:       authFn,
		memdDialer:   memdDialer,
		configCh:     make(chan *cfgBucket, 5),
		configWaitCh: make(chan *memdRequest, 5),
		deadServerCh: make(chan *memdQueueConn, 5),
	}
	if err := c.connect(memdAddrs, httpAddrs); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Agent) connect(memdAddrs, httpAddrs []string) error {
	var firstConfig *cfgBucket
	for _, thisHostPort := range memdAddrs {
		srv := createMemdQueueConn(thisHostPort, c.useSsl, c.memdDialer)

		atomic.AddUint64(&c.Stats.NumServerConnect, 1)

		err := srv.Connect(c.authFn)
		if err != nil {
			continue
		}

		cccpBytes, err := srv.DoCccpRequest()
		if err != nil {
			srv.Close()
			continue
		}

		bk, err := parseConfig(srv, cccpBytes)
		if err != nil {
			srv.Close()
			continue
		}

		// Build some fake routing data, this is used to essentially 'pass' the
		//   server connection we already have over to the config update function.
		//   It also gives it something to CAS against, note that we do not return
		//   from this function until after the config update happens, meaning this
		//   temporary routing data should not ever be used, so no need to CAS it.
		c.routingInfo = unsafe.Pointer(&routeData{
			servers: []*memdQueueConn{srv},
		})

		srv.SetHandlers(c.handleServerNmv, c.handleServerDeath)

		firstConfig = bk

		break
	}

	if firstConfig == nil {
		panic("HTTP configurations not yet supported")

		//go httpConfigHandler()
		// Need to select here for timeouts
		//firstConfig := <-c.configCh

		//if firstConfig == nil {
		//	panic("Failed to retrieve first good configuration.")
		//}
	}

	c.updateConfig(firstConfig)
	go c.globalHandler()

	return nil
}

// Drains all the requests out of the queue for this server.  This must be
//   invoked only once this server no longer exists in the routing data or an
//   infinite loop will likely occur.
func (c *Agent) drainServer(s *memdQueueConn) {
	s.CloseAndDrain(func(req *memdRequest) {
		c.redispatchDirect(req)
	})
}

// This function is meant to be used when a memdRequest is internally shuffled
//   around.  It will fail to redispatch operations which are not allowed to be
//   moved between connections for whatever reason.
func (c *Agent) redispatchDirect(req *memdRequest) {
	if req.ReplicaIdx >= 0 {
		// Reschedule the operation
		c.dispatchDirect(req)
	} else {
		// Callback advising that a network failure caused this operation to
		//   not be processed, nothing outside the agent should really see this.
		req.Callback(nil, agentError{"Network failure"})
	}
}

func (c *Agent) routeRequest(req *memdRequest) *memdQueueConn {
	routingInfo := *(*routeData)(atomic.LoadPointer(&c.routingInfo))

	repId := req.ReplicaIdx
	if repId < 0 {
		panic("Uhh ohh...")
	} else {
		vbId := cbCrc(req.Key) % uint32(len(routingInfo.vbMap))
		req.Vbucket = uint16(vbId)
		srvIdx := routingInfo.vbMap[vbId][repId]
		return routingInfo.servers[srvIdx]
	}
}

// This immediately dispatches a request to the appropriate server based on the
//  currently available routing data.
func (c *Agent) dispatchDirect(req *memdRequest) error {
	// While not currently possible, this function has the potential to
	//   fail in the future if the client has already started to shutdown
	//   when a new request comes in
	// if c.isShutDown { return "Shutting down" }

	for {
		server := c.routeRequest(req)

		if !server.DispatchRequest(req) {
			continue
		}

		break
	}
	return nil
}

func (c *Agent) GetCapiEps() []string {
	routingInfo := *(*routeData)(atomic.LoadPointer(&c.routingInfo))
	return routingInfo.capiEpList
}

func (c *Agent) GetMgmtEps() []string {
	routingInfo := *(*routeData)(atomic.LoadPointer(&c.routingInfo))
	return routingInfo.mgmtEpList
}
