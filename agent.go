// Package gocbcore implements methods for low-level communication
// with a Couchbase Server cluster.
package gocbcore

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"golang.org/x/net/http2"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Agent represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	bucket            string
	username          string
	password          string
	tlsConfig         *tls.Config
	initFn            memdInitFunc
	useMutationTokens bool
	useKvErrorMaps    bool
	useEnhancedErrors bool

	configLock  sync.Mutex
	routingInfo routeDataPtr
	kvErrorMap  kvErrorMapPtr
	numVbuckets int

	serverFailuresLock sync.Mutex
	serverFailures     map[string]time.Time

	httpCli *http.Client

	confHttpRedialPeriod time.Duration
	confHttpRetryDelay   time.Duration
	confCccpMaxWait      time.Duration
	confCccpPollPeriod   time.Duration

	serverConnectTimeout time.Duration
	serverWaitTimeout    time.Duration
	nmvRetryDelay        time.Duration
	kvPoolSize           int
	maxQueueSize         int
}

// ServerConnectTimeout gets the timeout for each server connection, including all authentication steps.
func (agent *Agent) ServerConnectTimeout() time.Duration {
	return agent.serverConnectTimeout
}

// SetServerConnectTimeout sets the timeout for each server connection.
func (agent *Agent) SetServerConnectTimeout(timeout time.Duration) {
	agent.serverConnectTimeout = timeout
}

// HttpClient returns a pre-configured HTTP Client for communicating with
// Couchbase Server.  You must still specify authentication information
// for any dispatched requests.
func (agent *Agent) HttpClient() *http.Client {
	return agent.httpCli
}

func (agent *Agent) getErrorMap() *kvErrorMap {
	return agent.kvErrorMap.Get()
}

// AuthFunc is invoked by the agent to authenticate a client.
type AuthFunc func(client AuthClient, deadline time.Time) error

// AgentConfig specifies the configuration options for creation of an Agent.
type AgentConfig struct {
	MemdAddrs         []string
	HttpAddrs         []string
	TlsConfig         *tls.Config
	BucketName        string
	Username          string
	Password          string
	AuthHandler       AuthFunc
	UseMutationTokens bool
	UseKvErrorMaps    bool
	UseEnhancedErrors bool

	HttpRedialPeriod time.Duration
	HttpRetryDelay   time.Duration
	CccpMaxWait      time.Duration
	CccpPollPeriod   time.Duration

	ConnectTimeout       time.Duration
	ServerConnectTimeout time.Duration
	NmvRetryDelay        time.Duration
	KvPoolSize           int
	MaxQueueSize         int
}

// FromConnStr populates the AgentConfig with information from a
// Couchbase Connection String.
func (config *AgentConfig) FromConnStr(connStr string) error {
	baseSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return err
	}

	spec, err := gocbconnstr.Resolve(baseSpec)
	if err != nil {
		return err
	}

	fetchOption := func(name string) (string, bool) {
		optValue := spec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}
		return optValue[len(optValue)-1], true
	}

	// Grab the resolved hostnames into a set of string arrays
	var httpHosts []string
	for _, specHost := range spec.HttpHosts {
		httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	var memdHosts []string
	for _, specHost := range spec.MemdHosts {
		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	// Get bootstrap_on option to determine which, if any, of the bootstrap nodes should be cleared
	switch val, _ := fetchOption("bootstrap_on"); val {
	case "http":
		memdHosts = nil
		if len(httpHosts) == 0 {
			return errors.New("bootstrap_on=http but no HTTP hosts in connection string")
		}
	case "cccp":
		httpHosts = nil
		if len(memdHosts) == 0 {
			return errors.New("bootstrap_on=cccp but no CCCP/Memcached hosts in connection string")
		}
	case "both":
	case "":
		// Do nothing
		break
	default:
		return errors.New("bootstrap_on={http,cccp,both}")
	}
	config.MemdAddrs = memdHosts
	config.HttpAddrs = httpHosts

	var tlsConfig *tls.Config
	if spec.UseSsl {
		certpath, _ := fetchOption("certpath")

		tlsConfig = &tls.Config{}
		if certpath == "" {
			tlsConfig.InsecureSkipVerify = true
		} else {
			cacert, err := ioutil.ReadFile(certpath)
			if err != nil {
				return err
			}

			roots := x509.NewCertPool()
			ok := roots.AppendCertsFromPEM(cacert)
			if !ok {
				return ErrInvalidCert
			}
			tlsConfig.RootCAs = roots
		}
	}
	config.TlsConfig = tlsConfig

	if spec.Bucket != "" {
		config.BucketName = spec.Bucket
	}

	if valStr, ok := fetchOption("config_total_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("config_total_timeout option must be a number")
		}
		config.ConnectTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("config_node_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("config_node_timeout option must be a number")
		}
		config.ServerConnectTimeout = time.Duration(val) * time.Millisecond
	}

	// This option is experimental
	if valStr, ok := fetchOption("http_redial_period"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("http redial period option must be a number")
		}
		config.HttpRedialPeriod = time.Duration(val) * time.Millisecond
	}

	// This option is experimental
	if valStr, ok := fetchOption("http_retry_delay"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("http retry delay option must be a number")
		}
		config.HttpRetryDelay = time.Duration(val) * time.Millisecond
	}

	// This option is deprecated (see config_poll_floor_interval)
	if valStr, ok := fetchOption("cccp_max_wait"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("cccp max wait option must be a number")
		}
		config.CccpMaxWait = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("config_poll_floor_interval"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("config pool floor interval option must be a number")
		}
		config.CccpMaxWait = time.Duration(val) * time.Millisecond
	}

	// This option is deprecated (see config_poll_interval)
	if valStr, ok := fetchOption("cccp_poll_period"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("cccp pool period option must be a number")
		}
		config.CccpPollPeriod = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("config_poll_interval"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("config pool interval option must be a number")
		}
		config.CccpPollPeriod = time.Duration(val) * time.Millisecond
	}

	// This option is experimental
	if valStr, ok := fetchOption("kv_pool_size"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kv pool size option must be a number")
		}
		config.KvPoolSize = int(val)
	}

	// This option is experimental
	if valStr, ok := fetchOption("max_queue_size"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("max queue size option must be a number")
		}
		config.MaxQueueSize = int(val)
	}

	if valStr, ok := fetchOption("use_kverrmaps"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return fmt.Errorf("use_kverrmaps option must be a boolean")
		}
		config.UseKvErrorMaps = val
	}

	if valStr, ok := fetchOption("use_enhanced_errors"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return fmt.Errorf("use_enhanced_errors option must be a boolean")
		}
		config.UseEnhancedErrors = val
	}

	if valStr, ok := fetchOption("fetch_mutation_tokens"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return fmt.Errorf("fetch_mutation_tokens option must be a boolean")
		}
		config.UseMutationTokens = val
	}

	return nil
}

func createInitFn(config *AgentConfig) memdInitFunc {
	return func(client *syncClient, deadline time.Time) error {
		if config.AuthHandler != nil {
			return config.AuthHandler(client, deadline)
		}

		if err := SaslAuthPlain(config.Username, config.Password, client, deadline); err != nil {
			return err
		}

		if client.SupportsFeature(featureSelectBucket) {
			if err := client.ExecSelectBucket([]byte(config.BucketName), deadline); err != nil {
				return err
			}
		}

		return nil
	}
}

// CreateAgent creates an agent for performing normal operations.
func CreateAgent(config *AgentConfig) (*Agent, error) {
	initFn := createInitFn(config)
	return createAgent(config, initFn)
}

// CreateDcpAgent creates an agent for performing DCP operations.
func CreateDcpAgent(config *AgentConfig, dcpStreamName string, openFlags DcpOpenFlag) (*Agent, error) {
	// We wrap the authorization system to force DCP channel opening
	//   as part of the "initialization" for any servers.
	initFn := createInitFn(config)
	dcpInitFn := func(client *syncClient, deadline time.Time) error {
		if err := initFn(client, deadline); err != nil {
			return err
		}
		if err := client.ExecOpenDcpConsumer(dcpStreamName, openFlags, deadline); err != nil {
			return err
		}
		if err := client.ExecEnableDcpNoop(180*time.Second, deadline); err != nil {
			return err
		}
		return client.ExecEnableDcpBufferAck(8*1024*1024, deadline)
	}
	return createAgent(config, dcpInitFn)
}

func createAgent(config *AgentConfig, initFn memdInitFunc) (*Agent, error) {
	// TODO(brett19): Put all configurable options in the AgentConfig

	httpTransport := &http.Transport{
		TLSClientConfig: config.TlsConfig,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	err := http2.ConfigureTransport(httpTransport)
	if err != nil {
		logDebugf("failed to configure http2: %s", err)
	}

	c := &Agent{
		bucket:    config.BucketName,
		username:  config.Username,
		password:  config.Password,
		tlsConfig: config.TlsConfig,
		initFn:    initFn,
		httpCli: &http.Client{
			Transport: httpTransport,
		},
		useMutationTokens:    config.UseMutationTokens,
		useKvErrorMaps:       config.UseKvErrorMaps,
		useEnhancedErrors:    config.UseEnhancedErrors,
		serverFailures:       make(map[string]time.Time),
		serverConnectTimeout: 7000 * time.Millisecond,
		serverWaitTimeout:    5 * time.Second,
		nmvRetryDelay:        100 * time.Millisecond,
		kvPoolSize:           1,
		maxQueueSize:         2048,
		confHttpRetryDelay:   10 * time.Second,
		confHttpRedialPeriod: 10 * time.Second,
		confCccpMaxWait:      3 * time.Second,
		confCccpPollPeriod:   2500 * time.Millisecond,
	}

	connectTimeout := 60000 * time.Millisecond
	if config.ConnectTimeout > 0 {
		connectTimeout = config.ConnectTimeout
	}

	if config.ServerConnectTimeout > 0 {
		c.serverConnectTimeout = config.ServerConnectTimeout
	}
	if config.NmvRetryDelay > 0 {
		c.nmvRetryDelay = config.NmvRetryDelay
	}
	if config.KvPoolSize > 0 {
		c.kvPoolSize = config.KvPoolSize
	}
	if config.MaxQueueSize > 0 {
		c.maxQueueSize = config.MaxQueueSize
	}
	if config.HttpRetryDelay > 0 {
		c.confHttpRetryDelay = config.HttpRetryDelay
	}
	if config.HttpRedialPeriod > 0 {
		c.confHttpRedialPeriod = config.HttpRedialPeriod
	}
	if config.CccpMaxWait > 0 {
		c.confCccpMaxWait = config.CccpMaxWait
	}
	if config.CccpPollPeriod > 0 {
		c.confCccpPollPeriod = config.CccpPollPeriod
	}

	deadline := time.Now().Add(connectTimeout)
	if err := c.connect(config.MemdAddrs, config.HttpAddrs, deadline); err != nil {
		return nil, err
	}
	return c, nil
}

func (agent *Agent) cccpLooper() {
	tickTime := agent.confCccpPollPeriod
	maxWaitTime := agent.confCccpMaxWait

	logDebugf("CCCP Looper starting.")

	nodeIdx := -1
	for {
		// Wait 10 seconds
		time.Sleep(tickTime)

		routingInfo := agent.routingInfo.Get()
		if routingInfo == nil {
			// If we have a blank routingInfo, it indicates the client is shut down.
			break
		}

		numNodes := routingInfo.clientMux.NumPipelines()
		if numNodes == 0 {
			logDebugf("CCCPPOLL: No nodes available to poll")
			continue
		}

		if nodeIdx < 0 {
			nodeIdx = rand.Intn(numNodes)
		}

		var foundConfig *cfgBucket
		for nodeOff := 0; nodeOff < numNodes; nodeOff++ {
			nodeIdx = (nodeIdx + 1) % numNodes

			pipeline := routingInfo.clientMux.GetPipeline(nodeIdx)

			client := syncClient{
				client: &memdPipelineSenderWrap{
					pipeline: pipeline,
				},
			}
			cccpBytes, err := client.ExecCccpRequest(time.Now().Add(maxWaitTime))
			if err != nil {
				logDebugf("CCCPPOLL: Failed to retrieve CCCP config. %v", err)
				continue
			}

			hostName, err := hostFromHostPort(pipeline.Address())
			if err != nil {
				logErrorf("CCCPPOLL: Failed to parse source address. %v", err)
				continue
			}

			bk, err := parseConfig(cccpBytes, hostName)
			if err != nil {
				logDebugf("CCCPPOLL: Failed to parse CCCP config. %v", err)
				continue
			}

			foundConfig = bk
			break
		}

		if foundConfig == nil {
			logDebugf("CCCPPOLL: Failed to retrieve config from any node.")
			continue
		}

		logDebugf("CCCPPOLL: Received new config")
		agent.updateConfig(foundConfig)
	}
}

func (agent *Agent) connect(memdAddrs, httpAddrs []string, deadline time.Time) error {
	logDebugf("Attempting to connect...")

	for _, thisHostPort := range memdAddrs {
		logDebugf("Trying server at %s", thisHostPort)

		srvDeadlineTm := time.Now().Add(agent.serverConnectTimeout)
		if srvDeadlineTm.After(deadline) {
			srvDeadlineTm = deadline
		}

		logDebugf("Trying to connect")
		client, err := agent.dialMemdClient(thisHostPort)
		if IsErrorStatus(err, StatusAuthError) ||
			IsErrorStatus(err, StatusAccessError) {
			return err
		} else if err != nil {
			logDebugf("Connecting failed! %v", err)
			continue
		}

		disconnectClient := func() {
			err := client.Close()
			if err != nil {
				logErrorf("Failed to shut down client connection (%s)", err)
			}
		}

		syncCli := syncClient{
			client: client,
		}

		logDebugf("Attempting to request CCCP configuration")
		cccpBytes, err := syncCli.ExecCccpRequest(srvDeadlineTm)
		if err != nil {
			logDebugf("Failed to retrieve CCCP config. %v", err)
			disconnectClient()
			continue
		}

		hostName, err := hostFromHostPort(thisHostPort)
		if err != nil {
			logErrorf("Failed to parse CCCP source address. %v", err)
			disconnectClient()
			continue
		}

		bk, err := parseConfig(cccpBytes, hostName)
		if err != nil {
			logDebugf("Failed to parse CCCP configuration. %v", err)
			disconnectClient()
			continue
		}

		if !bk.supportsCccp() {
			logDebugf("Bucket does not support CCCP")
			disconnectClient()
			break
		}

		routeCfg := buildRouteConfig(bk, agent.IsSecure())
		if !routeCfg.IsValid() {
			logDebugf("Configuration was deemed invalid")
			disconnectClient()
			continue
		}

		logDebugf("Successfully connected")

		// Build some fake routing data, this is used to indicate that
		//  client is "alive".  A nil routeData causes immediate shutdown.
		agent.routingInfo.Update(nil, &routeData{
			revId: -1,
		})

		// TODO(brett19): Save the client that we build for bootstrap
		disconnectClient()

		if routeCfg.vbMap != nil {
			agent.numVbuckets = routeCfg.vbMap.NumVbuckets()
		} else {
			agent.numVbuckets = 0
		}

		agent.applyConfig(routeCfg)

		go agent.cccpLooper()

		return nil
	}

	signal := make(chan error, 1)

	var epList []string
	for _, hostPort := range httpAddrs {
		if !agent.IsSecure() {
			epList = append(epList, fmt.Sprintf("http://%s", hostPort))
		} else {
			epList = append(epList, fmt.Sprintf("https://%s", hostPort))
		}
	}
	agent.routingInfo.Update(nil, &routeData{
		revId:      -1,
		mgmtEpList: epList,
	})

	var routeCfg *routeConfig

	logDebugf("Starting HTTP looper! %v", epList)
	go agent.httpLooper(func(cfg *cfgBucket, err error) bool {
		if err != nil {
			signal <- err
			return true
		}

		newRouteCfg := buildRouteConfig(cfg, agent.IsSecure())
		if !newRouteCfg.IsValid() {
			// Something is invalid about this config, keep trying
			return false
		}

		routeCfg = newRouteCfg
		signal <- nil
		return true
	})

	err := <-signal
	if err != nil {
		return err
	}

	if routeCfg.vbMap != nil {
		agent.numVbuckets = routeCfg.vbMap.NumVbuckets()
	} else {
		agent.numVbuckets = 0
	}

	agent.applyConfig(routeCfg)

	return nil
}

// Close shuts down the agent, disconnecting from all servers and failing
// any outstanding operations with ErrShutdown.
func (agent *Agent) Close() error {
	agent.configLock.Lock()

	// Clear the routingInfo so no new operations are performed
	//   and retrieve the last active routing configuration
	routingInfo := agent.routingInfo.Clear()
	if routingInfo == nil {
		agent.configLock.Unlock()
		return ErrShutdown
	}

	// Shut down the client multiplexer which will close all its queues
	// effectively causing all the clients to shut down.
	muxCloseErr := routingInfo.clientMux.Close()

	// Drain all the pipelines and error their requests, then
	//  drain the dead queue and error those requests.
	routingInfo.clientMux.Drain(func(req *memdQRequest) {
		req.tryCallback(nil, ErrShutdown)
	})

	agent.configLock.Unlock()

	return muxCloseErr
}

// IsSecure returns whether this client is connected via SSL.
func (agent *Agent) IsSecure() bool {
	return agent.tlsConfig != nil
}

// BucketUUID returns the UUID of the bucket we are connected to.
func (agent *Agent) BucketUUID() string {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return ""
	}

	return routingInfo.uuid
}

// KeyToVbucket translates a particular key to its assigned vbucket.
func (agent *Agent) KeyToVbucket(key []byte) uint16 {
	// TODO(brett19): The KeyToVbucket Bucket API should return an error

	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return 0
	}

	if routingInfo.vbMap == nil {
		return 0
	}

	return routingInfo.vbMap.VbucketByKey(key)
}

// KeyToServer translates a particular key to its assigned server index.
func (agent *Agent) KeyToServer(key []byte, replicaIdx uint32) int {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return -1
	}

	if routingInfo.vbMap == nil {
		return -1
	}

	serverIdx, err := routingInfo.vbMap.NodeByKey(key, replicaIdx)
	if err != nil {
		return -1
	}
	return serverIdx
}

// VbucketToServer returns the server index for a particular vbucket.
func (agent *Agent) VbucketToServer(vbID uint16, replicaIdx uint32) int {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return -1
	}

	if routingInfo.vbMap == nil {
		return -1
	}

	serverIdx, err := routingInfo.vbMap.NodeByVbucket(vbID, replicaIdx)
	if err != nil {
		return -1
	}

	return serverIdx
}

// NumVbuckets returns the number of VBuckets configured on the
// connected cluster.
func (agent *Agent) NumVbuckets() int {
	return agent.numVbuckets
}

// NumReplicas returns the number of replicas configured on the
// connected cluster.
func (agent *Agent) NumReplicas() int {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return 0
	}

	if routingInfo.vbMap == nil {
		return 0
	}

	return routingInfo.vbMap.NumReplicas()
}

// NumServers returns the number of servers accessible for K/V.
func (agent *Agent) NumServers() int {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return 0
	}
	return routingInfo.clientMux.NumPipelines()
}

// TODO(brett19): Update VbucketsOnServer to return all servers.
// Otherwise, we could race the route map update and get a
// non-continuous list of vbuckets for each server.

// VbucketsOnServer returns the list of VBuckets for a server.
func (agent *Agent) VbucketsOnServer(index int) []uint16 {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return nil
	}

	if routingInfo.vbMap == nil {
		return nil
	}

	vbList := routingInfo.vbMap.VbucketsByServer(0)

	if len(vbList) <= index {
		// Invalid server index
		return nil
	}

	return vbList[index]
}

// CapiEps returns all the available endpoints for performing
// map-reduce queries.
func (agent *Agent) CapiEps() []string {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.capiEpList
}

// MgmtEps returns all the available endpoints for performing
// management queries.
func (agent *Agent) MgmtEps() []string {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.mgmtEpList
}

// N1qlEps returns all the available endpoints for performing
// N1QL queries.
func (agent *Agent) N1qlEps() []string {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.n1qlEpList
}

// FtsEps returns all the available endpoints for performing
// FTS queries.
func (agent *Agent) FtsEps() []string {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.ftsEpList
}
