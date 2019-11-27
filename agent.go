// Package gocbcore implements methods for low-level communication
// with a Couchbase Server cluster.
package gocbcore

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

type agentConfig struct {
	networkType          string
	useMutationTokens    bool
	useCompression       bool
	useDurations         bool
	disableDecompression bool
	useCollections       bool

	compressionMinSize  int
	compressionMinRatio float64

	noRootTraceSpans bool
}

// Agent represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	clientID    string
	userAgent   string
	auth        AuthProvider
	authHandler authFunc
	bucketName  string
	bucketLock  sync.Mutex
	tlsConfig   *tls.Config
	initFn      memdInitFunc

	closeNotify        chan struct{}
	cccpLooperDoneSig  chan struct{}
	httpLooperDoneSig  chan struct{}
	gcccpLooperDoneSig chan struct{}
	gcccpLooperStopSig chan struct{}

	configLock  sync.Mutex
	routingInfo routeDataPtr
	kvErrorMap  kvErrorMapPtr
	numVbuckets int

	tracer RequestTracer

	serverFailuresLock sync.Mutex
	serverFailures     map[string]time.Time

	httpCli *http.Client

	confHTTPRedialPeriod time.Duration
	confHTTPRetryDelay   time.Duration
	confCccpMaxWait      time.Duration
	confCccpPollPeriod   time.Duration

	kvConnectTimeout  time.Duration
	serverWaitTimeout time.Duration
	kvPoolSize        int
	maxQueueSize      int

	zombieLock      sync.RWMutex
	zombieOps       []*zombieLogEntry
	useZombieLogger uint32

	dcpPriority  DcpAgentPriority
	useDcpExpiry bool

	cidMgr *collectionIDManager

	durabilityLevelStatus durabilityLevelStatus
	clusterCapabilities   uint32
	supportsCollections   bool

	cachedClients       map[string]*memdClient
	cachedClientsLock   sync.Mutex
	cachedHTTPEndpoints []string
	supportsGCCCP       bool

	defaultRetryStrategy RetryStrategy

	circuitBreakerConfig CircuitBreakerConfig

	agentConfig
}

// ServerConnectTimeout gets the timeout for each server connection, including all authentication steps.
func (agent *Agent) ServerConnectTimeout() time.Duration {
	return agent.kvConnectTimeout
}

// SetServerConnectTimeout sets the timeout for each server connection.
func (agent *Agent) SetServerConnectTimeout(timeout time.Duration) {
	agent.kvConnectTimeout = timeout
}

// HTTPClient returns a pre-configured HTTP Client for communicating with
// Couchbase Server.  You must still specify authentication information
// for any dispatched requests.
func (agent *Agent) HTTPClient() *http.Client {
	return agent.httpCli
}

func (agent *Agent) getErrorMap() *kvErrorMap {
	return agent.kvErrorMap.Get()
}

// AuthFunc is invoked by the agent to authenticate a client. This function returns two channels to allow for for multi-stage
// authentication processes (such as SCRAM). The continue callback should be called when further asynchronous bootstrapping
// requests (such as select bucket) can be sent. The completed callback should be called when authentication is completed,
// or failed. It should contain any error that occurred. If completed is called before continue then continue will be called
// first internally, the success value will be determined by whether or not an error is present.
type AuthFunc func(client AuthClient, deadline time.Time, continueCb func(), completedCb func(error)) error

// authFunc wraps AuthFunc to provide a better to the user.
type authFunc func(client AuthClient, deadline time.Time) (completedCh chan BytesAndError, continueCh chan bool, err error)

// CreateAgent creates an agent for performing normal operations.
func CreateAgent(config *AgentConfig) (*Agent, error) {
	initFn := func(client *syncClient, deadline time.Time, agent *Agent) error {
		return nil
	}

	return createAgent(config, initFn)
}

// CreateDcpAgent creates an agent for performing DCP operations.
func CreateDcpAgent(config *AgentConfig, dcpStreamName string, openFlags DcpOpenFlag) (*Agent, error) {
	// We wrap the authorization system to force DCP channel opening
	//   as part of the "initialization" for any servers.
	initFn := func(client *syncClient, deadline time.Time, agent *Agent) error {
		if err := client.ExecOpenDcpConsumer(dcpStreamName, openFlags, deadline); err != nil {
			return err
		}
		if err := client.ExecEnableDcpNoop(180*time.Second, deadline); err != nil {
			return err
		}
		var priority string
		switch agent.dcpPriority {
		case DcpAgentPriorityLow:
			priority = "low"
		case DcpAgentPriorityMed:
			priority = "medium"
		case DcpAgentPriorityHigh:
			priority = "high"
		}
		if err := client.ExecDcpControl("set_priority", priority, deadline); err != nil {
			return err
		}

		if agent.useDcpExpiry {
			if err := client.ExecDcpControl("enable_expiry_opcode", "true", deadline); err != nil {
				return err
			}
		}

		if config.UseDCPStreamID {
			if err := client.ExecDcpControl("enable_stream_id", "true", deadline); err != nil {
				return err
			}
		}

		if err := client.ExecEnableDcpClientEnd(deadline); err != nil {
			return err
		}
		return client.ExecEnableDcpBufferAck(8*1024*1024, deadline)
	}

	return createAgent(config, initFn)
}

func createAgent(config *AgentConfig, initFn memdInitFunc) (*Agent, error) {
	logInfof("SDK Version: gocbcore/%s", goCbCoreVersionStr)
	logInfof("Creating new agent: %+v", config)

	var tlsConfig *tls.Config
	if config.UseTLS {
		tlsConfig = &tls.Config{
			RootCAs: config.TLSRootCAs,
			GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return config.Auth.Certificate(AuthCertRequest{})
			},
		}
	}

	httpTransport := &http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConns:        config.HTTPMaxIdleConns,
		MaxIdleConnsPerHost: config.HTTPMaxIdleConnsPerHost,
		IdleConnTimeout:     config.HTTPIdleConnectionTimeout,
	}
	err := http2.ConfigureTransport(httpTransport)
	if err != nil {
		logDebugf("failed to configure http2: %s", err)
	}

	httpCli := &http.Client{
		Transport: httpTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// All that we're doing here is setting auth on any redirects.
			// For that reason we can just pull it off the oldest (first) request.
			if len(via) >= 10 {
				// Just duplicate the default behaviour for maximum redirects.
				return errors.New("stopped after 10 redirects")
			}

			oldest := via[0]
			auth := oldest.Header.Get("Authorization")
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}

			return nil
		},
	}

	tracer := config.Tracer
	if tracer == nil {
		tracer = noopTracer{}
	}

	maxQueueSize := 2048

	c := &Agent{
		clientID:    formatCbUID(randomCbUID()),
		userAgent:   config.UserAgent,
		bucketName:  config.BucketName,
		auth:        config.Auth,
		tlsConfig:   tlsConfig,
		initFn:      initFn,
		httpCli:     httpCli,
		closeNotify: make(chan struct{}),
		tracer:      tracer,

		serverFailures:        make(map[string]time.Time),
		kvConnectTimeout:      7000 * time.Millisecond,
		serverWaitTimeout:     5 * time.Second,
		kvPoolSize:            1,
		maxQueueSize:          maxQueueSize,
		confHTTPRetryDelay:    10 * time.Second,
		confHTTPRedialPeriod:  10 * time.Second,
		confCccpMaxWait:       3 * time.Second,
		confCccpPollPeriod:    2500 * time.Millisecond,
		dcpPriority:           config.DcpAgentPriority,
		useDcpExpiry:          config.UseDCPExpiry,
		durabilityLevelStatus: durabilityLevelStatusUnknown,
		cachedClients:         make(map[string]*memdClient),
		defaultRetryStrategy:  config.DefaultRetryStrategy,
		circuitBreakerConfig:  config.CircuitBreakerConfig,

		agentConfig: agentConfig{
			networkType:         config.NetworkType,
			useMutationTokens:   config.UseMutationTokens,
			useCompression:      config.UseCompression,
			useCollections:      config.UseCollections,
			compressionMinSize:  32,
			compressionMinRatio: 0.83,
			useDurations:        config.UseDurations,
			noRootTraceSpans:    config.NoRootTraceSpans,
		},
	}
	c.cidMgr = newCollectionIDManager(c, maxQueueSize)

	connectTimeout := 60000 * time.Millisecond
	if config.ConnectTimeout > 0 {
		connectTimeout = config.ConnectTimeout
	}

	if config.KVConnectTimeout > 0 {
		c.kvConnectTimeout = config.KVConnectTimeout
	}
	if config.KvPoolSize > 0 {
		c.kvPoolSize = config.KvPoolSize
	}
	if config.MaxQueueSize > 0 {
		c.maxQueueSize = config.MaxQueueSize
	}
	if config.HTTPRetryDelay > 0 {
		c.confHTTPRetryDelay = config.HTTPRetryDelay
	}
	if config.HTTPRedialPeriod > 0 {
		c.confHTTPRedialPeriod = config.HTTPRedialPeriod
	}
	if config.CccpMaxWait > 0 {
		c.confCccpMaxWait = config.CccpMaxWait
	}
	if config.CccpPollPeriod > 0 {
		c.confCccpPollPeriod = config.CccpPollPeriod
	}
	if config.CompressionMinSize > 0 {
		c.compressionMinSize = config.CompressionMinSize
	}
	if config.CompressionMinRatio > 0 {
		c.compressionMinRatio = config.CompressionMinRatio
		if c.compressionMinRatio >= 1.0 {
			c.compressionMinRatio = 1.0
		}
	}
	if c.defaultRetryStrategy == nil {
		c.defaultRetryStrategy = newFailFastRetryStrategy()
	}

	authMechanisms := []AuthMechanism{
		ScramSha512AuthMechanism,
		ScramSha256AuthMechanism,
		ScramSha1AuthMechanism}

	// PLAIN authentication is only supported over TLS
	if config.UseTLS {
		authMechanisms = append(authMechanisms, PlainAuthMechanism)
	}

	deadline := time.Now().Add(connectTimeout)
	if config.BucketName == "" {
		if err := c.connectG3CP(config.MemdAddrs, config.HTTPAddrs, authMechanisms, deadline); err != nil {
			return nil, err
		}
	} else {
		if err := c.connectWithBucket(config.MemdAddrs, config.HTTPAddrs, authMechanisms, deadline); err != nil {
			return nil, err
		}
	}

	if config.UseZombieLogger {
		// We setup the zombie logger after connecting so that we don't end up leaking the logging goroutine.
		// We also don't enable the zombie logger on the agent until here so that the operations performed
		// when connecting don't trigger a zombie log to occur when the logger isn't yet setup.
		atomic.StoreUint32(&c.useZombieLogger, 1)

		zombieLoggerInterval := 10 * time.Second
		zombieLoggerSampleSize := 10
		if config.ZombieLoggerInterval > 0 {
			zombieLoggerInterval = config.ZombieLoggerInterval
		}
		if config.ZombieLoggerSampleSize > 0 {
			zombieLoggerSampleSize = config.ZombieLoggerSampleSize
		}
		// zombieOps must have a static capacity for its lifetime, the capacity should
		// never be altered so that it is consistent across the zombieLogger and
		// recordZombieResponse.
		c.zombieOps = make([]*zombieLogEntry, 0, zombieLoggerSampleSize)
		go c.zombieLogger(zombieLoggerInterval, zombieLoggerSampleSize)
	}

	return c, nil
}

func (agent *Agent) buildAuthHandler(client AuthClient, authMechanisms []AuthMechanism,
	deadline time.Time) (func(mechanism AuthMechanism), error) {

	if len(authMechanisms) == 0 {
		// If we're using something like client auth then we might not want an auth handler.
		return nil, nil
	}

	var nextAuth func(mechanism AuthMechanism)
	creds, err := getKvAuthCreds(agent.auth, client.Address())
	if err != nil {
		return nil, err
	}

	if creds.Username != "" || creds.Password != "" {
		// If we only have 1 auth mechanism then we've either we've already decided what mechanism to use
		// or the user has only decided to support 1. Either way we don't need to check what the server supports.
		getAuthFunc := func(mechanism AuthMechanism, deadline time.Time) authFunc {
			return func(client AuthClient, deadline time.Time) (chan BytesAndError, chan bool, error) {
				continueCh := make(chan bool, 1)
				completedCh := make(chan BytesAndError, 1)
				hasContinued := int32(0)
				callErr := saslMethod(mechanism, creds.Username, creds.Password, client, deadline, func() {
					// hasContinued should never be 1 here but let's guard against it.
					if atomic.CompareAndSwapInt32(&hasContinued, 0, 1) {
						continueCh <- true
					}
				}, func(err error) {
					if atomic.CompareAndSwapInt32(&hasContinued, 0, 1) {
						sendContinue := true
						if err != nil {
							sendContinue = false
						}
						continueCh <- sendContinue
					}
					completedCh <- BytesAndError{Err: err}
				})
				if callErr != nil {
					return nil, nil, err
				}
				return completedCh, continueCh, nil
			}
		}

		if len(authMechanisms) == 1 {
			agent.authHandler = getAuthFunc(authMechanisms[0], deadline)
		} else {
			nextAuth = func(mechanism AuthMechanism) {
				agent.authHandler = getAuthFunc(mechanism, deadline)
			}
			agent.authHandler = getAuthFunc(authMechanisms[0], deadline)
		}
	}

	return nextAuth, nil
}

func (agent *Agent) connectWithBucket(memdAddrs, httpAddrs []string, authMechanisms []AuthMechanism, deadline time.Time) error {
	for _, thisHostPort := range memdAddrs {
		logDebugf("Trying server at %s for %p", thisHostPort, agent)

		srvDeadlineTm := time.Now().Add(agent.kvConnectTimeout)
		if srvDeadlineTm.After(deadline) {
			srvDeadlineTm = deadline
		}

		logDebugf("Trying to connect %p/%s", agent, thisHostPort)
		client, err := agent.dialMemdClient(thisHostPort, srvDeadlineTm)
		if err != nil {
			logDebugf("Connecting failed %p/%s! %v", agent, thisHostPort, err)
			continue
		}

		syncCli := syncClient{
			client: client,
		}

		var nextAuth func(mechanism AuthMechanism)
		if agent.authHandler == nil {
			nextAuth, err = agent.buildAuthHandler(&syncCli, authMechanisms, srvDeadlineTm)
			if err != nil {
				logDebugf("Building auth failed %p/%s! %v", agent, thisHostPort, err)
				continue
			}
		}

		logDebugf("Trying to bootstrap agent %p against %s", agent, thisHostPort)
		err = agent.bootstrap(client, authMechanisms, nextAuth, srvDeadlineTm)
		if errors.Is(err, ErrAuthenticationFailure) {
			agent.disconnectClient(client)
			return err
		} else if err != nil {
			logDebugf("Bootstrap failed %p/%s! %v", agent, thisHostPort, err)
			agent.disconnectClient(client)
			continue
		}
		logDebugf("Bootstrapped %p/%s", agent, thisHostPort)

		if agent.useCollections {
			agent.supportsCollections = client.SupportsFeature(FeatureCollections)
			if !agent.supportsCollections {
				logDebugf("Collections disabled as unsupported")
			}
		} else {
			agent.supportsCollections = false
		}

		if client.SupportsFeature(FeatureEnhancedDurability) {
			agent.durabilityLevelStatus = durabilityLevelStatusSupported
		} else {
			agent.durabilityLevelStatus = durabilityLevelStatusUnsupported
		}

		logDebugf("Attempting to request CCCP configuration")
		cfgBytes, err := syncCli.ExecGetClusterConfig(srvDeadlineTm)
		if err != nil {
			logDebugf("Failed to retrieve CCCP config %p/%s. %v", agent, thisHostPort, err)
			agent.disconnectClient(client)
			continue
		}

		hostName, err := hostFromHostPort(thisHostPort)
		if err != nil {
			logErrorf("Failed to parse CCCP source address %p/%s. %v", agent, thisHostPort, err)
			agent.disconnectClient(client)
			continue
		}

		bk, err := parseBktConfig(cfgBytes, hostName)
		if err != nil {
			logDebugf("Failed to parse cluster configuration %p/%s. %v", agent, thisHostPort, err)
			agent.disconnectClient(client)
			continue
		}

		if !bk.supportsCccp() {
			logDebugf("Bucket does not support CCCP %p/%s", agent, thisHostPort)
			agent.disconnectClient(client)
			break
		}

		routeCfg := agent.buildFirstRouteConfig(bk, thisHostPort)
		logDebugf("Using network type %s for connections", agent.networkType)
		if !routeCfg.IsValid() {
			logDebugf("Configuration was deemed invalid %+v", routeCfg)
			agent.disconnectClient(client)
			continue
		}

		agent.updateClusterCapabilities(bk)
		logDebugf("Successfully connected agent %p to %s", agent, thisHostPort)

		// Build some fake routing data, this is used to indicate that
		//  client is "alive".  A nil routeData causes immediate shutdown.
		agent.routingInfo.Update(nil, &routeData{
			revID: -1,
		})

		agent.cacheClientNoLock(client)

		if routeCfg.vbMap != nil {
			agent.numVbuckets = routeCfg.vbMap.NumVbuckets()
		} else {
			agent.numVbuckets = 0
		}

		agent.applyRoutingConfig(routeCfg)

		agent.cccpLooperDoneSig = make(chan struct{})
		go agent.cccpLooper()

		return nil
	}

	return agent.tryStartHTTPLooper(httpAddrs)
}

func (agent *Agent) connectG3CP(memdAddrs, httpAddrs []string, authMechanisms []AuthMechanism, deadline time.Time) error {
	logDebugf("Attempting to connect %p...", agent)

	var routeCfg *routeConfig

	for _, thisHostPort := range memdAddrs {
		logDebugf("Trying server at %s for %p", thisHostPort, agent)

		srvDeadlineTm := time.Now().Add(agent.kvConnectTimeout)
		if srvDeadlineTm.After(deadline) {
			srvDeadlineTm = deadline
		}

		logDebugf("Trying to connect %p/%s", agent, thisHostPort)
		client, err := agent.dialMemdClient(thisHostPort, srvDeadlineTm)
		if err != nil {
			logDebugf("Connecting failed %p/%s! %v", agent, thisHostPort, err)
			continue
		}

		syncCli := syncClient{
			client: client,
		}

		var nextAuth func(mechanism AuthMechanism)
		if agent.authHandler == nil {
			nextAuth, err = agent.buildAuthHandler(&syncCli, authMechanisms, srvDeadlineTm)
			if err != nil {
				logDebugf("Building auth failed %p/%s! %v", agent, thisHostPort, err)
				continue
			}
		}

		logDebugf("Trying to bootstrap agent %p against %s", agent, thisHostPort)
		err = agent.bootstrap(client, authMechanisms, nextAuth, srvDeadlineTm)
		if errors.Is(err, ErrAuthenticationFailure) {
			agent.disconnectClient(client)
			for _, cli := range agent.cachedClients {
				agent.disconnectClient(cli)
			}
			return err
		} else if err != nil {
			logDebugf("Bootstrap failed %p/%s! %v", agent, thisHostPort, err)
			agent.cacheClientNoLock(client)
			continue
		}
		logDebugf("Bootstrapped %p/%s", agent, thisHostPort)

		if agent.useCollections {
			agent.supportsCollections = client.SupportsFeature(FeatureCollections)
			if !agent.supportsCollections {
				logDebugf("Collections disabled as unsupported")
			}
		} else {
			agent.supportsCollections = false
		}

		if client.SupportsFeature(FeatureEnhancedDurability) {
			agent.durabilityLevelStatus = durabilityLevelStatusSupported
		} else {
			agent.durabilityLevelStatus = durabilityLevelStatusUnsupported
		}

		logDebugf("Attempting to request CCCP configuration")
		cfgBytes, err := syncCli.ExecGetClusterConfig(srvDeadlineTm)
		if err != nil {
			logDebugf("Failed to retrieve CCCP config %p/%s. %v", agent, thisHostPort, err)
			agent.cacheClientNoLock(client)
			continue
		}

		hostName, err := hostFromHostPort(thisHostPort)
		if err != nil {
			logErrorf("Failed to parse CCCP source address %p/%s. %v", agent, thisHostPort, err)
			agent.cacheClientNoLock(client)
			continue
		}

		cfg, err := parseClusterConfig(cfgBytes, hostName)
		if err != nil {
			logDebugf("Failed to parse cluster configuration %p/%s. %v", agent, thisHostPort, err)
			agent.cacheClientNoLock(client)
			continue
		}

		routeCfg = agent.buildFirstRouteConfig(cfg, thisHostPort)
		logDebugf("Using network type %s for connections", agent.networkType)
		if !routeCfg.IsValid() {
			logDebugf("Configuration was deemed invalid %+v", routeCfg)
			agent.disconnectClient(client)
			continue
		}

		agent.updateClusterCapabilities(cfg)
		logDebugf("Successfully connected agent %p to %s", agent, thisHostPort)
		agent.cacheClientNoLock(client)
	}

	if len(agent.cachedClients) == 0 {
		// If we're using gcccp or if we haven't failed due to cccp then fail.
		logDebugf("No bucket selected and no clients cached, connect failed for %p", agent)
		return errBadHosts
	}

	// In the case of G3CP we don't need to worry about connecting over HTTP as there's no bucket.
	// If we've got cached clients then we made a connection and we want to use gcccp so no errors here.
	agent.cachedHTTPEndpoints = httpAddrs
	if routeCfg == nil {
		// No error but we don't support GCCCP.
		logDebugf("GCCCP unsupported, connections being held in trust.")
		return nil
	}
	agent.supportsGCCCP = true
	// Build some fake routing data, this is used to indicate that
	//  client is "alive".  A nil routeData causes immediate shutdown.
	agent.routingInfo.Update(nil, &routeData{
		revID: -1,
	})

	if routeCfg.vbMap != nil {
		agent.numVbuckets = routeCfg.vbMap.NumVbuckets()
	} else {
		agent.numVbuckets = 0
	}

	agent.applyRoutingConfig(routeCfg)

	agent.gcccpLooperDoneSig = make(chan struct{})
	agent.gcccpLooperStopSig = make(chan struct{})
	go agent.gcccpLooper()

	return nil

}

func (agent *Agent) disconnectClient(client *memdClient) {
	err := client.Close()
	if err != nil {
		logErrorf("Failed to shut down client connection (%s)", err)
	}
}

func (agent *Agent) cacheClientNoLock(client *memdClient) {
	agent.cachedClients[client.Address()] = client
}

func (agent *Agent) tryStartHTTPLooper(httpAddrs []string) error {
	signal := make(chan error, 1)
	var routeCfg *routeConfig

	var epList []string
	for _, hostPort := range httpAddrs {
		if !agent.IsSecure() {
			epList = append(epList, fmt.Sprintf("http://%s", hostPort))
		} else {
			epList = append(epList, fmt.Sprintf("https://%s", hostPort))
		}
	}

	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		agent.routingInfo.Update(nil, &routeData{
			revID:      -1,
			mgmtEpList: epList,
		})
	}

	logDebugf("Starting HTTP looper! %v", epList)
	agent.httpLooperDoneSig = make(chan struct{})
	go agent.httpLooper(func(cfg *cfgBucket, srcServer string, err error) bool {
		if err != nil {
			signal <- err
			return true
		}

		if agent.useCollections {
			agent.supportsCollections = cfg.supports("collections")
			if !agent.supportsCollections {
				logDebugf("Collections disabled as unsupported")
			}
		} else {
			agent.supportsCollections = false
		}

		if cfg.supports("syncreplication") {
			agent.durabilityLevelStatus = durabilityLevelStatusSupported
		} else {
			agent.durabilityLevelStatus = durabilityLevelStatusUnsupported
		}

		newRouteCfg := agent.buildFirstRouteConfig(cfg, srcServer)
		if !newRouteCfg.IsValid() {
			// Something is invalid about this config, keep trying
			return false
		}

		agent.updateClusterCapabilities(cfg)
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

	agent.applyRoutingConfig(routeCfg)

	return nil
}

func (agent *Agent) buildFirstRouteConfig(config cfgObj, srcServer string) *routeConfig {
	if agent.networkType != "" && agent.networkType != "auto" {
		return buildRouteConfig(config, agent.IsSecure(), agent.networkType, true)
	}

	defaultRouteConfig := buildRouteConfig(config, agent.IsSecure(), "default", true)

	// First we check if the source server is from the defaults list
	srcInDefaultConfig := false
	for _, endpoint := range defaultRouteConfig.kvServerList {
		if endpoint == srcServer {
			srcInDefaultConfig = true
		}
	}
	for _, endpoint := range defaultRouteConfig.mgmtEpList {
		if endpoint == srcServer {
			srcInDefaultConfig = true
		}
	}
	if srcInDefaultConfig {
		agent.networkType = "default"
		return defaultRouteConfig
	}

	// Next lets see if we have an external config, if so, default to that
	externalRouteCfg := buildRouteConfig(config, agent.IsSecure(), "external", true)
	if externalRouteCfg.IsValid() {
		agent.networkType = "external"
		return externalRouteCfg
	}

	// If all else fails, default to the implicit default config
	agent.networkType = "default"
	return defaultRouteConfig
}

func (agent *Agent) updateConfig(cfg cfgObj) {
	updated := agent.updateRoutingConfig(cfg)
	if !updated {
		return
	}

	agent.updateClusterCapabilities(cfg)
}

func (agent *Agent) getCachedClient(address string) *memdClient {
	agent.cachedClientsLock.Lock()
	cli, ok := agent.cachedClients[address]
	if !ok {
		agent.cachedClientsLock.Unlock()
		return nil
	}
	delete(agent.cachedClients, address)
	agent.cachedClientsLock.Unlock()

	return cli
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
		return errShutdown
	}

	// Notify everyone that we are shutting down
	close(agent.closeNotify)

	// Shut down the client multiplexer which will close all its queues
	// effectively causing all the clients to shut down.
	muxCloseErr := routingInfo.clientMux.Close()

	// Drain all the pipelines and error their requests, then
	//  drain the dead queue and error those requests.
	routingInfo.clientMux.Drain(func(req *memdQRequest) {
		req.tryCallback(nil, errShutdown)
	})

	agent.configLock.Unlock()

	agent.cachedClientsLock.Lock()
	for _, cli := range agent.cachedClients {
		err := cli.Close()
		if err != nil {
			logDebugf("Failed to close client %p", cli)
		}
	}
	agent.cachedClients = make(map[string]*memdClient)
	agent.cachedClientsLock.Unlock()

	// Wait for our external looper goroutines to finish, note that if the
	// specific looper wasn't used, it will be a nil value otherwise it
	// will be an open channel till its closed to signal completion.
	if agent.cccpLooperDoneSig != nil {
		<-agent.cccpLooperDoneSig
	}
	if agent.gcccpLooperDoneSig != nil {
		<-agent.gcccpLooperDoneSig
	}
	if agent.httpLooperDoneSig != nil {
		<-agent.httpLooperDoneSig
	}

	// Close the transports so that they don't hold open goroutines.
	if tsport, ok := agent.httpCli.Transport.(*http.Transport); ok {
		tsport.CloseIdleConnections()
	} else {
		logDebugf("Could not close idle connections for transport")
	}

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

	if routingInfo.vbMap != nil {
		serverIdx, err := routingInfo.vbMap.NodeByKey(key, replicaIdx)
		if err != nil {
			return -1
		}

		return serverIdx
	}

	if routingInfo.ketamaMap != nil {
		serverIdx, err := routingInfo.ketamaMap.NodeByKey(key)
		if err != nil {
			return -1
		}

		return serverIdx
	}

	return -1
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

func (agent *Agent) bucketType() bucketType {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return bktTypeInvalid
	}

	return routingInfo.bktType
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

// ClientID returns the unique id for this agent
func (agent *Agent) ClientID() string {
	return agent.clientID
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

// CbasEps returns all the available endpoints for performing
// CBAS queries.
func (agent *Agent) CbasEps() []string {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return nil
	}
	return routingInfo.cbasEpList
}

// HasCollectionsSupport verifies whether or not collections are available on the agent.
func (agent *Agent) HasCollectionsSupport() bool {
	return agent.supportsCollections
}

// UsingGCCCP returns whether or not the Agent is currently using GCCCP polling.
func (agent *Agent) UsingGCCCP() bool {
	return agent.supportsGCCCP
}

func (agent *Agent) bucket() string {
	agent.bucketLock.Lock()
	defer agent.bucketLock.Unlock()
	return agent.bucketName
}

func (agent *Agent) setBucket(bucket string) {
	agent.bucketLock.Lock()
	defer agent.bucketLock.Unlock()
	agent.bucketName = bucket
}

// SelectBucket performs a select bucket operation against the cluster.
func (agent *Agent) SelectBucket(bucketName string, deadline time.Time) error {
	if agent.bucket() != "" {
		return errBucketAlreadySelected
	}

	logDebugf("Selecting on %p", agent)

	// Stop the gcccp looper if it's running, if we connected to a node but gcccp wasn't supported then the looper
	// won't be running.
	if agent.gcccpLooperStopSig != nil {
		agent.gcccpLooperStopSig <- struct{}{}
		<-agent.gcccpLooperDoneSig
		logDebugf("GCCCP poller halted for %p", agent)
	}

	agent.setBucket(bucketName)
	var routeCfg *routeConfig

	if agent.UsingGCCCP() {
		routingInfo := agent.routingInfo.Get()
		if routingInfo != nil {
			// We should only get here if cachedClients was empty.
			for i := 0; i < routingInfo.clientMux.NumPipelines(); i++ {
				// Each pipeline should only have 1 connection whilst using GCCCP.
				pipeline := routingInfo.clientMux.GetPipeline(i)
				client := syncClient{
					client: &memdPipelineSenderWrap{
						pipeline: pipeline,
					},
				}
				logDebugf("Selecting bucket against pipeline %p/%s", pipeline, pipeline.Address())

				_, err := client.doBasicOp(cmdSelectBucket, []byte(bucketName), nil, nil, deadline)
				if err != nil {
					// This means that we can't connect to the bucket because something is invalid so bail.
					if errors.Is(err, ErrAuthenticationFailure) {
						agent.setBucket("")
						return err
					}

					// Otherwise close the pipeline and let the later config refresh create a new set of connections to this
					// node.
					logDebugf("Shutting down pipeline %s/%p after failing to select bucket", pipeline.Address(), pipeline)
					err = pipeline.Close()
					if err != nil {
						logDebugf("Failed to shutdown pipeline %s/%p (%v)", pipeline.Address(), pipeline, err)
					}
					continue
				}
				logDebugf("Bucket selected successfully against pipeline %p/%s", pipeline, pipeline.Address())

				//if routeCfg == nil {
				cccpBytes, err := client.ExecGetClusterConfig(deadline)
				if err != nil {
					logDebugf("CCCPPOLL: Failed to retrieve CCCP config. %v", err)
					continue
				}

				hostName, err := hostFromHostPort(pipeline.Address())
				if err != nil {
					logErrorf("CCCPPOLL: Failed to parse source address. %v", err)
					continue
				}

				bk, err := parseBktConfig(cccpBytes, hostName)
				if err != nil {
					logDebugf("CCCPPOLL: Failed to parse CCCP config. %v", err)
					continue
				}

				routeCfg = buildRouteConfig(bk, agent.IsSecure(), agent.networkType, false)
				if !routeCfg.IsValid() {
					logDebugf("Configuration was deemed invalid %+v", routeCfg)
					routeCfg = nil
					continue
				}
				//}
			}
		}
	} else {
		// We don't need to keep the lock on this, if we have cached clients then we don't support gcccp so no pipelines are running.
		agent.cachedClientsLock.Lock()
		clients := agent.cachedClients
		agent.cachedClientsLock.Unlock()

		for _, cli := range clients {
			// waitCh := make(chan error)
			client := syncClient{
				client: cli,
			}

			logDebugf("Selecting bucket against client %p/%s", cli, cli.Address())

			_, err := client.doBasicOp(cmdSelectBucket, []byte(bucketName), nil, nil, deadline)
			if err != nil {
				// This means that we can't connect to the bucket because something is invalid so bail.
				if errors.Is(err, ErrAuthenticationFailure) {
					agent.setBucket("")
					return err
				}

				// Otherwise keep the client around and it'll get used for pipeline client later, it might connect correctly
				// later.
				continue
			}
			logDebugf("Bucket selected successfully against client %p/%s", cli, cli.Address())

			//if routeCfg == nil {
			cccpBytes, err := client.ExecGetClusterConfig(deadline)
			if err != nil {
				logDebugf("CCCPPOLL: Failed to retrieve CCCP config. %v", err)
				continue
			}

			hostName, err := hostFromHostPort(cli.Address())
			if err != nil {
				logErrorf("CCCPPOLL: Failed to parse source address. %v", err)
				continue
			}

			bk, err := parseBktConfig(cccpBytes, hostName)
			if err != nil {
				logDebugf("CCCPPOLL: Failed to parse CCCP config. %v", err)
				continue
			}

			routeCfg = agent.buildFirstRouteConfig(bk, cli.Address())
			if !routeCfg.IsValid() {
				logDebugf("Configuration was deemed invalid %+v", routeCfg)
				routeCfg = nil
				continue
			}
			//}
		}
	}

	if routeCfg == nil || !routeCfg.IsValid() {
		logDebugf("No valid route config created, starting HTTP looper.")
		// If we failed to get a routeCfg then try the http looper instead, this will be the case for memcached buckets.
		err := agent.tryStartHTTPLooper(agent.cachedHTTPEndpoints)
		if err != nil {
			agent.setBucket("")
			return err
		}
		return nil
	}

	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		// Build some fake routing data, this is used to indicate that
		// client is "alive".  A nil routeData causes immediate shutdown.
		// If we don't support GCCP then we could hit this.
		agent.routingInfo.Update(nil, &routeData{
			revID: -1,
		})
	}

	// We need to update the numVbuckets as previously they would have been 0 even if we had been gcccp looping
	if routeCfg.vbMap != nil {
		agent.numVbuckets = routeCfg.vbMap.NumVbuckets()
	} else {
		agent.numVbuckets = 0
	}

	agent.applyRoutingConfig(routeCfg)

	logDebugf("Select bucket completed, starting CCCP looper.")

	agent.cccpLooperDoneSig = make(chan struct{})
	go agent.cccpLooper()
	return nil
}

func (agent *Agent) newMemdClientMux(hostPorts []string) *memdClientMux {
	if agent.bucket() == "" {
		return newMemdClientMux(hostPorts, 1, agent.maxQueueSize, agent.slowDialMemdClient, agent.circuitBreakerConfig)
	}

	return newMemdClientMux(hostPorts, agent.kvPoolSize, agent.maxQueueSize, agent.slowDialMemdClient, agent.circuitBreakerConfig)
}
