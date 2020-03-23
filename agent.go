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
	clientID       string
	userAgent      string
	auth           AuthProvider
	authHandler    authFuncHandler
	authMechanisms []AuthMechanism
	bucketName     string
	bucketLock     sync.Mutex
	tlsConfig      *tls.Config
	initFn         memdInitFunc

	tracer RequestTracer

	serverFailuresLock sync.Mutex
	serverFailures     map[string]time.Time

	httpComponent *httpComponent

	kvConnectTimeout  time.Duration
	serverWaitTimeout time.Duration

	zombieLock      sync.RWMutex
	zombieOps       []*zombieLogEntry
	useZombieLogger uint32

	dcpPriority  DcpAgentPriority
	useDcpExpiry bool

	cidMgr *collectionIDManager

	defaultRetryStrategy RetryStrategy

	circuitBreakerConfig CircuitBreakerConfig

	cfgManager       *configManager
	pollerController *pollerController
	kvMux            *kvMux
	httpMux          *httpMux
	errMapManager    *errMapManager

	n1qlCmpt      *n1qlQueryComponent
	analyticsCmpt *analyticsQueryComponent
	searchCmpt    *searchQueryComponent
	viewCmpt      *viewQueryComponent
	waitCmpt      *waitUntilConfigComponent

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
	return agent.httpComponent.cli
}

// AuthFunc is invoked by the agent to authenticate a client. This function returns two channels to allow for for multi-stage
// authentication processes (such as SCRAM). The continue callback should be called when further asynchronous bootstrapping
// requests (such as select bucket) can be sent. The completed callback should be called when authentication is completed,
// or failed. It should contain any error that occurred. If completed is called before continue then continue will be called
// first internally, the success value will be determined by whether or not an error is present.
type AuthFunc func(client AuthClient, deadline time.Time, continueCb func(), completedCb func(error)) error

// authFunc wraps AuthFunc to provide a better to the user.
type authFunc func() (completedCh chan BytesAndError, continueCh chan bool, err error)

type authFuncHandler func(client AuthClient, deadline time.Time, mechanism AuthMechanism) authFunc

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
				cert, err := config.Auth.Certificate(AuthCertRequest{})
				if err != nil {
					return nil, err
				}

				if cert == nil {
					return &tls.Certificate{}, nil
				}

				return cert, nil
			},
			InsecureSkipVerify: config.TLSSkipVerify,
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

	c := &Agent{
		clientID:   formatCbUID(randomCbUID()),
		userAgent:  config.UserAgent,
		bucketName: config.BucketName,
		auth:       config.Auth,
		tlsConfig:  tlsConfig,
		initFn:     initFn,
		tracer:     tracer,

		serverFailures:       make(map[string]time.Time),
		kvConnectTimeout:     7000 * time.Millisecond,
		serverWaitTimeout:    5 * time.Second,
		dcpPriority:          config.DcpAgentPriority,
		useDcpExpiry:         config.UseDCPExpiry,
		defaultRetryStrategy: config.DefaultRetryStrategy,
		circuitBreakerConfig: config.CircuitBreakerConfig,
		errMapManager:        newErrMapManager(config.BucketName),

		agentConfig: agentConfig{
			useMutationTokens:    config.UseMutationTokens,
			disableDecompression: config.DisableDecompression,
			useCompression:       config.UseCompression,
			useCollections:       config.UseCollections,
			compressionMinSize:   32,
			compressionMinRatio:  0.83,
			useDurations:         config.UseDurations,
			noRootTraceSpans:     config.NoRootTraceSpans,
		},
	}

	if config.KVConnectTimeout > 0 {
		c.kvConnectTimeout = config.KVConnectTimeout
	}

	kvPoolSize := 1
	if config.KvPoolSize > 0 {
		kvPoolSize = config.KvPoolSize
	}

	maxQueueSize := 2048
	if config.MaxQueueSize > 0 {
		maxQueueSize = config.MaxQueueSize
	}

	confHTTPRetryDelay := 10 * time.Second
	if config.HTTPRetryDelay > 0 {
		confHTTPRetryDelay = config.HTTPRetryDelay
	}

	confHTTPRedialPeriod := 10 * time.Second
	if config.HTTPRedialPeriod > 0 {
		confHTTPRedialPeriod = config.HTTPRedialPeriod
	}

	confCccpMaxWait := 3 * time.Second
	if config.CccpMaxWait > 0 {
		confCccpMaxWait = config.CccpMaxWait
	}

	confCccpPollPeriod := 2500 * time.Millisecond
	if config.CccpPollPeriod > 0 {
		confCccpPollPeriod = config.CccpPollPeriod
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

	c.cidMgr = newCollectionIDManager(c, maxQueueSize)

	var httpEpList []string
	for _, hostPort := range config.HTTPAddrs {
		if !c.IsSecure() {
			httpEpList = append(httpEpList, fmt.Sprintf("http://%s", hostPort))
		} else {
			httpEpList = append(httpEpList, fmt.Sprintf("https://%s", hostPort))
		}
	}

	c.cfgManager = newConfigManager(
		configManagerProperties{
			NetworkType:  config.NetworkType,
			UseSSL:       config.UseTLS,
			SrcMemdAddrs: config.MemdAddrs,
			SrcHTTPAddrs: httpEpList,
		},
		c.onInvalidConfig,
	)

	c.kvMux = newKVMux(
		kvMuxProps{
			queueSize:          maxQueueSize,
			poolSize:           kvPoolSize,
			collectionsEnabled: c.useCollections,
		},
		c.cfgManager,
		c.slowDialMemdClient,
	)
	c.httpMux = newHTTPMux(c.circuitBreakerConfig, c.cfgManager)
	c.httpComponent = newHTTPComponent(httpCli, c.httpMux, c.auth, c.userAgent)
	c.pollerController = newPollerController(
		newCCCPConfigController(
			cccpPollerProperties{
				confCccpMaxWait:    confCccpMaxWait,
				confCccpPollPeriod: confCccpPollPeriod,
			},
			c.kvMux,
			c.cfgManager,
		),
		newHTTPConfigController(
			c.bucket(),
			httpPollerProperties{
				httpComponent:        c.httpComponent,
				confHTTPRetryDelay:   confHTTPRetryDelay,
				confHTTPRedialPeriod: confHTTPRedialPeriod,
			},
			c.httpMux,
			c.cfgManager,
		),
	)
	c.n1qlCmpt = newN1QLQueryComponent(c.httpComponent, c.cfgManager)
	c.analyticsCmpt = newAnalyticsQueryComponent(c.httpComponent)
	c.searchCmpt = newSearchQueryComponent(c.httpComponent)
	c.viewCmpt = newViewQueryComponent(c.httpComponent)
	c.waitCmpt = newWaitUntilConfigComponent(c.cfgManager)

	c.authMechanisms = []AuthMechanism{
		ScramSha512AuthMechanism,
		ScramSha256AuthMechanism,
		ScramSha1AuthMechanism}

	// PLAIN authentication is only supported over TLS
	if config.UseTLS {
		c.authMechanisms = append(c.authMechanisms, PlainAuthMechanism)
	}

	if c.authHandler == nil {
		c.authHandler = c.buildAuthHandler()
	}

	cfg := &routeConfig{
		kvServerList: config.MemdAddrs,
		mgmtEpList:   httpEpList,
		revID:        -1,
	}

	c.httpMux.OnNewRouteConfig(cfg)
	c.kvMux.OnNewRouteConfig(cfg)

	go c.pollerController.Start()

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

func (agent *Agent) buildAuthHandler() authFuncHandler {
	return func(client AuthClient, deadline time.Time, mechanism AuthMechanism) authFunc {
		creds, err := getKvAuthCreds(agent.auth, client.Address())
		if err != nil {
			return nil
		}

		if creds.Username != "" || creds.Password != "" {
			return func() (chan BytesAndError, chan bool, error) {
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

		return nil
	}
}

func (agent *Agent) disconnectClient(client *memdClient) {
	err := client.Close()
	if err != nil {
		logErrorf("Failed to shut down client connection (%s)", err)
	}
}

func (agent *Agent) onInvalidConfig() {
	err := agent.Close()
	if err != nil {
		logErrorf("Invalid config caused agent close failure (%s)", err)
	}
}

// Close shuts down the agent, disconnecting from all servers and failing
// any outstanding operations with ErrShutdown.
func (agent *Agent) Close() error {
	routeCloseErr := agent.kvMux.Close()
	agent.pollerController.Stop()

	// Wait for our external looper goroutines to finish, note that if the
	// specific looper wasn't used, it will be a nil value otherwise it
	// will be an open channel till its closed to signal completion.
	<-agent.pollerController.Done()

	// Close the transports so that they don't hold open goroutines.
	agent.httpComponent.Close()

	return routeCloseErr
}

// IsSecure returns whether this client is connected via SSL.
func (agent *Agent) IsSecure() bool {
	return agent.tlsConfig != nil
}

// BucketUUID returns the UUID of the bucket we are connected to.
func (agent *Agent) BucketUUID() string {
	return agent.kvMux.ConfigUUID()
}

// KeyToVbucket translates a particular key to its assigned vbucket.
func (agent *Agent) KeyToVbucket(key []byte) uint16 {
	return agent.kvMux.KeyToVbucket(key)
}

// KeyToServer translates a particular key to its assigned server index.
func (agent *Agent) KeyToServer(key []byte, replicaIdx uint32) int {
	return agent.kvMux.KeyToServer(key, replicaIdx)
}

// VbucketToServer returns the server index for a particular vbucket.
func (agent *Agent) VbucketToServer(vbID uint16, replicaIdx uint32) int {
	return agent.kvMux.VbucketToServer(vbID, replicaIdx)
}

// NumVbuckets returns the number of VBuckets configured on the
// connected cluster.
func (agent *Agent) NumVbuckets() int {
	return agent.kvMux.NumVBuckets()
}

// NumReplicas returns the number of replicas configured on the
// connected cluster.
func (agent *Agent) NumReplicas() int {
	return agent.kvMux.NumReplicas()
}

// NumServers returns the number of servers accessible for K/V.
func (agent *Agent) NumServers() int {
	return agent.kvMux.NumPipelines()
}

// VbucketsOnServer returns the list of VBuckets for a server.
func (agent *Agent) VbucketsOnServer(index int) []uint16 {
	return agent.kvMux.VbucketsOnServer(index)
}

// ClientID returns the unique id for this agent
func (agent *Agent) ClientID() string {
	return agent.clientID
}

// CapiEps returns all the available endpoints for performing
// map-reduce queries.
func (agent *Agent) CapiEps() []string {
	return agent.httpMux.CapiEps()
}

// MgmtEps returns all the available endpoints for performing
// management queries.
func (agent *Agent) MgmtEps() []string {
	return agent.httpMux.MgmtEps()
}

// N1qlEps returns all the available endpoints for performing
// N1QL queries.
func (agent *Agent) N1qlEps() []string {
	return agent.httpMux.N1qlEps()
}

// FtsEps returns all the available endpoints for performing
// FTS queries.
func (agent *Agent) FtsEps() []string {
	return agent.httpMux.FtsEps()
}

// CbasEps returns all the available endpoints for performing
// CBAS queries.
func (agent *Agent) CbasEps() []string {
	return agent.httpMux.CbasEps()
}

// HasCollectionsSupport verifies whether or not collections are available on the agent.
func (agent *Agent) HasCollectionsSupport() bool {
	return agent.kvMux.SupportsCollections()
}

// UsingGCCCP returns whether or not the Agent is currently using GCCCP polling.
func (agent *Agent) UsingGCCCP() bool {
	return agent.kvMux.SupportsGCCCP()
}

// WaitUntilReady returns whether or not the Agent has seen a valid cluster config.
func (agent *Agent) WaitUntilReady(cb func()) (PendingOp, error) {
	return agent.waitCmpt.WaitUntilFirstConfig(cb)
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
