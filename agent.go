// Package gocbcore implements methods for low-level communication
// with a Couchbase Server cluster.
package gocbcore

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Agent represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	clientID             string
	bucketName           string
	initFn               memdInitFunc
	defaultRetryStrategy RetryStrategy

	pollerController configPollerController
	kvMux            *kvMux
	httpMux          *httpMux
	dialer           *memdClientDialerComponent

	cfgManager   *configManagementComponent
	errMap       *errMapComponent
	collections  *collectionsComponent
	tracer       *tracerComponent
	http         *httpComponent
	diagnostics  *diagnosticsComponent
	crud         *crudComponent
	observe      *observeComponent
	stats        *statsComponent
	n1ql         *n1qlQueryComponent
	analytics    *analyticsQueryComponent
	search       *searchQueryComponent
	views        *viewQueryComponent
	zombieLogger *zombieLoggerComponent

	// These connection settings are only ever changed when ForceReconnect or ReconfigureSecurity are called.
	connectionSettingsLock sync.Mutex
	auth                   AuthProvider
	authMechanisms         []AuthMechanism
	tlsConfig              *dynTLSConfig
}

// HTTPClient returns a pre-configured HTTP Client for communicating with
// Couchbase Server.  You must still specify authentication information
// for any dispatched requests.
func (agent *Agent) HTTPClient() *http.Client {
	return agent.http.cli
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
	initFn := func(client *memdClient, deadline time.Time) error {
		return nil
	}

	return createAgent(config, initFn)
}

func createAgent(config *AgentConfig, initFn memdInitFunc) (*Agent, error) {
	logInfof("SDK Version: gocbcore/%s", goCbCoreVersionStr)
	logInfof("Creating new agent: %+v", config)

	tracer := config.TracerConfig.Tracer
	if tracer == nil {
		tracer = noopTracer{}
	}

	tracerCmpt := newTracerComponent(tracer, config.BucketName, config.TracerConfig.NoRootTraceSpans, config.MeterConfig.Meter)

	c := &Agent{
		clientID:   formatCbUID(randomCbUID()),
		bucketName: config.BucketName,
		initFn:     initFn,
		tracer:     tracerCmpt,

		defaultRetryStrategy: config.DefaultRetryStrategy,

		errMap: newErrMapManager(config.BucketName),
		auth:   config.SecurityConfig.Auth,
	}

	var tlsConfig *dynTLSConfig
	if config.SecurityConfig.UseTLS {
		if config.SecurityConfig.TLSRootCAProvider == nil {
			return nil, wrapError(errInvalidArgument, "must provide TLSRootCAProvider when UseTls is true")
		}
		tlsConfig = createTLSConfig(config.SecurityConfig.Auth, config.SecurityConfig.TLSRootCAProvider)
	}
	c.tlsConfig = tlsConfig

	httpIdleConnTimeout := 4500 * time.Millisecond
	if config.HTTPConfig.IdleConnectionTimeout > 0 {
		httpIdleConnTimeout = config.HTTPConfig.IdleConnectionTimeout
	}

	circuitBreakerConfig := config.CircuitBreakerConfig
	userAgent := config.UserAgent
	useMutationTokens := config.IoConfig.UseMutationTokens
	disableDecompression := config.CompressionConfig.DisableDecompression
	useCompression := config.CompressionConfig.Enabled
	useCollections := config.IoConfig.UseCollections
	useJSONHello := !config.IoConfig.DisableJSONHello
	usePITRHello := config.IoConfig.EnablePITRHello
	useXErrorHello := !config.IoConfig.DisableXErrorHello
	useSyncReplicationHello := !config.IoConfig.DisableSyncReplicationHello
	compressionMinSize := 32
	compressionMinRatio := 0.83
	useDurations := config.IoConfig.UseDurations
	useOutOfOrder := config.IoConfig.UseOutOfOrderResponses

	kvConnectTimeout := 7000 * time.Millisecond
	if config.KVConfig.ConnectTimeout > 0 {
		kvConnectTimeout = config.KVConfig.ConnectTimeout
	}

	serverWaitTimeout := 5 * time.Second
	if config.KVConfig.ServerWaitBackoff > 0 {
		serverWaitTimeout = config.KVConfig.ServerWaitBackoff
	}

	kvPoolSize := 1
	if config.KVConfig.PoolSize > 0 {
		kvPoolSize = config.KVConfig.PoolSize
	}

	maxQueueSize := 2048
	if config.KVConfig.MaxQueueSize > 0 {
		maxQueueSize = config.KVConfig.MaxQueueSize
	}

	confHTTPRetryDelay := 10 * time.Second
	if config.ConfigPollerConfig.HTTPRetryDelay > 0 {
		confHTTPRetryDelay = config.ConfigPollerConfig.HTTPRetryDelay
	}

	confHTTPRedialPeriod := 10 * time.Second
	if config.ConfigPollerConfig.HTTPRedialPeriod > 0 {
		confHTTPRedialPeriod = config.ConfigPollerConfig.HTTPRedialPeriod
	}

	confHTTPMaxWait := 5 * time.Second
	if config.ConfigPollerConfig.HTTPMaxWait > 0 {
		confHTTPMaxWait = config.ConfigPollerConfig.HTTPMaxWait
	}

	confCccpMaxWait := 3 * time.Second
	if config.ConfigPollerConfig.CccpMaxWait > 0 {
		confCccpMaxWait = config.ConfigPollerConfig.CccpMaxWait
	}

	confCccpPollPeriod := 2500 * time.Millisecond
	if config.ConfigPollerConfig.CccpPollPeriod > 0 {
		confCccpPollPeriod = config.ConfigPollerConfig.CccpPollPeriod
	}

	if config.CompressionConfig.MinSize > 0 {
		compressionMinSize = config.CompressionConfig.MinSize
	}
	if config.CompressionConfig.MinRatio > 0 {
		compressionMinRatio = config.CompressionConfig.MinRatio
		if compressionMinRatio >= 1.0 {
			compressionMinRatio = 1.0
		}
	}
	if c.defaultRetryStrategy == nil {
		c.defaultRetryStrategy = newFailFastRetryStrategy()
	}

	c.authMechanisms = authMechanismsFromConfig(config.SecurityConfig.AuthMechanisms, tlsConfig != nil)

	httpEpList := routeEndpoints{}
	var srcHTTPAddrs []routeEndpoint
	for _, hostPort := range config.SeedConfig.HTTPAddrs {
		if config.SecurityConfig.UseTLS && !config.SecurityConfig.NoTLSSeedNode {
			ep := routeEndpoint{
				Address:    fmt.Sprintf("https://%s", hostPort),
				IsSeedNode: true,
			}
			httpEpList.SSLEndpoints = append(httpEpList.SSLEndpoints, ep)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		} else {
			ep := routeEndpoint{
				Address:    fmt.Sprintf("http://%s", hostPort),
				IsSeedNode: true,
			}
			httpEpList.NonSSLEndpoints = append(httpEpList.NonSSLEndpoints, ep)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		}
	}

	if config.OrphanReporterConfig.Enabled {
		zombieLoggerInterval := 10 * time.Second
		zombieLoggerSampleSize := 10
		if config.OrphanReporterConfig.ReportInterval > 0 {
			zombieLoggerInterval = config.OrphanReporterConfig.ReportInterval
		}
		if config.OrphanReporterConfig.SampleSize > 0 {
			zombieLoggerSampleSize = config.OrphanReporterConfig.SampleSize
		}

		c.zombieLogger = newZombieLoggerComponent(zombieLoggerInterval, zombieLoggerSampleSize)
		go c.zombieLogger.Start()
	}

	kvServerList := routeEndpoints{}
	var srcMemdAddrs []routeEndpoint
	for _, seed := range config.SeedConfig.MemdAddrs {
		if config.SecurityConfig.UseTLS && !config.SecurityConfig.NoTLSSeedNode {
			kvServerList.SSLEndpoints = append(kvServerList.SSLEndpoints, routeEndpoint{
				Address:    seed,
				IsSeedNode: true,
			})
			srcMemdAddrs = kvServerList.SSLEndpoints
		} else {
			kvServerList.NonSSLEndpoints = append(kvServerList.NonSSLEndpoints, routeEndpoint{
				Address:    seed,
				IsSeedNode: true,
			})
			srcMemdAddrs = kvServerList.NonSSLEndpoints
		}
	}

	c.cfgManager = newConfigManager(
		configManagerProperties{
			NetworkType:  config.IoConfig.NetworkType,
			SrcMemdAddrs: srcMemdAddrs,
			SrcHTTPAddrs: srcHTTPAddrs,
			UseTLS:       tlsConfig != nil,
		},
	)

	c.dialer = newMemdClientDialerComponent(
		memdClientDialerProps{
			ServerWaitTimeout:    serverWaitTimeout,
			KVConnectTimeout:     kvConnectTimeout,
			ClientID:             c.clientID,
			CompressionMinSize:   compressionMinSize,
			CompressionMinRatio:  compressionMinRatio,
			DisableDecompression: disableDecompression,
			NoTLSSeedNode:        config.SecurityConfig.NoTLSSeedNode,
		},
		bootstrapProps{
			HelloProps: helloProps{
				CollectionsEnabled:     useCollections,
				MutationTokensEnabled:  useMutationTokens,
				CompressionEnabled:     useCompression,
				DurationsEnabled:       useDurations,
				OutOfOrderEnabled:      useOutOfOrder,
				JSONFeatureEnabled:     useJSONHello,
				XErrorFeatureEnabled:   useXErrorHello,
				SyncReplicationEnabled: useSyncReplicationHello,
				PITRFeatureEnabled:     usePITRHello,
			},
			Bucket:        c.bucketName,
			UserAgent:     userAgent,
			ErrMapManager: c.errMap,
		},
		circuitBreakerConfig,
		c.zombieLogger,
		c.tracer,
		initFn,
	)
	c.kvMux = newKVMux(
		kvMuxProps{
			QueueSize:          maxQueueSize,
			PoolSize:           kvPoolSize,
			CollectionsEnabled: useCollections,
			NoTLSSeedNode:      config.SecurityConfig.NoTLSSeedNode,
		},
		c.cfgManager,
		c.errMap,
		c.tracer,
		c.dialer,
		&kvMuxState{
			tlsConfig:      tlsConfig,
			authMechanisms: c.authMechanisms,
			auth:           config.SecurityConfig.Auth,
		},
	)
	c.collections = newCollectionIDManager(
		collectionIDProps{
			MaxQueueSize:         config.KVConfig.MaxQueueSize,
			DefaultRetryStrategy: c.defaultRetryStrategy,
		},
		c.kvMux,
		c.tracer,
		c.cfgManager,
	)
	c.httpMux = newHTTPMux(
		circuitBreakerConfig,
		c.cfgManager,
		&httpClientMux{tlsConfig: tlsConfig, auth: config.SecurityConfig.Auth},
		config.SecurityConfig.NoTLSSeedNode,
	)
	c.http = newHTTPComponent(
		httpComponentProps{
			UserAgent:            userAgent,
			DefaultRetryStrategy: c.defaultRetryStrategy,
		},
		httpClientProps{
			maxIdleConns:        config.HTTPConfig.MaxIdleConns,
			maxIdleConnsPerHost: config.HTTPConfig.MaxIdleConnsPerHost,
			idleTimeout:         httpIdleConnTimeout,
		},
		c.httpMux,
		c.tracer,
	)

	if len(config.SeedConfig.MemdAddrs) == 0 && config.BucketName == "" {
		// The http poller can't run without a bucket. We don't trigger an error for this case
		// because AgentGroup users who use memcached buckets on non-default ports will end up here.
		logDebugf("No bucket name specified and only http addresses specified, not running config poller")
		c.diagnostics = newDiagnosticsComponent(c.kvMux, c.httpMux, c.http, c.bucketName, c.defaultRetryStrategy, nil)
	} else {
		var poller configPollerController
		if config.SecurityConfig.NoTLSSeedNode {
			poller = newSeedConfigController(srcHTTPAddrs[0].Address, c.bucketName,
				httpPollerProperties{
					httpComponent:        c.http,
					confHTTPRetryDelay:   confHTTPRetryDelay,
					confHTTPRedialPeriod: confHTTPRedialPeriod,
					confHTTPMaxWait:      confHTTPMaxWait,
				}, c.cfgManager)
		} else {
			var httpPoller *httpConfigController
			if c.bucketName != "" {
				httpPoller = newHTTPConfigController(
					c.bucketName,
					httpPollerProperties{
						httpComponent:        c.http,
						confHTTPRetryDelay:   confHTTPRetryDelay,
						confHTTPRedialPeriod: confHTTPRedialPeriod,
						confHTTPMaxWait:      confHTTPMaxWait,
					},
					c.httpMux,
					c.cfgManager,
				)
			}
			poller = newPollerController(
				newCCCPConfigController(
					cccpPollerProperties{
						confCccpMaxWait:    confCccpMaxWait,
						confCccpPollPeriod: confCccpPollPeriod,
					},
					c.kvMux,
					c.cfgManager,
					c.isPollingFallbackError,
				),
				httpPoller,
				c.cfgManager,
				c.isPollingFallbackError,
			)
		}
		c.pollerController = poller
		c.diagnostics = newDiagnosticsComponent(c.kvMux, c.httpMux, c.http, c.bucketName, c.defaultRetryStrategy, c.pollerController)
	}
	c.dialer.AddBootstrapFailHandler(c)
	c.dialer.AddBootstrapFailHandler(c.diagnostics)

	c.observe = newObserveComponent(c.collections, c.defaultRetryStrategy, c.tracer, c.kvMux)
	c.crud = newCRUDComponent(c.collections, c.defaultRetryStrategy, c.tracer, c.errMap, c.kvMux)
	c.stats = newStatsComponent(c.kvMux, c.defaultRetryStrategy, c.tracer)
	c.n1ql = newN1QLQueryComponent(c.http, c.cfgManager, c.tracer)
	c.analytics = newAnalyticsQueryComponent(c.http, c.tracer)
	c.search = newSearchQueryComponent(c.http, c.tracer)
	c.views = newViewQueryComponent(c.http, c.tracer)

	// Kick everything off.
	cfg := &routeConfig{
		kvServerList: kvServerList,
		mgmtEpList:   httpEpList,
		revID:        -1,
	}

	c.httpMux.OnNewRouteConfig(cfg)
	c.kvMux.OnNewRouteConfig(cfg)

	if c.pollerController != nil {
		go c.pollerController.Start()
	}

	return c, nil
}

// Close shuts down the agent, disconnecting from all servers and failing
// any outstanding operations with ErrShutdown.
func (agent *Agent) Close() error {
	poller := agent.pollerController
	if poller != nil {
		poller.Stop()
	}

	routeCloseErr := agent.kvMux.Close()

	if agent.zombieLogger != nil {
		agent.zombieLogger.Stop()
	}

	if poller != nil {
		// Wait for our external looper goroutines to finish, note that if the
		// specific looper wasn't used, it will be a nil value otherwise it
		// will be an open channel till its closed to signal completion.
		pollerCh := poller.Done()
		if pollerCh != nil {
			<-pollerCh
		}
	}

	// Close the transports so that they don't hold open goroutines.
	agent.http.Close()

	return routeCloseErr
}

// ClientID returns the unique id for this agent
func (agent *Agent) ClientID() string {
	return agent.clientID
}

// MemdEps returns all the available endpoints for performing KV/DCP operations (using the memcached binary protocol).
// As apposed to other endpoints, these will have the 'couchbase(s)://' scheme prefix.
func (agent *Agent) MemdEps() []string {
	snapshot, err := agent.kvMux.PipelineSnapshot()
	if err != nil {
		return []string{}
	}
	return snapshot.state.KVEps()
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

// EventingEps returns all the available endpoints for managing/interacting with the Eventing Service.
func (agent *Agent) EventingEps() []string {
	return agent.httpMux.EventingEps()
}

// GSIEps returns all the available endpoints for managing/interacting with the GSI Service.
func (agent *Agent) GSIEps() []string {
	return agent.httpMux.GSIEps()
}

// BackupEps returns all the available endpoints for managing/interacting with the Backup Service.
func (agent *Agent) BackupEps() []string {
	return agent.httpMux.BackupEps()
}

// HasCollectionsSupport verifies whether or not collections are available on the agent.
func (agent *Agent) HasCollectionsSupport() bool {
	return agent.kvMux.SupportsCollections()
}

// IsSecure returns whether this client is connected via SSL.
func (agent *Agent) IsSecure() bool {
	return agent.kvMux.IsSecure()
}

// UsingGCCCP returns whether or not the Agent is currently using GCCCP polling.
func (agent *Agent) UsingGCCCP() bool {
	return agent.kvMux.SupportsGCCCP()
}

// HasSeenConfig returns whether or not the Agent has seen a valid cluster config. This does not mean that the agent
// currently has active connections.
// Volatile: This API is subject to change at any time.
func (agent *Agent) HasSeenConfig() (bool, error) {
	seen, err := agent.kvMux.ConfigRev()
	if err != nil {
		return false, err
	}

	return seen > -1, nil
}

// WaitUntilReady returns whether or not the Agent has seen a valid cluster config.
func (agent *Agent) WaitUntilReady(deadline time.Time, opts WaitUntilReadyOptions, cb WaitUntilReadyCallback) (PendingOp, error) {
	return agent.diagnostics.WaitUntilReady(deadline, opts, cb)
}

// ConfigSnapshot returns a snapshot of the underlying configuration currently in use.
func (agent *Agent) ConfigSnapshot() (*ConfigSnapshot, error) {
	return agent.kvMux.ConfigSnapshot()
}

// BucketName returns the name of the bucket that the agent is using, if any.
// Uncommitted: This API may change in the future.
func (agent *Agent) BucketName() string {
	return agent.bucketName
}

// ForceReconnect gracefully rebuilds all connections being used by the agent.
// Any persistent in flight requests (e.g. DCP) will be terminated with ErrForcedReconnect.
//
// Internal: This should never be used and is not supported.
func (agent *Agent) ForceReconnect() {
	agent.connectionSettingsLock.Lock()
	auth := agent.auth
	mechs := agent.authMechanisms
	tlsConfig := agent.tlsConfig
	agent.connectionSettingsLock.Unlock()
	agent.kvMux.ForceReconnect(tlsConfig, mechs, auth, true)
}

// ReconfigureSecurityOptions are the options available to the ReconfigureSecurity function.
type ReconfigureSecurityOptions struct {
	UseTLS bool
	// If is nil will default to the TLSRootCAProvider already in use by the agent.
	TLSRootCAProvider func() *x509.CertPool

	Auth AuthProvider

	// AuthMechanisms is the list of mechanisms that the SDK can use to attempt authentication.
	// Note that if you add PLAIN to the list, this will cause credential leakage on the network
	// since PLAIN sends the credentials in cleartext. It is disabled by default to prevent downgrade attacks. We
	// recommend using a TLS connection if using PLAIN.
	// If is nil will default to the AuthMechanisms already in use by the Agent.
	AuthMechanisms []AuthMechanism
}

// ReconfigureSecurity updates the security configuration being used by the agent. This includes the ability to
// toggle TLS on and off.
//
// Calling this function will cause all underlying connections to be reconnected. The exception to this is the
// connection to the seed node (usually localhost), which will only be reconnected if the AuthProvider is provided
// on the options.
//
// This function can only be called when the seed poller is in use i.e. when the ns_server scheme is used.
// Internal: This should never be used and is not supported.
func (agent *Agent) ReconfigureSecurity(opts ReconfigureSecurityOptions) error {
	_, ok := agent.pollerController.(*seedConfigController)
	if !ok {
		return errors.New("reconfigure tls is only supported when the agent is in ns server mode")
	}

	var authProvided bool
	auth := opts.Auth
	mechs := opts.AuthMechanisms
	agent.connectionSettingsLock.Lock()
	if auth == nil {
		auth = agent.auth
	} else {
		authProvided = true
	}
	if len(mechs) == 0 {
		mechs = agent.authMechanisms
	}

	var tlsConfig *dynTLSConfig
	if opts.UseTLS {
		if opts.TLSRootCAProvider == nil {
			return wrapError(errInvalidArgument, "must provide TLSRootCAProvider when UseTLS is true")
		}
		tlsConfig = createTLSConfig(auth, opts.TLSRootCAProvider)
	}

	agent.auth = auth
	agent.authMechanisms = mechs
	agent.tlsConfig = tlsConfig
	agent.connectionSettingsLock.Unlock()

	agent.cfgManager.UseTLS(tlsConfig != nil)
	agent.kvMux.ForceReconnect(tlsConfig, mechs, auth, authProvided)
	agent.httpMux.UpdateTLS(tlsConfig, auth)
	return nil
}

func (agent *Agent) onBootstrapFail(err error) {
	// If this error is a legitimate fallback reason then we should immediately start the http poller.
	if agent.pollerController != nil && agent.isPollingFallbackError(err) {
		agent.pollerController.ForceHTTPPoller()
	}
}

func (agent *Agent) isPollingFallbackError(err error) bool {
	return isPollingFallbackError(err, agent.bucketName)
}

func authMechanismsFromConfig(authMechanisms []AuthMechanism, useTLS bool) []AuthMechanism {
	if len(authMechanisms) == 0 {
		if useTLS {
			authMechanisms = []AuthMechanism{PlainAuthMechanism}
		} else {
			// No user specified auth mechanisms so set our defaults.
			authMechanisms = []AuthMechanism{
				ScramSha512AuthMechanism,
				ScramSha256AuthMechanism,
				ScramSha1AuthMechanism}
		}
	} else if !useTLS {
		// The user has specified their own mechanisms and not using TLS so we check if they've set PLAIN.
		for _, mech := range authMechanisms {
			if mech == PlainAuthMechanism {
				logWarnf("PLAIN sends credentials in plaintext, this will cause credential leakage on the network")
			}
		}
	}
	return authMechanisms
}
