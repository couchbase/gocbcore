package gocbcore

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// ColumnarAgent represents the base client handling connections to the Couchbase Columnar service.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
// Internal: This should never be used and is not supported.
type ColumnarAgent struct {
	clientID string

	poller  *cccpConfigController
	kvMux   *kvMux
	httpMux *columnarMux
	dialer  *memdClientDialerComponent

	cfgManager *configManagementComponent
	errMap     *errMapComponent
	columnar   *columnarComponent

	auth      AuthProvider
	tlsConfig *dynTLSConfig

	// not really necessary but without this various things will end up panicking
	tracer *tracerComponent

	srvDetails  *srvDetails
	shutdownSig chan struct{}
	isShutdown  atomic.Bool
}

func CreateColumnarAgent(config *ColumnarAgentConfig) (*ColumnarAgent, error) {
	logInfof("SDK Version: gocbcore/%s", goCbCoreVersionStr)
	logInfof("Creating new columnar agent: %+v", config)

	if config.SecurityConfig.TLSRootCAProvider == nil {
		return nil, wrapError(ErrInvalidArgument, "tlsRootCAProvider cannot be nil")
	}

	tlsConfig := createTLSConfig(config.SecurityConfig.Auth, config.SecurityConfig.CipherSuite, config.SecurityConfig.TLSRootCAProvider)

	c := &ColumnarAgent{
		clientID: formatCbUID(randomCbUID()),

		errMap: newErrMapManager(""),
		auth:   config.SecurityConfig.Auth,

		tlsConfig: tlsConfig,

		shutdownSig: make(chan struct{}),
	}

	serverWaitTimeout := 5 * time.Second
	if config.KVConfig.ServerWaitBackoff > 0 {
		serverWaitTimeout = config.KVConfig.ServerWaitBackoff
	}

	httpIdleConnTimeout := 1000 * time.Millisecond
	if config.HTTPConfig.IdleConnectionTimeout > 0 {
		httpIdleConnTimeout = config.HTTPConfig.IdleConnectionTimeout
	}

	connectTimeout := 10 * time.Second
	if config.ConnectTimeout > 0 {
		connectTimeout = config.ConnectTimeout
	}

	confCccpMaxWait := 3 * time.Second
	if config.ConfigPollerConfig.CccpMaxWait > 0 {
		confCccpMaxWait = config.ConfigPollerConfig.CccpMaxWait
	}

	confCccpPollPeriod := 2500 * time.Millisecond
	if config.ConfigPollerConfig.CccpPollPeriod > 0 {
		confCccpPollPeriod = config.ConfigPollerConfig.CccpPollPeriod
	}

	userAgent := config.UserAgent
	kvPoolSize := 1
	maxQueueSize := 2048
	kvBufferSize := uint(1024)

	kvServerList := routeEndpoints{}
	var srcMemdAddrs []routeEndpoint
	for _, seed := range config.SeedConfig.MemdAddrs {
		kvServerList.SSLEndpoints = append(kvServerList.SSLEndpoints, routeEndpoint{
			Address:    seed,
			IsSeedNode: true,
		})
		srcMemdAddrs = kvServerList.SSLEndpoints
	}

	if config.SeedConfig.SRVRecord != nil {
		c.srvDetails = &srvDetails{
			Addrs:  kvServerList,
			Record: *config.SeedConfig.SRVRecord,
		}
	}

	c.cfgManager = newConfigManager(
		configManagerProperties{
			NetworkType:  "",
			SrcMemdAddrs: srcMemdAddrs,
			SrcHTTPAddrs: []routeEndpoint{},
			UseTLS:       true,
			SeedNodeAddr: "",
		},
	)

	c.tracer = &tracerComponent{
		tracer: noopTracer{},
	}

	c.dialer = newMemdClientDialerComponent(
		memdClientDialerProps{
			ServerWaitTimeout:    serverWaitTimeout,
			KVConnectTimeout:     connectTimeout,
			ClientID:             c.clientID,
			CompressionMinSize:   0,
			CompressionMinRatio:  0,
			DisableDecompression: false,
			NoTLSSeedNode:        false,
			ConnBufSize:          kvBufferSize,
		},
		bootstrapProps{
			HelloProps: helloProps{
				CollectionsEnabled:             true,
				MutationTokensEnabled:          false,
				CompressionEnabled:             false,
				DurationsEnabled:               false,
				OutOfOrderEnabled:              false,
				JSONFeatureEnabled:             false,
				XErrorFeatureEnabled:           false,
				SyncReplicationEnabled:         false,
				PITRFeatureEnabled:             false,
				ResourceUnitsEnabled:           false,
				ClusterMapNotificationsEnabled: true,
			},
			Bucket:        "",
			UserAgent:     userAgent,
			ErrMapManager: c.errMap,
		},
		CircuitBreakerConfig{Enabled: false},
		nil,
		c.tracer,
		nil,
		c.cfgManager,
	)

	c.kvMux = newKVMux(
		kvMuxProps{
			QueueSize:          maxQueueSize,
			PoolSize:           kvPoolSize,
			CollectionsEnabled: true,
			NoTLSSeedNode:      false,
		},
		c.cfgManager,
		c.errMap,
		c.tracer,
		c.dialer,
		&kvMuxState{
			tlsConfig:          tlsConfig,
			authMechanisms:     []AuthMechanism{PlainAuthMechanism},
			auth:               config.SecurityConfig.Auth,
			expectedBucketName: "",
		},
		c.maybeRefreshSRVRecord,
	)

	c.httpMux = newColumnarMux(c.cfgManager, &columnarClientMux{
		tlsConfig: tlsConfig,
		auth:      config.SecurityConfig.Auth,
	})

	c.columnar = newColumnarComponent(columnarComponentProps{
		UserAgent: userAgent,
	}, columnarHTTPClientProps{
		MaxIdleConns:        config.HTTPConfig.MaxIdleConns,
		MaxIdleConnsPerHost: config.HTTPConfig.MaxIdleConnsPerHost,
		IdleTimeout:         httpIdleConnTimeout,
		ConnectTimeout:      connectTimeout,
		MaxConnsPerHost:     config.HTTPConfig.MaxConnsPerHost,
	}, c.httpMux)

	cccpFetcher := newCCCPConfigFetcher(confCccpMaxWait)
	c.poller = newCCCPConfigController(
		cccpPollerProperties{
			confCccpPollPeriod: confCccpPollPeriod,
			cccpConfigFetcher:  cccpFetcher,
		},
		c.kvMux,
		c.cfgManager,
		nil,
	)

	c.cfgManager.SetConfigFetcher(cccpFetcher)
	c.cfgManager.AddConfigWatcher(c.dialer)

	// Kick everything off.
	cfg := &routeConfig{
		kvServerList: kvServerList,
		revID:        -1,
	}

	c.httpMux.OnNewRouteConfig(cfg)
	c.kvMux.OnNewRouteConfig(cfg)

	go func() {
		lastReset := time.Now()
		resetRetriesAfter := 30 * time.Second
		backoff := ExponentialBackoff(1*time.Millisecond, 1*time.Second, 2)
		var retries uint32
		for {
			err := c.poller.DoLoop()
			if err == nil {
				return
			}

			if time.Now().Sub(lastReset) > resetRetriesAfter {
				retries = 0
				lastReset = time.Now()
			}

			if errors.Is(err, errNoCCCPHosts) {
				logInfof("poller exited with no hosts, attempting srv refresh: %v", err)
				c.attemptSRVRefresh()
			} else {
				logInfof("poller exited with error: %v", err)
			}

			dura := backoff(retries)
			retries++

			select {
			case <-c.shutdownSig:
				return
			case <-time.After(dura):
			}

		}
	}()

	return c, nil
}

func (agent *ColumnarAgent) Query(ctx context.Context, opts ColumnarQueryOptions) (*ColumnarRowReader, error) {
	return agent.columnar.Query(ctx, opts)
}

// Close shuts down the agent, disconnecting from all servers and failing
// any outstanding operations with ErrShutdown.
func (agent *ColumnarAgent) Close() error {
	logInfof("Columnar agent closing")
	agent.isShutdown.Store(true)
	poller := agent.poller
	if poller != nil {
		poller.Stop()
	}
	routeCloseErr := agent.kvMux.Close()
	agent.cfgManager.Close()

	// Close the transports so that they don't hold open goroutines.
	agent.columnar.Close()
	close(agent.shutdownSig)

	logInfof("Columnar agent close complete")

	return routeCloseErr
}

// Bits to support maybeRefreshSRVRecord.

// IsSecure returns whether this client is connected via SSL.
func (agent *ColumnarAgent) IsSecure() bool {
	return true
}

func (agent *ColumnarAgent) maybeRefreshSRVRecord() {
	pipelines, err := agent.kvMux.PipelineSnapshot()
	if err == nil {
		pipelines.Iterate(0, func(pipeline *memdPipeline) bool {
			clients := pipeline.Clients()
			for _, cli := range clients {
				err := cli.Error()
				if err != nil {
					logDebugf("Found error in pipeline client %p/%s: %v", cli, cli.address, err)
					agent.columnar.SetBootstrapError(err)
					return true
				}
			}

			return false
		})
	}

	maybeRefreshSRVRecord(agent)
}

func (agent *ColumnarAgent) attemptSRVRefresh() {
	if agent.isShutdown.Load() {
		return
	}

	srvDetails := agent.srv()
	if srvDetails == nil {
		logInfof("no srv record in use, aborting srv refresh")
		return
	}

	attemptSRVRefresh(agent, srvDetails)
}

func (agent *ColumnarAgent) srv() *srvDetails {
	return agent.srvDetails
}

func (agent *ColumnarAgent) setSRVAddrs(addrs routeEndpoints) {
	agent.srvDetails.Addrs = addrs
}

func (agent *ColumnarAgent) routeConfigWatchers() []routeConfigWatcher {
	return agent.cfgManager.Watchers()
}

func (agent *ColumnarAgent) stopped() <-chan struct{} {
	return agent.shutdownSig
}

func (agent *ColumnarAgent) resetConfig() {
	// Reset the config manager to accept the next config that the poller fetches.
	// This is safe to do here, we're blocking the poller from fetching a config and if we're here then
	// we can't be performing ops.
	agent.cfgManager.ResetConfig()
	// Reset the dialer so that the next connections to bootstrap fetch a config and kick off the poller again.
	agent.dialer.ResetConfig()
}
