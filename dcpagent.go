package gocbcore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

// DCPAgent represents the base client handling DCP connections to a Couchbase Server.
type DCPAgent struct {
	clientID   string
	bucketName string
	initFn     memdInitFunc

	pollerController configPollerController
	kvMux            *kvMux
	httpMux          *httpMux
	dialer           *memdClientDialerComponent

	cfgManager  *configManagementComponent
	errMap      *errMapComponent
	tracer      *tracerComponent
	diagnostics *diagnosticsComponent
	dcp         *dcpComponent
	http        *httpComponent

	// These connection settings are only ever changed when ForceReconnect or ReconfigureSecurity are called.
	connectionSettingsLock sync.Mutex
	auth                   AuthProvider
	authMechanisms         []AuthMechanism
	tlsConfig              *dynTLSConfig
}

// CreateDcpAgent creates an agent for performing DCP operations.
func CreateDcpAgent(config *DCPAgentConfig, dcpStreamName string, openFlags memd.DcpOpenFlag) (*DCPAgent, error) {
	logInfof("SDK Version: gocbcore/%s", goCbCoreVersionStr)
	logInfof("Creating new dcp agent: %+v", config)

	userAgent := config.UserAgent
	disableDecompression := config.CompressionConfig.DisableDecompression
	useCompression := config.CompressionConfig.Enabled
	useCollections := config.IoConfig.UseCollections
	useJSONHello := !config.IoConfig.DisableJSONHello
	usePITRHello := config.IoConfig.EnablePITRHello
	useXErrorHello := !config.IoConfig.DisableXErrorHello
	useSyncReplicationHello := !config.IoConfig.DisableSyncReplicationHello
	dcpBufferSize := 20 * 1024 * 1024
	compressionMinSize := 32
	compressionMinRatio := 0.83
	dcpBackfillOrderStr := ""
	dcpPriorityStr := ""

	kvConnectTimeout := 7000 * time.Millisecond
	if config.KVConfig.ConnectTimeout > 0 {
		kvConnectTimeout = config.KVConfig.ConnectTimeout
	}

	serverWaitTimeout := 5 * time.Second

	kvPoolSize := 1
	if config.KVConfig.PoolSize > 0 {
		kvPoolSize = config.KVConfig.PoolSize
	}

	maxQueueSize := 2048
	if config.KVConfig.MaxQueueSize > 0 {
		maxQueueSize = config.KVConfig.MaxQueueSize
	}

	kvBufferSize := uint(0)
	if config.KVConfig.ConnectionBufferSize > 0 {
		kvBufferSize = config.KVConfig.ConnectionBufferSize
	}

	confCccpMaxWait := 3 * time.Second
	if config.ConfigPollerConfig.CccpMaxWait > 0 {
		confCccpMaxWait = config.ConfigPollerConfig.CccpMaxWait
	}

	confCccpPollPeriod := 2500 * time.Millisecond
	if config.ConfigPollerConfig.CccpPollPeriod > 0 {
		confCccpPollPeriod = config.ConfigPollerConfig.CccpPollPeriod
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

	if config.CompressionConfig.MinSize > 0 {
		compressionMinSize = config.CompressionConfig.MinSize
	}
	if config.CompressionConfig.MinRatio > 0 {
		compressionMinRatio = config.CompressionConfig.MinRatio
		if compressionMinRatio >= 1.0 {
			compressionMinRatio = 1.0
		}
	}

	if config.DCPConfig.BufferSize > 0 {
		dcpBufferSize = config.DCPConfig.BufferSize
	}
	dcpQueueSize := (dcpBufferSize + 23) / 24

	switch config.DCPConfig.AgentPriority {
	case DcpAgentPriorityLow:
		dcpPriorityStr = "low"
	case DcpAgentPriorityMed:
		dcpPriorityStr = "medium"
	case DcpAgentPriorityHigh:
		dcpPriorityStr = "high"
	}

	// If the user doesn't explicitly set the backfill order, the DCP control flag will not be sent to the cluster
	// and the default will implicitly be used (which is 'round-robin').
	switch config.DCPConfig.BackfillOrder {
	case DCPBackfillOrderRoundRobin:
		dcpBackfillOrderStr = "round-robin"
	case DCPBackfillOrderSequential:
		dcpBackfillOrderStr = "sequential"
	}

	tracerCmpt := newTracerComponent(noopTracer{}, config.BucketName, false, nil)

	// We wrap the authorization system to force DCP channel opening
	//   as part of the "initialization" for any servers.
	initFn := func(client *memdClient, deadline time.Time) error {
		sclient := &syncClient{client: client}
		if err := sclient.ExecOpenDcpConsumer(dcpStreamName, openFlags, deadline); err != nil {
			return err
		}

		if err := sclient.ExecEnableDcpNoop(180*time.Second, deadline); err != nil {
			return err
		}

		if dcpPriorityStr != "" {
			if err := sclient.ExecDcpControl("set_priority", dcpPriorityStr, deadline); err != nil {
				return err
			}
		}

		if config.DCPConfig.UseExpiryOpcode {
			if err := sclient.ExecDcpControl("enable_expiry_opcode", "true", deadline); err != nil {
				return err
			}
		}

		if config.DCPConfig.UseStreamID {
			if err := sclient.ExecDcpControl("enable_stream_id", "true", deadline); err != nil {
				return err
			}
		}

		if config.DCPConfig.UseOSOBackfill {
			if err := sclient.ExecDcpControl("enable_out_of_order_snapshots", "true", deadline); err != nil {
				return err
			}
		}

		if dcpBackfillOrderStr != "" {
			if err := sclient.ExecDcpControl("backfill_order", dcpBackfillOrderStr, deadline); err != nil {
				return err
			}
		}

		if !config.DCPConfig.DisableBufferAcknowledgement {
			if err := sclient.ExecEnableDcpBufferAck(dcpBufferSize, deadline); err != nil {
				return err
			}
		}

		return sclient.ExecEnableDcpClientEnd(deadline)
	}

	c := &DCPAgent{
		clientID:   formatCbUID(randomCbUID()),
		bucketName: config.BucketName,
		initFn:     initFn,
		tracer:     tracerCmpt,

		errMap: newErrMapManager(config.BucketName),
		auth:   config.SecurityConfig.Auth,
	}

	c.authMechanisms = authMechanismsFromConfig(config.SecurityConfig.AuthMechanisms, config.SecurityConfig.UseTLS)

	var tlsConfig *dynTLSConfig
	if config.SecurityConfig.UseTLS {
		tlsConfig = createTLSConfig(config.SecurityConfig.Auth, config.SecurityConfig.TLSRootCAProvider)
	}
	c.tlsConfig = tlsConfig

	circuitBreakerConfig := CircuitBreakerConfig{
		Enabled: false,
	}

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
			NetworkType:   config.IoConfig.NetworkType,
			SrcMemdAddrs:  srcMemdAddrs,
			SrcHTTPAddrs:  srcHTTPAddrs,
			UseTLS:        tlsConfig != nil,
			NoTLSSeedNode: config.SecurityConfig.NoTLSSeedNode,
		},
	)

	c.dialer = newMemdClientDialerComponent(
		memdClientDialerProps{
			ServerWaitTimeout:    serverWaitTimeout,
			KVConnectTimeout:     kvConnectTimeout,
			ClientID:             c.clientID,
			DCPQueueSize:         dcpQueueSize,
			CompressionMinSize:   compressionMinSize,
			CompressionMinRatio:  compressionMinRatio,
			DisableDecompression: disableDecompression,
			NoTLSSeedNode:        config.SecurityConfig.NoTLSSeedNode,
			ConnBufSize:          kvBufferSize,
		},
		bootstrapProps{
			HelloProps: helloProps{
				CollectionsEnabled:     useCollections,
				CompressionEnabled:     useCompression,
				JSONFeatureEnabled:     useJSONHello,
				PITRFeatureEnabled:     usePITRHello,
				XErrorFeatureEnabled:   useXErrorHello,
				SyncReplicationEnabled: useSyncReplicationHello,
			},
			Bucket:        c.bucketName,
			UserAgent:     userAgent,
			ErrMapManager: c.errMap,
		},
		circuitBreakerConfig,
		nil,
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
	c.httpMux = newHTTPMux(
		circuitBreakerConfig,
		c.cfgManager,
		&httpClientMux{tlsConfig: tlsConfig, auth: config.SecurityConfig.Auth},
		config.SecurityConfig.NoTLSSeedNode,
	)
	c.http = newHTTPComponent(
		httpComponentProps{
			UserAgent: userAgent,
		},
		httpClientProps{
			maxIdleConns:        config.HTTPConfig.MaxIdleConns,
			maxIdleConnsPerHost: config.HTTPConfig.MaxIdleConnsPerHost,
			idleTimeout:         config.HTTPConfig.IdleConnectionTimeout,
		},
		c.httpMux,
		c.tracer,
	)

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

	c.diagnostics = newDiagnosticsComponent(c.kvMux, nil, nil, c.bucketName, newFailFastRetryStrategy(), c.pollerController)
	c.dcp = newDcpComponent(c.kvMux, config.DCPConfig.UseStreamID)

	c.dialer.AddBootstrapFailHandler(c)
	c.dialer.AddBootstrapFailHandler(c.diagnostics)

	// Kick everything off.
	cfg := &routeConfig{
		kvServerList: kvServerList,
		mgmtEpList:   httpEpList,
		revID:        -1,
	}

	c.httpMux.OnNewRouteConfig(cfg)
	c.kvMux.OnNewRouteConfig(cfg)

	go c.pollerController.Start()

	return c, nil
}

// IsSecure returns whether this client is connected via SSL.
func (agent *DCPAgent) IsSecure() bool {
	return agent.kvMux.IsSecure()
}

// Close shuts down the agent, disconnecting from all servers and failing
// any outstanding operations with ErrShutdown.
func (agent *DCPAgent) Close() error {
	routeCloseErr := agent.kvMux.Close()
	agent.pollerController.Stop()

	// Wait for our external looper goroutines to finish, note that if the
	// specific looper wasn't used, it will be a nil value otherwise it
	// will be an open channel till its closed to signal completion.
	<-agent.pollerController.Done()

	return routeCloseErr
}

// WaitUntilReady returns whether or not the Agent has seen a valid cluster config.
func (agent *DCPAgent) WaitUntilReady(deadline time.Time, opts WaitUntilReadyOptions,
	cb WaitUntilReadyCallback) (PendingOp, error) {
	return agent.diagnostics.WaitUntilReady(deadline, opts, cb)
}

// OpenStream opens a DCP stream for a particular VBucket and, optionally, filter.
func (agent *DCPAgent) OpenStream(vbID uint16, flags memd.DcpStreamAddFlag, vbUUID VbUUID, startSeqNo,
	endSeqNo, snapStartSeqNo, snapEndSeqNo SeqNo, evtHandler StreamObserver, opts OpenStreamOptions,
	cb OpenStreamCallback) (PendingOp, error) {
	return agent.dcp.OpenStream(vbID, flags, vbUUID, startSeqNo, endSeqNo, snapStartSeqNo, snapEndSeqNo, evtHandler, opts, cb)
}

// CloseStream shuts down an open stream for the specified VBucket.
func (agent *DCPAgent) CloseStream(vbID uint16, opts CloseStreamOptions, cb CloseStreamCallback) (PendingOp, error) {
	return agent.dcp.CloseStream(vbID, opts, cb)
}

// GetFailoverLog retrieves the fail-over log for a particular VBucket.  This is used
// to resume an interrupted stream after a node fail-over has occurred.
func (agent *DCPAgent) GetFailoverLog(vbID uint16, cb GetFailoverLogCallback) (PendingOp, error) {
	return agent.dcp.GetFailoverLog(vbID, cb)
}

// GetVbucketSeqnos returns the last checkpoint for a particular VBucket.  This is useful
// for starting a DCP stream from wherever the server currently is.
func (agent *DCPAgent) GetVbucketSeqnos(serverIdx int, state memd.VbucketState, opts GetVbucketSeqnoOptions,
	cb GetVBucketSeqnosCallback) (PendingOp, error) {
	return agent.dcp.GetVbucketSeqnos(serverIdx, state, opts, cb)
}

// HasCollectionsSupport verifies whether or not collections are available on the agent.
func (agent *DCPAgent) HasCollectionsSupport() bool {
	return agent.kvMux.SupportsCollections()
}

// ConfigSnapshot returns a snapshot of the underlying configuration currently in use.
func (agent *DCPAgent) ConfigSnapshot() (*ConfigSnapshot, error) {
	return agent.kvMux.ConfigSnapshot()
}

// ForceReconnect gracefully rebuilds all connections being used by the agent.
// Any persistent in flight requests (e.g. DCP) will be terminated with ErrForcedReconnect.
//
// Internal: This should never be used and is not supported.
func (agent *DCPAgent) ForceReconnect() {
	agent.connectionSettingsLock.Lock()
	auth := agent.auth
	mechs := agent.authMechanisms
	tlsConfig := agent.tlsConfig
	agent.connectionSettingsLock.Unlock()
	agent.kvMux.ForceReconnect(tlsConfig, mechs, auth, true)
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
func (agent *DCPAgent) ReconfigureSecurity(opts ReconfigureSecurityOptions) error {
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

func (agent *DCPAgent) onBootstrapFail(err error) {
	// If this error is a legitimate fallback reason then we should immediately start the http poller.
	if agent.pollerController != nil && agent.isPollingFallbackError(err) {
		agent.pollerController.ForceHTTPPoller()
	}
}

func (agent *DCPAgent) isPollingFallbackError(err error) bool {
	return isPollingFallbackError(err, agent.bucketName)
}
