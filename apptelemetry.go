package gocbcore

import (
	"sync"
	"time"
)

// TelemetryReporterConfig specifies options related to the ns_server telemetry reporter
// Volatile: This API is subject to change at any time.
type TelemetryReporterConfig struct {
	// ExternalEndpoint overrides the reporting endpoint discovered through the cluster config.
	ExternalEndpoint string

	// Backoff specifies the duration to wait before attempting to reconnect to a telemetry endpoint.
	// Defaults to 5 seconds.
	Backoff time.Duration

	// PingInterval specifies the time duration to wait between consecutive PING commands to the app telemetry server.
	// Defaults to 30 seconds.
	PingInterval time.Duration

	// PingTimeout specifies the time to allow the server to respond to a PING, before closing the connection and
	// attempting to reconnect.
	// Defaults to 2 seconds.
	PingTimeout time.Duration
}

type telemetryClient interface {
	connectIfNotStarted()
	connectionInitiated() bool
	Close()
	usesExternalEndpoint() bool
	updateEndpoints(endpoints telemetryEndpoints)
}

type TelemetryReporter struct {
	client  telemetryClient
	metrics telemetryStore
}

func (t *TelemetryReporter) started() bool {
	if t.client != nil {
		return t.client.connectionInitiated()
	}
	return false
}

func (t *TelemetryReporter) maybeStart() {
	if t.client != nil {
		t.client.connectIfNotStarted()
	}
}

func (t *TelemetryReporter) Close() {
	if t.client != nil {
		logDebugf("Closing telemetry reporter")
		t.client.Close()
	}
}

func (t *TelemetryReporter) updateEndpoints(endpoints telemetryEndpoints) {
	if t.client != nil {
		t.client.updateEndpoints(endpoints)
	}
}

// recordOperationCompletion records the completion of an operation from the user's perspective either successfully
// or with a cancellation/timeout.
func (t *TelemetryReporter) recordOperationCompletion(outcome telemetryOutcome, attributes telemetryOperationAttributes) {
	t.metrics.recordOperationCompletion(outcome, attributes)
}

// recordOrphanedResponse records the latency of a request for an operation that was cancelled or timed out, after
// we have received the (now orphaned) response from the server.
func (t *TelemetryReporter) recordOrphanedResponse(attributes telemetryOperationAttributes) {
	t.metrics.recordOrphanedResponse(attributes)
}

// exportMetrics returns the serialized metrics and resets all metrics histograms and counters.
func (t *TelemetryReporter) exportMetrics() string {
	return t.metrics.serialize()
}

// CreateTelemetryReporter creates a new app telemetry reporter.
func CreateTelemetryReporter(cfg TelemetryReporterConfig) *TelemetryReporter {
	t := &TelemetryReporter{
		metrics: newTelemetryMetrics(),
	}
	client := newTelemetryWebsocketClient(cfg.Backoff, cfg.PingInterval, cfg.PingTimeout, func() string {
		return t.exportMetrics()
	})

	if cfg.ExternalEndpoint != "" {
		client.fixedEndpoints = true
		eps := telemetryEndpoints{
			epList: []routeEndpoint{{Address: cfg.ExternalEndpoint}},
		}
		client.endpoints = eps
		client.connectIfNotStarted()
	}
	t.client = client
	return t
}

type telemetryComponent struct {
	reporter   *TelemetryReporter
	cfgMgr     configManager
	agent      string
	bucketName string

	mutableDataLock sync.Mutex
	mutableData     telemetryComponentMutableData
}

type telemetryComponentMutableData struct {
	auth      AuthProvider
	tlsConfig *dynTLSConfig
	srcConfig *routeConfig
}

type telemetryComponentProps struct {
	reporter   *TelemetryReporter
	agent      string
	bucketName string
	cfgMgr     configManager
	auth       AuthProvider
	tlsConfig  *dynTLSConfig
}

func newTelemetryComponent(props telemetryComponentProps) *telemetryComponent {
	tc := &telemetryComponent{
		cfgMgr:     props.cfgMgr,
		agent:      props.agent,
		bucketName: props.bucketName,

		mutableData: telemetryComponentMutableData{
			auth:      props.auth,
			tlsConfig: props.tlsConfig,
			srcConfig: nil,
		},
	}
	if props.reporter != nil {
		tc.reporter = props.reporter
	}
	if tc.reporter != nil {
		tc.cfgMgr.AddConfigWatcher(tc)
	}

	return tc
}

func (tc *telemetryComponent) TelemetryEnabled() bool {
	return tc != nil && tc.reporter != nil
}

func (tc *telemetryComponent) UpdateTLS(tlsConfig *dynTLSConfig, auth AuthProvider) {
	tc.mutableDataLock.Lock()
	defer tc.mutableDataLock.Unlock()

	// In theory it's possible that we could get here before a route config has been seen,
	// in which case there are no endpoints to update.
	if tc.mutableData.srcConfig != nil {
		tc.updateEndpoints(tlsConfig, auth, tc.mutableData.srcConfig)
	}

	tc.mutableData = telemetryComponentMutableData{
		auth:      auth,
		tlsConfig: tlsConfig,
		srcConfig: tc.mutableData.srcConfig,
	}
}

func (tc *telemetryComponent) updateEndpoints(tlsConfig *dynTLSConfig, auth AuthProvider, cfg *routeConfig) {
	eps := telemetryEndpoints{
		tlsConfig: tlsConfig,
		auth:      auth,
	}
	if tlsConfig != nil {
		eps.epList = cfg.appTelemetryEpList.SSLEndpoints
	} else {
		eps.epList = cfg.appTelemetryEpList.NonSSLEndpoints
	}

	tc.reporter.updateEndpoints(eps)
}

func (tc *telemetryComponent) OnNewRouteConfig(cfg *routeConfig) {
	tc.mutableDataLock.Lock()
	defer tc.mutableDataLock.Unlock()

	eps := telemetryEndpoints{
		tlsConfig: tc.mutableData.tlsConfig,
		auth:      tc.mutableData.auth,
	}
	if tc.mutableData.tlsConfig != nil {
		eps.epList = cfg.appTelemetryEpList.SSLEndpoints
	} else {
		eps.epList = cfg.appTelemetryEpList.NonSSLEndpoints
	}

	tc.reporter.updateEndpoints(eps)

	if len(eps.epList) > 0 {
		// This calls telemetryClient.connectIfNotStarted which does the connection asynchronously.
		// Connecting must not block this goroutine.
		tc.reporter.maybeStart()
	}

	tc.mutableData = telemetryComponentMutableData{
		auth:      tc.mutableData.auth,
		tlsConfig: tc.mutableData.tlsConfig,
		srcConfig: cfg,
	}
}

func (tc *telemetryComponent) RecordOp(outcome telemetryOutcome, attributes telemetryOperationAttributes) {
	if tc.reporter == nil {
		return
	}
	if !tc.reporter.started() {
		return
	}

	attributes.agent = tc.agent
	if attributes.service == MemdService {
		attributes.bucket = tc.bucketName
	}

	tc.reporter.recordOperationCompletion(outcome, attributes)
}

func (tc *telemetryComponent) RecordOrphanedResponse(attributes telemetryOperationAttributes) {
	if tc.reporter == nil {
		return
	}
	if !tc.reporter.started() {
		return
	}

	attributes.agent = tc.agent
	if attributes.service == MemdService {
		attributes.bucket = tc.bucketName
	}

	tc.reporter.recordOrphanedResponse(attributes)
}
