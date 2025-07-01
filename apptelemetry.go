package gocbcore

import (
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

func (t *TelemetryReporter) recordOp(attributes telemetryOperationAttributes) {
	t.metrics.recordOp(attributes)
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

	auth      AuthProvider
	tlsConfig *dynTLSConfig
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
		auth:       props.auth,
		tlsConfig:  props.tlsConfig,
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

func (tc *telemetryComponent) OnNewRouteConfig(cfg *routeConfig) {
	eps := telemetryEndpoints{
		tlsConfig: tc.tlsConfig,
		auth:      tc.auth,
	}
	if tc.tlsConfig != nil {
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
}

func (tc *telemetryComponent) RecordOp(attributes telemetryOperationAttributes) {
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

	tc.reporter.recordOp(attributes)
}
