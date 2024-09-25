package gocbcore

import (
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// RequestTracer describes the tracing abstraction in the SDK.
type RequestTracer interface {
	RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan
}

// RequestSpan is the interface for spans that are created by a RequestTracer.
type RequestSpan interface {
	End()
	Context() RequestSpanContext
	AddEvent(name string, timestamp time.Time)
	SetAttribute(key string, value interface{})
}

// RequestSpanContext is the interface for external span contexts that can be passed in into the SDK option blocks.
type RequestSpanContext interface {
}

type noopSpan struct{}
type noopSpanContext struct{}

var (
	defaultNoopSpanContext = noopSpanContext{}
	defaultNoopSpan        = noopSpan{}
)

type noopTracer struct {
}

func (tracer noopTracer) RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan {
	return defaultNoopSpan
}

func (span noopSpan) End() {
}

func (span noopSpan) Context() RequestSpanContext {
	return defaultNoopSpanContext
}

func (span noopSpan) SetAttribute(key string, value interface{}) {
}

func (span noopSpan) AddEvent(key string, timestamp time.Time) {
}

type opTracer struct {
	parentContext RequestSpanContext
	opSpan        RequestSpan
}

func (tracer *opTracer) Finish() {
	if tracer.opSpan != nil {
		tracer.opSpan.End()
	}
}

func (tracer *opTracer) RootContext() RequestSpanContext {
	if tracer.opSpan != nil {
		return tracer.opSpan.Context()
	}

	return tracer.parentContext
}

// ClusterLabels encapsulates the cluster UUID and cluster name as published by the server.
type ClusterLabels struct {
	ClusterUUID string
	ClusterName string
}

type tracerComponent struct {
	tracer                    RequestTracer
	bucket                    string
	noRootTraceSpans          bool
	metrics                   Meter
	valueRecorderAttribsCache sync.Map
	cfgMgr                    configManager
	clusterLabels             atomic.Value
}

func newTracerComponent(tracer RequestTracer, bucket string, noRootTraceSpans bool, metrics Meter, cfgMgr configManager) *tracerComponent {
	reqTracer := tracer
	if reqTracer == nil {
		reqTracer = noopTracer{}
	}

	tc := &tracerComponent{
		tracer:           reqTracer,
		bucket:           bucket,
		noRootTraceSpans: noRootTraceSpans,
		metrics:          metrics,
		cfgMgr:           cfgMgr,
	}

	if cfgMgr != nil && (tracer != nil || metrics != nil) {
		cfgMgr.AddConfigWatcher(tc)
	}

	return tc
}

func (tc *tracerComponent) CreateOpTrace(operationName string, parentContext RequestSpanContext) *opTracer {
	if tc.noRootTraceSpans {
		return &opTracer{
			parentContext: parentContext,
			opSpan:        nil,
		}
	}

	opSpan := tc.tracer.RequestSpan(parentContext, operationName)
	opSpan.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		opSpan.SetAttribute(spanAttribClusterNameKey, labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		opSpan.SetAttribute(spanAttribClusterUUIDKey, labels.ClusterUUID)
	}

	return &opTracer{
		parentContext: parentContext,
		opSpan:        opSpan,
	}
}

func (tc *tracerComponent) StartHTTPDispatchSpan(req *httpRequest, name string) RequestSpan {
	span := tc.tracer.RequestSpan(req.RootTraceContext, name)
	return span
}

func (tc *tracerComponent) StopHTTPDispatchSpan(span RequestSpan, req *http.Request, id string, retries uint32) {
	span.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		span.SetAttribute(spanAttribClusterNameKey, labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		span.SetAttribute(spanAttribClusterUUIDKey, labels.ClusterUUID)
	}
	span.SetAttribute(spanAttribNetTransportKey, spanAttribNetTransportValue)
	if id != "" {
		span.SetAttribute(spanAttribOperationIDKey, id)
	}
	remoteName, remotePort, err := net.SplitHostPort(req.Host)
	if err != nil {
		logDebugf("Failed to split host port: %s", err)
	}

	span.SetAttribute(spanAttribNetPeerNameKey, remoteName)
	span.SetAttribute(spanAttribNetPeerPortKey, remotePort)
	span.SetAttribute(spanAttribNumRetries, retries)
	span.End()
}

func (tc *tracerComponent) StartCmdTrace(req *memdQRequest) {
	req.processingLock.Lock()
	if req.cmdTraceSpan != nil {
		req.processingLock.Unlock()
		logWarnf("Attempted to start tracing on traced request OP=0x%x, Opaque=%d", req.Command, req.Opaque)
		return
	}

	if req.RootTraceContext == nil {
		req.processingLock.Unlock()
		return
	}

	req.cmdTraceSpan = tc.tracer.RequestSpan(req.RootTraceContext, req.Packet.Command.Name())
	req.cmdTraceSpan.SetAttribute(spanAttribDBSystemKey, "couchbase")
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		req.cmdTraceSpan.SetAttribute(spanAttribClusterNameKey, labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		req.cmdTraceSpan.SetAttribute(spanAttribClusterUUIDKey, labels.ClusterUUID)
	}
	req.processingLock.Unlock()
}

func (tc *tracerComponent) StartNetTrace(req *memdQRequest) {
	req.processingLock.Lock()
	if req.cmdTraceSpan == nil {
		req.processingLock.Unlock()
		return
	}

	if req.netTraceSpan != nil {
		req.processingLock.Unlock()
		logWarnf("Attempted to start net tracing on traced request")
		return
	}

	req.netTraceSpan = tc.tracer.RequestSpan(req.cmdTraceSpan.Context(), spanNameDispatchToServer)
	req.netTraceSpan.SetAttribute(spanAttribDBSystemKey, "couchbase")
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		req.netTraceSpan.SetAttribute(spanAttribClusterNameKey, labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		req.netTraceSpan.SetAttribute(spanAttribClusterUUIDKey, labels.ClusterUUID)
	}
	req.processingLock.Unlock()
}

func (tc *tracerComponent) ResponseValueRecord(service, operation string, start time.Time) {
	if tc.metrics == nil {
		return
	}
	key := service + "." + operation
	attribs, ok := tc.valueRecorderAttribsCache.Load(key)
	if !ok {
		// It doesn't really matter if we end up storing the attribs against the same key multiple times. We just need
		// to have a read efficient cache that doesn't cause actual data races.
		attribs = map[string]string{
			metricAttribServiceKey: service,
		}
		if operation != "" {
			attribs.(map[string]string)[metricAttribOperationKey] = operation
		}
		clusterLabels := tc.ClusterLabels()
		if clusterLabels.ClusterUUID != "" {
			attribs.(map[string]string)[metricAttribClusterUUIDKey] = clusterLabels.ClusterUUID
		}
		if clusterLabels.ClusterName != "" {
			attribs.(map[string]string)[metricAttribClusterNameKey] = clusterLabels.ClusterName
		}
		tc.valueRecorderAttribsCache.Store(key, attribs)
	}

	recorder, err := tc.metrics.ValueRecorder(meterNameCBOperations, attribs.(map[string]string))
	if err != nil {
		logDebugf("Failed to get value recorder: %v", err)
		return
	}

	duration := uint64(time.Since(start).Microseconds())
	if duration == 0 {
		duration = uint64(1 * time.Microsecond)
	}

	recorder.RecordValue(duration)
}

func (tc *tracerComponent) OnNewRouteConfig(cfg *routeConfig) {
	tc.clusterLabels.Store(ClusterLabels{
		ClusterUUID: cfg.clusterUUID,
		ClusterName: cfg.clusterName,
	})
}

func (tc *tracerComponent) ClusterLabels() ClusterLabels {
	v := tc.clusterLabels.Load()
	if v == nil {
		return ClusterLabels{}
	}
	return v.(ClusterLabels)
}

func stopCmdTraceLocked(req *memdQRequest) {
	if req.RootTraceContext == nil {
		return
	}

	if req.cmdTraceSpan == nil {
		logWarnf("Attempted to stop tracing on untraced request")
		return
	}

	req.cmdTraceSpan.SetAttribute(spanAttribNumRetries, req.RetryAttempts())

	req.cmdTraceSpan.End()
	req.cmdTraceSpan = nil
}

func cancelReqTraceLocked(req *memdQRequest, local, remote string) {
	if req.cmdTraceSpan != nil {
		if req.netTraceSpan != nil {
			stopNetTraceLocked(req, nil, local, remote)
		}

		stopCmdTraceLocked(req)
	}
}

func stopNetTraceLocked(req *memdQRequest, resp *memdQResponse, localAddress, remoteAddress string) {
	if req.cmdTraceSpan == nil {
		return
	}

	if req.netTraceSpan == nil {
		logWarnf("Attempted to stop net tracing on an untraced request")
		return
	}

	req.netTraceSpan.SetAttribute(spanAttribNetTransportKey, spanAttribNetTransportValue)
	if resp != nil {
		req.netTraceSpan.SetAttribute(spanAttribOperationIDKey, strconv.Itoa(int(resp.Opaque)))
		req.netTraceSpan.SetAttribute(spanAttribLocalIDKey, resp.sourceConnID)
	}
	localName, localPort, err := net.SplitHostPort(localAddress)
	if err != nil {
		logDebugf("Failed to split host port: %s", err)
	}

	remoteName, remotePort, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		logDebugf("Failed to split host port: %s", err)
	}

	req.netTraceSpan.SetAttribute(spanAttribNetHostNameKey, localName)
	req.netTraceSpan.SetAttribute(spanAttribNetHostPortKey, localPort)
	req.netTraceSpan.SetAttribute(spanAttribNetPeerNameKey, remoteName)
	req.netTraceSpan.SetAttribute(spanAttribNetPeerPortKey, remotePort)
	if resp != nil && resp.Packet.ServerDurationFrame != nil {
		req.netTraceSpan.SetAttribute(spanAttribServerDurationKey, resp.Packet.ServerDurationFrame.ServerDuration)
	}

	req.netTraceSpan.End()
	req.netTraceSpan = nil
}

type opTelemetryHandler struct {
	tracer            *opTracer
	service           string
	operation         string
	start             time.Time
	metricsCompleteFn func(string, string, time.Time)
}

func (tc *tracerComponent) StartTelemeteryHandler(service, operation string, traceContext RequestSpanContext) *opTelemetryHandler {
	return &opTelemetryHandler{
		tracer:            tc.CreateOpTrace(operation, traceContext),
		service:           service,
		operation:         operation,
		start:             time.Now(),
		metricsCompleteFn: tc.ResponseValueRecord,
	}
}

func (oth *opTelemetryHandler) RootContext() RequestSpanContext {
	return oth.tracer.RootContext()
}

func (oth *opTelemetryHandler) StartTime() time.Time {
	return oth.start
}

func (oth *opTelemetryHandler) Finish() {
	oth.tracer.Finish()
	oth.metricsCompleteFn(oth.service, oth.operation, oth.start)
}
