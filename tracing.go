package gocbcore

import (
	"net"
	"net/http"
	"strconv"
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
	opSpan        *spanWrapper
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
	tracer           *tracerWrapper
	bucket           string
	noRootTraceSpans bool
	meter            *meterWrapper
	cfgMgr           configManager
	clusterLabels    atomic.Value
}

func newTracerComponent(tracer RequestTracer, conventions []ObservabilitySemanticConvention, bucket string, noRootTraceSpans bool, meter Meter, cfgMgr configManager) *tracerComponent {
	tc := &tracerComponent{
		tracer:           newTracerWrapper(tracer, conventions),
		bucket:           bucket,
		noRootTraceSpans: noRootTraceSpans,
		meter:            newMeterWrapper(meter, conventions),
		cfgMgr:           cfgMgr,
	}
	tc.meter.getClusterLabelsFn = func() ClusterLabels {
		return tc.ClusterLabels()
	}

	if cfgMgr != nil && (tracer != nil || meter != nil) {
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

	opSpan := tc.tracer.StartSpan(parentContext, operationName)
	opSpan.SetSystemName()
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		opSpan.SetClusterName(labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		opSpan.SetClusterUUID(labels.ClusterUUID)
	}

	return &opTracer{
		parentContext: parentContext,
		opSpan:        opSpan,
	}
}

func (tc *tracerComponent) StartHTTPDispatchSpan(req *httpRequest, name string) *spanWrapper {
	span := tc.tracer.StartSpan(req.RootTraceContext, name)
	return span
}

func (tc *tracerComponent) StopHTTPDispatchSpan(span *spanWrapper, req *http.Request, id string, retries uint32) {
	span.SetSystemName()
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		span.SetClusterName(labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		span.SetClusterUUID(labels.ClusterUUID)
	}
	span.SetNetworkTransportTCP()
	if id != "" {
		span.SetOperationID(id)
	}
	remoteName, remotePort, err := net.SplitHostPort(req.Host)
	if err != nil {
		logDebugf("Failed to split host port: %s", err)
	}

	span.SetPeerAddress(remoteName, remotePort)
	span.SetNumRetries(retries)
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

	req.cmdTraceSpan = tc.tracer.StartSpan(req.RootTraceContext, req.Packet.Command.Name())
	req.cmdTraceSpan.SetSystemName()
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		req.cmdTraceSpan.SetClusterName(labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		req.cmdTraceSpan.SetClusterUUID(labels.ClusterUUID)
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

	req.netTraceSpan = tc.tracer.StartSpan(req.cmdTraceSpan.Context(), spanNameDispatchToServer)
	req.netTraceSpan.SetSystemName()
	labels := tc.ClusterLabels()
	if labels.ClusterName != "" {
		req.netTraceSpan.SetClusterName(labels.ClusterName)
	}
	if labels.ClusterUUID != "" {
		req.netTraceSpan.SetClusterUUID(labels.ClusterUUID)
	}
	req.processingLock.Unlock()
}

func (tc *tracerComponent) ResponseValueRecord(service, operation string, start time.Time) {
	duration := uint64(time.Since(start).Microseconds())
	if duration == 0 {
		duration = uint64(1 * time.Microsecond)
	}

	tc.meter.RecordOperation(service, operation, duration)
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

	req.cmdTraceSpan.SetNumRetries(req.RetryAttempts())

	req.cmdTraceSpan.End()
	req.cmdTraceSpan = nil
}

func cancelReqTraceLocked(req *memdQRequest, local, remote, canonicalRemote string) {
	if req.cmdTraceSpan != nil {
		if req.netTraceSpan != nil {
			stopNetTraceLocked(req, nil, local, remote, canonicalRemote)
		}

		stopCmdTraceLocked(req)
	}
}

func stopNetTraceLocked(req *memdQRequest, resp *memdQResponse, localAddress, remoteAddress, canonicalRemoteAddress string) {
	if req.cmdTraceSpan == nil {
		return
	}

	if req.netTraceSpan == nil {
		logWarnf("Attempted to stop net tracing on an untraced request")
		return
	}

	req.netTraceSpan.SetNetworkTransportTCP()
	if resp != nil {
		req.netTraceSpan.SetOperationID(strconv.Itoa(int(resp.Opaque)))
		req.netTraceSpan.SetLocalID(resp.sourceConnID)
	}
	localName, localPort, err := net.SplitHostPort(localAddress)
	if err != nil {
		logDebugf("Failed to split host port: %s", err)
	}

	remoteName, remotePort, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		logDebugf("Failed to split host port: %s", err)
	}

	canonicalRemoteName, canonicalRemotePort, err := net.SplitHostPort(canonicalRemoteAddress)
	if err != nil {
		logDebugf("Failed to split host port: %s", err)
	}

	req.netTraceSpan.SetHostAddress(localName, localPort)
	req.netTraceSpan.SetPeerAddress(remoteName, remotePort)
	req.netTraceSpan.SetServerAddress(canonicalRemoteName, canonicalRemotePort)
	if resp != nil && resp.Packet.ServerDurationFrame != nil {
		req.netTraceSpan.SetServerDuration(resp.Packet.ServerDurationFrame.ServerDuration)
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
