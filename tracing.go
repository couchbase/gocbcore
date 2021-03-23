package gocbcore

import (
	"net"
	"net/http"
	"strconv"
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

// RequestSpanContext is the interface for for external span contexts that can be passed in into the SDK option blocks.
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

type tracerManager interface {
	CreateOpTrace(operationName string, parentContext RequestSpanContext) *opTracer
	StartHTTPDispatchSpan(req *httpRequest, name string) RequestSpan
	StopHTTPDispatchSpan(span RequestSpan, req *http.Request, id string)
	StartCmdTrace(req *memdQRequest)
	StartNetTrace(req *memdQRequest)
}

type tracerComponent struct {
	tracer           RequestTracer
	bucket           string
	noRootTraceSpans bool
}

func newTracerComponent(tracer RequestTracer, bucket string, noRootTraceSpans bool) *tracerComponent {
	return &tracerComponent{
		tracer:           tracer,
		bucket:           bucket,
		noRootTraceSpans: noRootTraceSpans,
	}
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

	return &opTracer{
		parentContext: parentContext,
		opSpan:        opSpan,
	}
}

func (tc *tracerComponent) StartHTTPDispatchSpan(req *httpRequest, name string) RequestSpan {
	span := tc.tracer.RequestSpan(req.RootTraceContext, name)
	return span
}

func (tc *tracerComponent) StopHTTPDispatchSpan(span RequestSpan, req *http.Request, id string) {
	span.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
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
	span.End()
}

func (tc *tracerComponent) StartCmdTrace(req *memdQRequest) {
	if req.cmdTraceSpan != nil {
		logWarnf("Attempted to start tracing on traced request")
		return
	}

	if req.RootTraceContext == nil {
		return
	}

	req.processingLock.Lock()
	req.cmdTraceSpan = tc.tracer.RequestSpan(req.RootTraceContext, req.Packet.Command.Name())

	req.processingLock.Unlock()
}

func (tc *tracerComponent) StartNetTrace(req *memdQRequest) {
	if req.cmdTraceSpan == nil {
		return
	}

	if req.netTraceSpan != nil {
		logWarnf("Attempted to start net tracing on traced request")
		return
	}

	req.processingLock.Lock()
	req.netTraceSpan = tc.tracer.RequestSpan(req.cmdTraceSpan.Context(), spanNameDispatchToServer)
	req.processingLock.Unlock()
}

func stopCmdTrace(req *memdQRequest) {
	if req.RootTraceContext == nil {
		return
	}

	if req.cmdTraceSpan == nil {
		logWarnf("Attempted to stop tracing on untraced request")
		return
	}

	req.cmdTraceSpan.SetAttribute(spanAttribDBSystemKey, "couchbase")

	req.cmdTraceSpan.End()
	req.cmdTraceSpan = nil
}

func cancelReqTrace(req *memdQRequest) {
	if req.cmdTraceSpan != nil {
		if req.netTraceSpan != nil {
			req.netTraceSpan.End()
		}

		req.cmdTraceSpan.End()
	}
}

func stopNetTrace(req *memdQRequest, resp *memdQResponse, localAddress, remoteAddress string) {
	if req.cmdTraceSpan == nil {
		return
	}

	if req.netTraceSpan == nil {
		logWarnf("Attempted to stop net tracing on an untraced request")
		return
	}

	req.netTraceSpan.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	req.netTraceSpan.SetAttribute(spanAttribNetTransportKey, spanAttribNetTransportValue)
	req.netTraceSpan.SetAttribute(spanAttribOperationIDKey, strconv.Itoa(int(resp.Opaque)))
	req.netTraceSpan.SetAttribute(spanAttribLocalIDKey, resp.sourceConnID)
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
	if resp.Packet.ServerDurationFrame != nil {
		req.netTraceSpan.SetAttribute(spanAttribServerDurationKey, resp.Packet.ServerDurationFrame.ServerDuration)
	}

	req.netTraceSpan.End()
	req.netTraceSpan = nil
}
