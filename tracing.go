package gocbcore

import (
	"encoding/json"
	"fmt"
	"time"
)

// RequestTracer describes the tracing abstraction in the SDK.
type RequestTracer interface {
	StartSpan(operationName string, parentContext RequestSpanContext) RequestSpan
}

// RequestSpan is the interface for spans that are created by a RequestTracer.
type RequestSpan interface {
	Finish()
	Context() RequestSpanContext
	SetTag(key string, value interface{}) RequestSpan
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

func (tracer noopTracer) StartSpan(operationName string, parentContext RequestSpanContext) RequestSpan {
	return defaultNoopSpan
}

func (span noopSpan) Finish() {
}

func (span noopSpan) Context() RequestSpanContext {
	return defaultNoopSpanContext
}

func (span noopSpan) SetTag(key string, value interface{}) RequestSpan {
	return defaultNoopSpan
}

type opTracer struct {
	parentContext RequestSpanContext
	opSpan        RequestSpan
}

func (tracer *opTracer) Finish() {
	if tracer.opSpan != nil {
		tracer.opSpan.Finish()
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
	StartHTTPSpan(req *httpRequest, name string) RequestSpan
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

	opSpan := tc.tracer.StartSpan(operationName, parentContext).
		SetTag("component", "couchbase-go-sdk").
		SetTag("db.instance", tc.bucket).
		SetTag("span.kind", "client")

	return &opTracer{
		parentContext: parentContext,
		opSpan:        opSpan,
	}
}

func (tc *tracerComponent) StartHTTPSpan(req *httpRequest, name string) RequestSpan {
	return tc.tracer.StartSpan(name, req.RootTraceContext).
		SetTag("retry", req.RetryAttempts())
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
	req.cmdTraceSpan = tc.tracer.StartSpan(req.Packet.Command.Name(), req.RootTraceContext).
		SetTag("retry", req.RetryAttempts())

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
	req.netTraceSpan = tc.tracer.StartSpan("rpc", req.cmdTraceSpan.Context()).
		SetTag("span.kind", "client")
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

	req.cmdTraceSpan.Finish()
	req.cmdTraceSpan = nil
}

func cancelReqTrace(req *memdQRequest) {
	if req.cmdTraceSpan != nil {
		if req.netTraceSpan != nil {
			req.netTraceSpan.Finish()
		}

		req.cmdTraceSpan.Finish()
	}
}

type zombieLogEntry struct {
	connectionID string
	operationID  string
	endpoint     string
	duration     time.Duration
	serviceType  string
}

type zombieLogItem struct {
	ConnectionID     string `json:"c"`
	OperationID      string `json:"i"`
	Endpoint         string `json:"r"`
	ServerDurationUs uint64 `json:"d"`
	ServiceType      string `json:"s"`
}

type zombieLogService struct {
	Service string          `json:"service"`
	Count   int             `json:"count"`
	Top     []zombieLogItem `json:"top"`
}

func (agent *Agent) zombieLogger(interval time.Duration, sampleSize int) {
	lastTick := time.Now()

	for {
		// We tick every 1 second to make sure that the goroutines
		// are cleaned up promptly after agent shutdown.
		<-time.After(1 * time.Second)

		routingInfo := agent.routingInfo.Get()
		if routingInfo == nil {
			// If the routing info is gone, the agent shut down and we should close
			return
		}

		if time.Now().Sub(lastTick) < interval {
			continue
		}

		lastTick = lastTick.Add(interval)

		// Preallocate space to copy the ops into...
		oldOps := make([]*zombieLogEntry, sampleSize)

		agent.zombieLock.Lock()
		// Escape early if we have no ops to log...
		if len(agent.zombieOps) == 0 {
			agent.zombieLock.Unlock()
			return
		}

		// Copy out our ops so we can cheaply print them out without blocking
		// our ops from actually being recorded in other goroutines (which would
		// effectively slow down the op pipeline for logging).

		oldOps = oldOps[0:len(agent.zombieOps)]
		copy(oldOps, agent.zombieOps)
		agent.zombieOps = agent.zombieOps[:0]

		agent.zombieLock.Unlock()

		jsonData := zombieLogService{
			Service: "kv",
		}

		for i := len(oldOps) - 1; i >= 0; i-- {
			op := oldOps[i]

			jsonData.Top = append(jsonData.Top, zombieLogItem{
				OperationID:      op.operationID,
				ConnectionID:     op.connectionID,
				Endpoint:         op.endpoint,
				ServerDurationUs: uint64(op.duration / time.Microsecond),
				ServiceType:      op.serviceType,
			})
		}

		jsonData.Count = len(jsonData.Top)

		jsonBytes, err := json.Marshal(jsonData)
		if err != nil {
			logDebugf("Failed to generate zombie logging JSON: %s", err)
		}

		logWarnf("Orphaned responses observed:\n %s", jsonBytes)
	}
}

func (agent *Agent) recordZombieResponse(resp *memdQResponse, client *memdClient) {
	entry := &zombieLogEntry{
		connectionID: client.connId,
		operationID:  fmt.Sprintf("0x%x", resp.Opaque),
		endpoint:     client.Address(),
		duration:     0,
		serviceType:  fmt.Sprintf("kv:%s", getCommandName(resp.Opcode)),
	}

	if resp.FrameExtras != nil && resp.FrameExtras.HasSrvDuration {
		entry.duration = resp.FrameExtras.SrvDuration
	}

	agent.zombieLock.RLock()

	if cap(agent.zombieOps) == 0 || (len(agent.zombieOps) == cap(agent.zombieOps) &&
		entry.duration < agent.zombieOps[0].duration) {
		// we are at capacity and we are faster than the fastest slow op or somehow in a state where capacity is 0.
		agent.zombieLock.RUnlock()
		return
	}

	agent.zombieLock.Lock()
	if cap(agent.zombieOps) == 0 || (len(agent.zombieOps) == cap(agent.zombieOps) &&
		entry.duration < agent.zombieOps[0].duration) {
		// we are at capacity and we are faster than the fastest slow op or somehow in a state where capacity is 0.
		agent.zombieLock.Unlock()
		return
	}

	req.netTraceSpan.SetTag("couchbase.operation_id", fmt.Sprintf("0x%x", resp.Opaque))
	req.netTraceSpan.SetTag("couchbase.local_id", resp.sourceConnID)
	if isLogRedactionLevelNone() {
		req.netTraceSpan.SetTag("couchbase.document_key", string(req.Key))
	}
	req.netTraceSpan.SetTag("local.address", localAddress)
	req.netTraceSpan.SetTag("peer.address", remoteAddress)
	if resp.Packet.ServerDurationFrame != nil {
		req.netTraceSpan.SetTag("server_duration", resp.Packet.ServerDurationFrame.ServerDuration)
	}

	req.netTraceSpan.Finish()
	req.netTraceSpan = nil
}
