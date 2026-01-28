package gocbcore

import (
	"strconv"
	"time"
)

type tracerWrapper struct {
	tracer                   RequestTracer
	includeLegacyConventions bool
	includeStableConventions bool
}

func newTracerWrapper(tracer RequestTracer, conventionOptIn []ObservabilitySemanticConvention) *tracerWrapper {
	reqTracer := tracer
	if reqTracer == nil {
		reqTracer = noopTracer{}
	}

	tw := &tracerWrapper{
		tracer: reqTracer,
	}

	if len(conventionOptIn) == 0 {
		// We only emit the legacy conventions by default
		tw.includeLegacyConventions = true
	} else {
		for _, convention := range conventionOptIn {
			switch convention {
			case ObservabilitySemanticConventionDatabase:
				tw.includeStableConventions = true
			case ObservabilitySemanticConventionDatabaseDup:
				tw.includeLegacyConventions = true
				tw.includeStableConventions = true
			}
		}
	}
	return tw
}

func (tw *tracerWrapper) StartSpan(context RequestSpanContext, name string) *spanWrapper {
	return &spanWrapper{
		span:                     tw.tracer.RequestSpan(context, name),
		includeLegacyConventions: tw.includeLegacyConventions,
		includeStableConventions: tw.includeStableConventions,
	}
}

type spanWrapper struct {
	span                     RequestSpan
	includeLegacyConventions bool
	includeStableConventions bool
}

func (sw *spanWrapper) End() {
	sw.span.End()
}

func (sw *spanWrapper) Context() RequestSpanContext {
	return sw.span.Context()
}

func (sw *spanWrapper) SetSystemName() {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("db.system", "couchbase")
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("db.system.name", "couchbase")
	}
}

func (sw *spanWrapper) SetClusterName(name string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("db.couchbase.cluster_name", name)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("couchbase.cluster.name", name)
	}
}

func (sw *spanWrapper) SetClusterUUID(uuid string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("db.couchbase.cluster_uuid", uuid)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("couchbase.cluster.uuid", uuid)
	}
}

func (sw *spanWrapper) SetNetworkTransportTCP() {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("net.transport", "IP.TCP")
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("network.transport", "tcp")
	}
}

func (sw *spanWrapper) SetOperationID(id string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("db.couchbase.operation_id", id)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("couchbase.operation_id", id)
	}
}

func (sw *spanWrapper) SetLocalID(id string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("db.couchbase.local_id", id)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("couchbase.local_id", id)
	}
}

func (sw *spanWrapper) SetHostAddress(endpoint, port string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("net.host.name", endpoint)
		sw.span.SetAttribute("net.host.port", port)
	}
	// Not in stable conventions
}

func (sw *spanWrapper) SetPeerAddress(endpoint, port string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("net.peer.name", endpoint)
		sw.span.SetAttribute("net.peer.port", port)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("network.peer.address", endpoint)
		// The stable conventions expect the port to be provided as an integer
		if portInt, err := strconv.Atoi(port); err == nil {
			sw.span.SetAttribute("network.peer.port", portInt)
		}
	}
}

func (sw *spanWrapper) SetServerAddress(endpoint, port string) {
	// Not in legacy conventions
	if sw.includeStableConventions {
		sw.span.SetAttribute("server.address", endpoint)
		// The stable conventions expect the port to be provided as an integer
		if portInt, err := strconv.Atoi(port); err == nil {
			sw.span.SetAttribute("server.port", portInt)
		}
	}
}

func (sw *spanWrapper) SetServerDuration(dur time.Duration) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("db.couchbase.server_duration", dur)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("couchbase.server_duration", dur)
	}
}

func (sw *spanWrapper) SetNumRetries(retries uint32) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute("db.couchbase.retries", retries)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute("couchbase.retries", retries)
	}
}
