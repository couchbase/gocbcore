package gocbcore

import (
	"sync"
)

type meterWrapper struct {
	meter                    Meter
	includeLegacyConventions bool
	includeStableConventions bool
	getClusterLabelsFn       func() ClusterLabels
	attributesCache          sync.Map
}

func newMeterWrapper(
	meter Meter,
	conventionOptIn []ObservabilitySemanticConvention,
) *meterWrapper {

	mw := &meterWrapper{meter: meter}

	if len(conventionOptIn) == 0 {
		// We only emit the legacy conventions by default
		mw.includeLegacyConventions = true
	} else {
		for _, convention := range conventionOptIn {
			switch convention {
			case ObservabilitySemanticConventionDatabase:
				mw.includeStableConventions = true
			case ObservabilitySemanticConventionDatabaseDup:
				mw.includeLegacyConventions = true
				mw.includeStableConventions = true
			}
		}
	}
	return mw
}

func (mw *meterWrapper) recordValue(name string, attributes map[string]string, value uint64) {
	if mw.meter == nil {
		return
	}
	recorder, err := mw.meter.ValueRecorder(name, attributes)
	if err != nil {
		logDebugf("Failed to get value recorder: %v", err)
		return
	}
	recorder.RecordValue(value)
}

func (mw *meterWrapper) RecordOperation(service, operation string, durationMicroseconds uint64) {
	if mw.meter == nil {
		return
	}

	if mw.includeLegacyConventions {
		key := "v0." + service + "." + operation
		attribs, ok := mw.attributesCache.Load(key)
		if !ok {
			attribs = map[string]string{
				"db.couchbase.service": service,
			}
			if operation != "" {
				attribs.(map[string]string)["db.operation"] = operation
			}
			if mw.getClusterLabelsFn != nil {
				clusterLabels := mw.getClusterLabelsFn()
				if clusterLabels.ClusterUUID != "" {
					attribs.(map[string]string)["db.couchbase.cluster_uuid"] = clusterLabels.ClusterUUID
				}
				if clusterLabels.ClusterName != "" {
					attribs.(map[string]string)["db.couchbase.cluster_name"] = clusterLabels.ClusterName
				}
			}
			mw.attributesCache.Store(key, attribs)
		}
		mw.recordValue("db.couchbase.operations", attribs.(map[string]string), durationMicroseconds)
	}

	if mw.includeStableConventions {
		key := "v1." + service + "." + operation
		attribs, ok := mw.attributesCache.Load(key)
		if !ok {
			attribs = map[string]string{
				"db.system.name":    "couchbase",
				"couchbase.service": service,
				"__unit":            "s",
			}
			if operation != "" {
				attribs.(map[string]string)["db.operation.name"] = operation
			}

			if mw.getClusterLabelsFn != nil {
				clusterLabels := mw.getClusterLabelsFn()
				if clusterLabels.ClusterUUID != "" {
					attribs.(map[string]string)["couchbase.cluster.uuid"] = clusterLabels.ClusterUUID
				}
				if clusterLabels.ClusterName != "" {
					attribs.(map[string]string)["couchbase.cluster.name"] = clusterLabels.ClusterName
				}
			}
			mw.attributesCache.Store(key, attribs)
		}
		mw.recordValue("db.client.operation.duration", attribs.(map[string]string), durationMicroseconds)
	}
}
