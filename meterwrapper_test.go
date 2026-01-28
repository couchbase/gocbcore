package gocbcore

import (
	"errors"
	"testing"
)

type semanticConventionTestMeterEntry struct {
	attributes map[string]string
	value      uint64
}

type semanticConventionTestMeter struct {
	entries map[string][]semanticConventionTestMeterEntry
}

func newSemanticConventionTestMeter() *semanticConventionTestMeter {
	return &semanticConventionTestMeter{
		entries: make(map[string][]semanticConventionTestMeterEntry),
	}
}

func (m *semanticConventionTestMeter) Counter(name string, tags map[string]string) (Counter, error) {
	return nil, errors.New("not implemented")
}

func (m *semanticConventionTestMeter) ValueRecorder(name string, tags map[string]string) (ValueRecorder, error) {
	return &semanticConventionTestValueRecorder{
		meterName:  name,
		attributes: tags,
		meter:      m,
	}, nil
}

type semanticConventionTestValueRecorder struct {
	meterName  string
	attributes map[string]string
	meter      *semanticConventionTestMeter
}

func (r *semanticConventionTestValueRecorder) RecordValue(val uint64) {
	r.meter.entries[r.meterName] = append(r.meter.entries[r.meterName], semanticConventionTestMeterEntry{
		attributes: r.attributes,
		value:      val,
	})
}

func (suite *UnitTestSuite) TestMeterWrapperSemanticConventions() {
	suite.T().Run("OnlyLegacyEmittedByDefault", func(t *testing.T) {
		meter := newSemanticConventionTestMeter()

		mw := newMeterWrapper(meter, nil)
		mw.getClusterLabelsFn = func() ClusterLabels {
			return ClusterLabels{
				ClusterName: "test-cluster",
				ClusterUUID: "test-uuid",
			}
		}
		suite.Require().True(mw.includeLegacyConventions)
		suite.Require().False(mw.includeStableConventions)

		mw.RecordOperation("kv", "get", 1000)

		suite.Require().Len(meter.entries, 1)

		entry := meter.entries["db.couchbase.operations"][0]
		suite.Assert().Len(entry.attributes, 4)
		suite.Assert().Equal("kv", entry.attributes["db.couchbase.service"])
		suite.Assert().Equal("get", entry.attributes["db.operation"])
		suite.Assert().Equal("test-cluster", entry.attributes["db.couchbase.cluster_name"])
		suite.Assert().Equal("test-uuid", entry.attributes["db.couchbase.cluster_uuid"])
		suite.Assert().Equal(uint64(1000), entry.value)
	})

	suite.T().Run("DatabaseOptInEmitsStableOnly", func(t *testing.T) {
		meter := newSemanticConventionTestMeter()

		mw := newMeterWrapper(meter, []ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabase,
		})
		mw.getClusterLabelsFn = func() ClusterLabels {
			return ClusterLabels{
				ClusterName: "test-cluster",
				ClusterUUID: "test-uuid",
			}
		}
		suite.Require().False(mw.includeLegacyConventions)
		suite.Require().True(mw.includeStableConventions)

		mw.RecordOperation("kv", "get", 1000)

		suite.Require().Len(meter.entries, 1)

		entry := meter.entries["db.client.operation.duration"][0]
		suite.Assert().Len(entry.attributes, 6)
		suite.Assert().Equal("couchbase", entry.attributes["db.system.name"])
		suite.Assert().Equal("s", entry.attributes["__unit"])
		suite.Assert().Equal("kv", entry.attributes["couchbase.service"])
		suite.Assert().Equal("get", entry.attributes["db.operation.name"])
		suite.Assert().Equal("test-cluster", entry.attributes["couchbase.cluster.name"])
		suite.Assert().Equal("test-uuid", entry.attributes["couchbase.cluster.uuid"])
		suite.Assert().Equal(uint64(1000), entry.value)
	})

	suite.T().Run("DatabaseDupOptInEmitsBoth", func(t *testing.T) {
		meter := newSemanticConventionTestMeter()

		mw := newMeterWrapper(meter, []ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabaseDup,
		})
		mw.getClusterLabelsFn = func() ClusterLabels {
			return ClusterLabels{
				ClusterName: "test-cluster",
				ClusterUUID: "test-uuid",
			}
		}
		suite.Require().True(mw.includeLegacyConventions)
		suite.Require().True(mw.includeStableConventions)

		mw.RecordOperation("kv", "get", 1000)

		suite.Require().Len(meter.entries, 2)

		stableEntry := meter.entries["db.client.operation.duration"][0]
		suite.Assert().Len(stableEntry.attributes, 6)
		suite.Assert().Equal("couchbase", stableEntry.attributes["db.system.name"])
		suite.Assert().Equal("s", stableEntry.attributes["__unit"])
		suite.Assert().Equal("kv", stableEntry.attributes["couchbase.service"])
		suite.Assert().Equal("get", stableEntry.attributes["db.operation.name"])
		suite.Assert().Equal("test-cluster", stableEntry.attributes["couchbase.cluster.name"])
		suite.Assert().Equal("test-uuid", stableEntry.attributes["couchbase.cluster.uuid"])
		suite.Assert().Equal(uint64(1000), stableEntry.value)

		legacyEntry := meter.entries["db.couchbase.operations"][0]
		suite.Assert().Len(legacyEntry.attributes, 4)
		suite.Assert().Equal("kv", legacyEntry.attributes["db.couchbase.service"])
		suite.Assert().Equal("get", legacyEntry.attributes["db.operation"])
		suite.Assert().Equal("test-cluster", legacyEntry.attributes["db.couchbase.cluster_name"])
		suite.Assert().Equal("test-uuid", legacyEntry.attributes["db.couchbase.cluster_uuid"])
		suite.Assert().Equal(uint64(1000), legacyEntry.value)
	})

	suite.T().Run("DatabaseDupTakesPrecedence", func(t *testing.T) {
		meter := newSemanticConventionTestMeter()

		mw := newMeterWrapper(meter, []ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabaseDup,
			ObservabilitySemanticConventionDatabase,
		})
		mw.getClusterLabelsFn = func() ClusterLabels {
			return ClusterLabels{
				ClusterName: "test-cluster",
				ClusterUUID: "test-uuid",
			}
		}
		suite.Require().True(mw.includeLegacyConventions)
		suite.Require().True(mw.includeStableConventions)

		mw.RecordOperation("kv", "get", 1000)

		suite.Require().Len(meter.entries, 2)

		stableEntry := meter.entries["db.client.operation.duration"][0]
		suite.Assert().Len(stableEntry.attributes, 6)
		suite.Assert().Equal("couchbase", stableEntry.attributes["db.system.name"])
		suite.Assert().Equal("s", stableEntry.attributes["__unit"])
		suite.Assert().Equal("kv", stableEntry.attributes["couchbase.service"])
		suite.Assert().Equal("get", stableEntry.attributes["db.operation.name"])
		suite.Assert().Equal("test-cluster", stableEntry.attributes["couchbase.cluster.name"])
		suite.Assert().Equal("test-uuid", stableEntry.attributes["couchbase.cluster.uuid"])
		suite.Assert().Equal(uint64(1000), stableEntry.value)

		legacyEntry := meter.entries["db.couchbase.operations"][0]
		suite.Assert().Len(legacyEntry.attributes, 4)
		suite.Assert().Equal("kv", legacyEntry.attributes["db.couchbase.service"])
		suite.Assert().Equal("get", legacyEntry.attributes["db.operation"])
		suite.Assert().Equal("test-cluster", legacyEntry.attributes["db.couchbase.cluster_name"])
		suite.Assert().Equal("test-uuid", legacyEntry.attributes["db.couchbase.cluster_uuid"])
		suite.Assert().Equal(uint64(1000), legacyEntry.value)
	})
}
