package gocbcore

import (
	"reflect"
	"strconv"
	"strings"
	"time"
)

type telemetryEntry struct {
	name  string
	value uint64
	tags  map[string]string
}

func splitString(s, sep string) (a, b string, found bool) {
	idx := strings.Index(s, sep)
	if idx < 0 {
		return "", "", false
	}

	return s[:idx], s[idx+1:], true
}

func (suite *UnitTestSuite) parseTelemetryEntry(entry string) telemetryEntry {
	name, rest, found := splitString(entry, "{")
	suite.Assert().True(found)

	rawTags, rest, found := splitString(rest, "}")
	suite.Assert().True(found)

	value, err := strconv.ParseUint(strings.TrimSpace(rest), 10, 64)
	suite.Assert().NoError(err)

	res := telemetryEntry{
		name:  name,
		value: value,
		tags:  make(map[string]string),
	}

	for _, rawTag := range strings.Split(rawTags, ",") {
		pair := strings.Split(rawTag, "=")
		suite.Assert().Len(pair, 2)
		res.tags[strings.TrimSpace(pair[0])] = strings.Trim(strings.TrimSpace(pair[1]), "\"")
	}

	return res
}

func (suite *UnitTestSuite) AssertTelemetryEntries(expected []telemetryEntry, actual []string) {
	suite.Require().Len(actual, len(expected))

	for _, rawEntry := range actual {
		entry := suite.parseTelemetryEntry(rawEntry)

		found := false
		for _, exp := range expected {
			if entry.name == exp.name && reflect.DeepEqual(entry.tags, exp.tags) {
				suite.Assert().Equal(exp.value, entry.value,
					"Unexpected value for entry: %s", rawEntry)
				found = true
				break
			}
		}

		suite.Require().True(found, "Unexpected telemetry entry: %s", rawEntry)
	}
}

func (suite *UnitTestSuite) TestTelemetryCounterSerialization() {
	counters := newTelemetryCounters()

	agent := "sdk/1.2.3"
	counters.recordOp(telemetryOperationAttributes{
		outcome:  telemetryOutcomeCanceled,
		node:     "node1",
		nodeUUID: "node1-uuid",
		service:  N1qlService,
		agent:    agent,
	})
	counters.recordOp(telemetryOperationAttributes{
		outcome:  telemetryOutcomeCanceled,
		node:     "node1",
		nodeUUID: "node1-uuid",
		service:  N1qlService,
		agent:    agent,
	})
	counters.recordOp(telemetryOperationAttributes{
		outcome:  telemetryOutcomeCanceled,
		node:     "node2",
		nodeUUID: "node2-uuid",
		service:  N1qlService,
		agent:    agent,
	})
	counters.recordOp(telemetryOperationAttributes{
		outcome:  telemetryOutcomeCanceled,
		node:     "node1",
		nodeUUID: "node1-uuid",
		service:  MemdService,
		bucket:   "default",
		agent:    agent,
	})
	counters.recordOp(telemetryOperationAttributes{
		outcome:  telemetryOutcomeTimedout,
		node:     "node1",
		nodeUUID: "node1-uuid",
		service:  MemdService,
		bucket:   "default",
		agent:    agent,
	})
	counters.recordOp(telemetryOperationAttributes{
		outcome:  telemetryOutcomeSuccess,
		node:     "node2",
		nodeUUID: "node2-uuid",
		service:  MemdService,
		bucket:   "default",
		agent:    agent,
	})

	serialized := counters.serialize()

	expected := []telemetryEntry{
		{
			name: "sdk_query_r_canceled",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
			},
			value: 2,
		},
		{
			name: "sdk_query_r_canceled",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node2",
				"node_uuid": "node2-uuid",
			},
			value: 1,
		},
		{
			name: "sdk_kv_r_canceled",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
				"bucket":    "default",
			},
			value: 1,
		},
		{
			name: "sdk_kv_r_timedout",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
				"bucket":    "default",
			},
			value: 1,
		},
		{
			name: "sdk_query_r_total",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
			},
			value: 2,
		},
		{
			name: "sdk_query_r_total",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node2",
				"node_uuid": "node2-uuid",
			},
			value: 1,
		},
		{
			name: "sdk_kv_r_total",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
				"bucket":    "default",
			},
			value: 2,
		},
		{
			name: "sdk_kv_r_total",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node2",
				"node_uuid": "node2-uuid",
				"bucket":    "default",
			},
			value: 1,
		},
	}

	suite.AssertTelemetryEntries(expected, serialized)
}

func (suite *UnitTestSuite) TestTelemetryHistogramSerialization() {
	histograms := newTelemetryHistograms()

	agent := "sdk/1.2.3"
	histograms.recordOp(telemetryOperationAttributes{
		duration: 10 * time.Millisecond,
		service:  N1qlService,
		node:     "node1",
		nodeUUID: "node1-uuid",
		agent:    agent,
	})
	histograms.recordOp(telemetryOperationAttributes{
		duration: 5 * time.Second,
		service:  N1qlService,
		node:     "node1",
		nodeUUID: "node1-uuid",
		agent:    agent,
	})
	histograms.recordOp(telemetryOperationAttributes{
		duration: 2 * time.Second,
		service:  MemdService,
		mutation: false,
		bucket:   "default",
		node:     "node1",
		nodeUUID: "node1-uuid",
		agent:    agent,
	})
	histograms.recordOp(telemetryOperationAttributes{
		duration: 5 * time.Millisecond,
		service:  MemdService,
		mutation: false,
		bucket:   "default",
		node:     "node1",
		nodeUUID: "node1-uuid",
		agent:    agent,
	})
	histograms.recordOp(telemetryOperationAttributes{
		duration: 700 * time.Millisecond,
		service:  MemdService,
		mutation: false,
		bucket:   "default",
		node:     "node1",
		nodeUUID: "node1-uuid",
		agent:    agent,
	})

	serialized := histograms.serialize()

	expectedKvRetrievalBins := map[string]uint64{
		"1":    0,
		"10":   1,
		"100":  1,
		"500":  1,
		"1000": 2,
		"2500": 3,
		"+Inf": 3,
	}

	expectedQueryBins := map[string]uint64{
		"100":   1,
		"1000":  1,
		"10000": 2,
		"30000": 2,
		"75000": 2,
		"+Inf":  2,
	}

	var expected []telemetryEntry

	for bin, count := range expectedKvRetrievalBins {
		expected = append(expected, telemetryEntry{
			name: "sdk_kv_retrieval_duration_milliseconds_bucket",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
				"bucket":    "default",
				"le":        bin,
			},
			value: count,
		})
	}
	expected = append(expected,
		telemetryEntry{
			name: "sdk_kv_retrieval_duration_milliseconds_count",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
				"bucket":    "default",
			},
			value: expectedKvRetrievalBins["+Inf"],
		},
		telemetryEntry{
			name: "sdk_kv_retrieval_duration_milliseconds_sum",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
				"bucket":    "default",
			},
			value: 2705,
		},
	)

	for bin, count := range expectedQueryBins {
		expected = append(expected, telemetryEntry{
			name: "sdk_query_duration_milliseconds_bucket",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
				"le":        bin,
			},
			value: count,
		})
	}
	expected = append(expected,
		telemetryEntry{
			name: "sdk_query_duration_milliseconds_count",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
			},
			value: expectedQueryBins["+Inf"],
		},
		telemetryEntry{
			name: "sdk_query_duration_milliseconds_sum",
			tags: map[string]string{
				"agent":     "sdk/1.2.3",
				"node":      "node1",
				"node_uuid": "node1-uuid",
			},
			value: 5010,
		},
	)

	suite.AssertTelemetryEntries(expected, serialized)
}
