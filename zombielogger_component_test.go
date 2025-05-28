package gocbcore

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"math"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *StandardTestSuite) TestZombieLoggerComponentCancelledOps() {
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)
	if suite.IsMockServer() {
		suite.T().Skip("Responses are likely to be received before we cancel the request when using mock server")
	}

	cfg := makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	cfg.OrphanReporterConfig = OrphanReporterConfig{
		Enabled:        true,
		ReportInterval: 1 * time.Hour, // Something long enough to not trigger during the test
		SampleSize:     50,
	}
	agent, err := CreateAgent(&cfg)
	suite.Require().NoError(err)
	defer agent.Close()

	for i := 0; i < 20; i++ {
		errCh := make(chan error, 1)
		// Doing a durable Set to give us more time to cancel the request before receiving a response.
		op, err := agent.Set(SetOptions{
			Key:                    []byte(fmt.Sprintf("zombie-test-%s", uuid.NewString()[:6])),
			CollectionName:         suite.CollectionName,
			ScopeName:              suite.ScopeName,
			Value:                  []byte(`{"foo": "bar"}`),
			Flags:                  EncodeCommonFlags(JSONType, NoCompression),
			DurabilityLevel:        memd.DurabilityLevelPersistToMajority,
			Deadline:               time.Now().Add(30 * time.Second),
			DurabilityLevelTimeout: 30 * time.Second,
		}, func(res *StoreResult, err error) {
			errCh <- err
			close(errCh)
		})
		suite.Require().NoError(err)
		memdReq := op.(*memdQRequest)
		for {
			if atomic.LoadUint32(&memdReq.Opaque) > 0 {
				// Opaque has been set, the request has been or is about to be dispatched.
				// Cancelling it now guarantees an orphaned response.
				memdReq.Cancel()
				break
			}
		}
		err = <-errCh
		suite.Require().ErrorIs(err, ErrRequestCanceled)
	}

	expectedZombieCount := 20
	var zombieCount int
	suite.Require().Eventually(
		func() bool {
			agent.zombieLogger.zombieLock.RLock()
			zombieCount = len(agent.zombieLogger.zombieOps)
			agent.zombieLogger.zombieLock.RUnlock()
			return zombieCount == expectedZombieCount
		},
		20*time.Second,
		10*time.Millisecond,
		"Zombie logger did not receive all expected zombie responses (Expected: %d, Got: %d)",
		expectedZombieCount,
		zombieCount)

	rawOutput := agent.zombieLogger.createOutput()
	var output zombieLogService
	err = json.Unmarshal(rawOutput, &output)
	suite.Require().NoError(err)
	suite.Require().Len(output, 1)
	suite.Require().Contains(output, "kv")

	suite.Require().Equal(expectedZombieCount, output["kv"].Count)
	zombieOps := output["kv"].Top
	previousTotalDurationUs := uint64(math.MaxUint64)
	for _, op := range zombieOps {
		suite.Assert().LessOrEqual(op.TotalDurationUs, previousTotalDurationUs, "Operations are not sorted by total server duration")
		previousTotalDurationUs = op.TotalDurationUs
		suite.Assert().LessOrEqual(op.ServerDurationUs, op.TotalServerDurationUs, "Last server duration is greater than total server duration")
	}
}

func (suite *UnitTestSuite) TestZombieLoggerComponentEncoding() {
	responses := []*memdQResponse{
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  23,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 2100 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  24,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 2200 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  25,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 1100 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdGet,
				Opaque:  27,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 2800 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  29,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 5000 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
	}

	totalDurations := map[uint32]time.Duration{
		23: 2200 * time.Microsecond,
		24: 3000 * time.Microsecond,
		25: 2700 * time.Microsecond,
		27: 4000 * time.Microsecond,
		29: 7000 * time.Microsecond,
	}

	totalServerDurations := map[uint32]time.Duration{
		23: 0,
		24: 0,
		25: 0,
		27: 0,
		29: 1000 * time.Microsecond,
	}

	z := newZombieLoggerComponent(1*time.Second, 4)
	go z.Start()
	for _, r := range responses {
		z.recordZombieResponseInternal(totalDurations[r.Opaque], totalServerDurations[r.Opaque], r, "9a1e99041b33322b/54cf79f08d852738", "10.112.210.1", "10.112.210.101")
	}
	z.Stop()

	jsonOutput := z.createOutput()

	type expectedOutputFormat struct {
		ConnectionID          string `json:"last_local_id"`
		OperationID           string `json:"operation_id"`
		RemoteSocket          string `json:"last_remote_socket,omitempty"`
		LocalSocket           string `json:"last_local_socket,omitempty"`
		ServerDurationUs      uint64 `json:"last_server_duration_us,omitempty"`
		TotalServerDurationUs uint64 `json:"total_server_duration_us,omitempty"`
		TotalDurationUs       uint64 `json:"total_duration_us,omitempty"`
		OperationName         string `json:"operation_name"`
	}

	expectedOutput := []expectedOutputFormat{
		{
			ConnectionID:          "9a1e99041b33322b/54cf79f08d852738",
			OperationID:           "0x1d",
			RemoteSocket:          "10.112.210.101",
			LocalSocket:           "10.112.210.1",
			ServerDurationUs:      5000,
			TotalServerDurationUs: 6000,
			TotalDurationUs:       7000,
			OperationName:         memd.CmdReplace.Name(),
		},
		{
			ConnectionID:          "9a1e99041b33322b/54cf79f08d852738",
			OperationID:           "0x1b",
			RemoteSocket:          "10.112.210.101",
			LocalSocket:           "10.112.210.1",
			ServerDurationUs:      2800,
			TotalServerDurationUs: 2800,
			TotalDurationUs:       4000,
			OperationName:         memd.CmdGet.Name(),
		},
		{
			ConnectionID:          "9a1e99041b33322b/54cf79f08d852738",
			OperationID:           "0x18",
			RemoteSocket:          "10.112.210.101",
			LocalSocket:           "10.112.210.1",
			ServerDurationUs:      2200,
			TotalServerDurationUs: 2200,
			TotalDurationUs:       3000,
			OperationName:         memd.CmdReplace.Name(),
		},
		{
			ConnectionID:          "9a1e99041b33322b/54cf79f08d852738",
			OperationID:           "0x19",
			RemoteSocket:          "10.112.210.101",
			LocalSocket:           "10.112.210.1",
			ServerDurationUs:      1100,
			TotalServerDurationUs: 1100,
			TotalDurationUs:       2700,
			OperationName:         memd.CmdReplace.Name(),
		},
	}

	expectedJsonOutput, err := json.Marshal(expectedOutput)
	suite.Require().Nil(err)

	var mapTopOutput map[string]json.RawMessage
	suite.Require().Nil(json.Unmarshal(jsonOutput, &mapTopOutput))

	suite.Require().Contains(mapTopOutput, "kv")

	var mapInnerOutput map[string]json.RawMessage
	suite.Require().Nil(json.Unmarshal(mapTopOutput["kv"], &mapInnerOutput))

	suite.Require().Contains(mapInnerOutput, "total_count")
	suite.Require().Contains(mapInnerOutput, "top_requests")

	var totalCount int
	suite.Require().Nil(json.Unmarshal(mapInnerOutput["total_count"], &totalCount))
	suite.Assert().Equal(4, totalCount)

	suite.Assert().Equal(expectedJsonOutput, []byte(mapInnerOutput["top_requests"]), fmt.Sprintf("Output was not as expected.\nExpected: %s\n     Got: %s", string(expectedJsonOutput), string(mapInnerOutput["top_requests"])))
}
