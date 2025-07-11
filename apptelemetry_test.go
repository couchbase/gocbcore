package gocbcore

import (
	"fmt"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"runtime"
	"sync/atomic"
	"time"
)

// noopTelemetryClient doesn't do anything, except declare that it initiated a connection, so it can be used when tests
// need the telemetryComponent to not skip telemetry reporting.
type noopTelemetryClient struct {
}

func (c *noopTelemetryClient) connectIfNotStarted() {
	return
}

func (c *noopTelemetryClient) connectionInitiated() bool {
	return true
}

func (c *noopTelemetryClient) Close() {
	return
}

func (c *noopTelemetryClient) usesExternalEndpoint() bool {
	return true
}

func (c *noopTelemetryClient) updateEndpoints(telemetryEndpoints) {
	return
}

func createAgentWithTelemetryReporter(reporter *TelemetryReporter) (*Agent, error) {
	cfg := makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	cfg.TelemetryConfig.TelemetryReporter = reporter
	cfg.OrphanReporterConfig.Enabled = true
	return CreateAgent(&cfg)
}

func createAgentGroupWithTelemetryReporter(reporter *TelemetryReporter) (*AgentGroup, error) {
	cfg := makeAgentGroupConfig(globalTestConfig)
	cfg.TelemetryConfig.TelemetryReporter = reporter
	return CreateAgentGroup(&cfg)
}

func (suite *StandardTestSuite) TestTelemetryWithKvOps() {
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)

	var kvNodes []string
	var kvNodeUuids []string
	for _, s := range suite.DefaultAgent().kvMux.getState().kvServerList {
		kvNodes = append(kvNodes, hostnameFromURI(s.Address))
		kvNodeUuids = append(kvNodeUuids, s.NodeUUID)
	}

	var counts map[string]*uint32

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOperationCompletion", mock.AnythingOfType("gocbcore.telemetryOutcome"), mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			outcome := args.Get(0).(telemetryOutcome)
			attrs := args.Get(1).(telemetryOperationAttributes)
			if runtime.GOOS == `windows` && suite.IsMockServer() {
				// Because of GoCaves being faster than a real server & Windows having a lower clock precision, ops
				// can sometimes appear to run instantaneously, we allow the duration to be 0s.
				suite.Assert().GreaterOrEqual(attrs.duration, time.Duration(0))
			} else {
				suite.Assert().Positive(attrs.duration)
			}
			suite.Assert().NotEmpty(attrs.node)
			suite.Assert().Contains(kvNodes, attrs.node)
			if suite.SupportsFeature(TestFeatureNodeUuid) {
				suite.Assert().NotEmpty(attrs.nodeUUID)
				suite.Assert().Contains(kvNodeUuids, attrs.nodeUUID)
			} else {
				suite.Assert().Empty(attrs.nodeUUID)
			}
			suite.Assert().Empty(attrs.altNode)
			suite.Assert().Equal(attrs.service, MemdService)
			suite.Assert().Equal(attrs.bucket, suite.BucketName)
			if counts != nil {
				suite.Assert().Equal(telemetryOutcomeSuccess, outcome)
				if attrs.mutation {
					if attrs.durable {
						atomic.AddUint32(counts["mutation_durable"], 1)
					} else {
						atomic.AddUint32(counts["mutation_non_durable"], 1)
					}
				} else {
					atomic.AddUint32(counts["retrieval"], 1)
				}
			}
		}).Return()

	agent, err := createAgentWithTelemetryReporter(&TelemetryReporter{metrics: mockStore, client: &noopTelemetryClient{}})
	suite.Require().NoError(err)
	defer agent.Close()

	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestTelemetryWithKvOps", suite.CollectionName, suite.ScopeName)

	counts = map[string]*uint32{
		"retrieval":            new(uint32),
		"mutation_durable":     new(uint32),
		"mutation_non_durable": new(uint32),
	}

	expectedRetrievalCount := uint32(10)
	expectedDurableMutationCount := uint32(2)
	expectedNonDurableMutationCount := uint32(5)

	for i := uint32(0); i < expectedNonDurableMutationCount; i++ {
		s.PushOp(agent.Set(SetOptions{
			Key:            []byte("test"),
			Value:          []byte("{}"),
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
		}, func(res *StoreResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Failed to set key: %v", err)
					return
				}
				if res.Cas == 0 {
					s.Fatalf("CAS was zero")
				}
			})
		}))
		s.Wait(0)
	}

	for i := uint32(0); i < expectedDurableMutationCount; i++ {
		s.PushOp(agent.Set(SetOptions{
			Key:                    []byte("test"),
			Value:                  []byte("{}"),
			CollectionName:         suite.CollectionName,
			ScopeName:              suite.ScopeName,
			DurabilityLevel:        memd.DurabilityLevelMajority,
			DurabilityLevelTimeout: 10 * time.Second,
		}, func(res *StoreResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Failed to set key: %v", err)
					return
				}
				if res.Cas == 0 {
					s.Fatalf("CAS was zero")
				}
			})
		}))
		s.Wait(0)
	}

	for i := uint32(0); i < expectedRetrievalCount; i++ {
		s.PushOp(agent.Get(GetOptions{
			Key:            []byte("test"),
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
		}, func(res *GetResult, err error) {
			s.Wrap(func() {
				suite.Require().NoError(err)
				suite.Require().NotZero(res.Cas)
			})
		}))
		s.Wait(0)
	}

	suite.Assert().Equal(expectedRetrievalCount, *counts["retrieval"])
	suite.Assert().Equal(expectedDurableMutationCount, *counts["mutation_durable"])
	suite.Assert().Equal(expectedNonDurableMutationCount, *counts["mutation_non_durable"])
}

func (suite *StandardTestSuite) TestTelemetryWithKvOpsNoTelemetryClient() {
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)
	var count atomic.Uint32

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOperationCompletion", mock.AnythingOfType("gocbcore.telemetryOutcome"), mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			count.Add(1)
		}).Return()

	agent, err := createAgentWithTelemetryReporter(&TelemetryReporter{metrics: mockStore})
	suite.Require().NoError(err)
	defer agent.Close()

	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestTelemetryWithKvOpsClientHasNoConnection", suite.CollectionName, suite.ScopeName)

	// Do some operations
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("test"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Failed to set key: %v", err)
				return
			}
			if res.Cas == 0 {
				s.Fatalf("CAS was zero")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Set(SetOptions{
		Key:                    []byte("test"),
		Value:                  []byte("{}"),
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Failed to set key: %v", err)
				return
			}
			if res.Cas == 0 {
				s.Fatalf("CAS was zero")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("test"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Failed to set key: %v", err)
				return
			}
			if res.Cas == 0 {
				s.Fatalf("CAS was zero")
			}
		})
	}))
	s.Wait(0)

	// No operations should have reached the telemetry store.
	suite.Assert().Zero(count.Load())
}

func (suite *StandardTestSuite) TestTelemetryWithQueryOps() {
	suite.EnsureSupportsFeature(TestFeatureGCCCP)
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	var queryCount uint32
	expectedQueryCount := uint32(4)

	var queryNodes []string
	var queryNodeUuids []string
	for _, s := range suite.DefaultAgent().httpMux.N1qlEps() {
		queryNodes = append(queryNodes, hostnameFromURI(s.Address))
		queryNodeUuids = append(queryNodeUuids, s.NodeUUID)
	}

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOperationCompletion", mock.AnythingOfType("gocbcore.telemetryOutcome"), mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			outcome := args.Get(0).(telemetryOutcome)
			attrs := args.Get(1).(telemetryOperationAttributes)
			suite.Assert().Positive(attrs.duration)
			suite.Assert().NotEmpty(attrs.node)
			suite.Assert().Contains(queryNodes, attrs.node)
			if suite.SupportsFeature(TestFeatureNodeUuid) {
				suite.Assert().NotEmpty(attrs.nodeUUID)
				suite.Assert().Contains(queryNodeUuids, attrs.nodeUUID)
			} else {
				suite.Assert().Empty(attrs.nodeUUID)
			}
			suite.Assert().Empty(attrs.altNode)
			suite.Assert().Equal(attrs.service, N1qlService)
			suite.Assert().Empty(attrs.bucket)
			suite.Assert().Equal(telemetryOutcomeSuccess, outcome)

			// KV-only attributes should be unset
			suite.Assert().Zero(attrs.durable)
			suite.Assert().Zero(attrs.mutation)

			atomic.AddUint32(&queryCount, 1)
		}).Return()

	ag, err := createAgentGroupWithTelemetryReporter(&TelemetryReporter{metrics: mockStore, client: &noopTelemetryClient{}})
	suite.Require().NoError(err)
	defer ag.Close()
	s := suite.GetHarness()

	for i := uint32(0); i < expectedQueryCount; i++ {
		resCh := make(chan *N1QLRowReader, 1)
		s.PushOp(ag.N1QLQuery(N1QLQueryOptions{
			Payload: []byte(`{"statement": "SELECT 1=1"}`),
		}, func(res *N1QLRowReader, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Failed to execute query: %v", err)
					return
				}

				resCh <- res
			})
		}))
		s.Wait(0)

		res := <-resCh
		suite.Require().NotNil(res)

		// One row expected
		suite.Require().NotNil(res.NextRow())
		suite.Require().Nil(res.NextRow())
		suite.Require().NoError(res.Err())
	}

	suite.Assert().Equal(expectedQueryCount, queryCount)
}

func (suite *StandardTestSuite) TestTelemetryWithQueryOpsNoTelemetryClient() {
	suite.EnsureSupportsFeature(TestFeatureGCCCP)
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	var count atomic.Uint32

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOperationCompletion", mock.AnythingOfType("gocbcore.telemetryOutcome"), mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			count.Add(1)
		}).Return()

	ag, err := createAgentGroupWithTelemetryReporter(&TelemetryReporter{metrics: mockStore})
	suite.Require().NoError(err)
	defer ag.Close()
	s := suite.GetHarness()

	for i := uint32(0); i < 2; i++ {
		resCh := make(chan *N1QLRowReader, 1)
		s.PushOp(ag.N1QLQuery(N1QLQueryOptions{
			Payload: []byte(`{"statement": "SELECT 1=1"}`),
		}, func(res *N1QLRowReader, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Failed to execute query: %v", err)
					return
				}

				resCh <- res
			})
		}))
		s.Wait(0)

		res := <-resCh
		suite.Require().NotNil(res)

		// One row expected
		suite.Require().NotNil(res.NextRow())
		suite.Require().Nil(res.NextRow())
		suite.Require().NoError(res.Err())
	}

	suite.Assert().Zero(count.Load())
}

func (suite *StandardTestSuite) TestTelemetryOrphanedResponses() {
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)
	if suite.IsMockServer() {
		suite.T().Skip("Responses are likely to be received before we cancel the request when using mock server")
	}

	var kvNodes []string
	var kvNodeUuids []string
	for _, s := range suite.DefaultAgent().kvMux.getState().kvServerList {
		kvNodes = append(kvNodes, hostnameFromURI(s.Address))
		kvNodeUuids = append(kvNodeUuids, s.NodeUUID)
	}

	var completionCount *atomic.Uint32
	var orphanCount *atomic.Uint32

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOperationCompletion", mock.AnythingOfType("gocbcore.telemetryOutcome"), mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			outcome := args.Get(0).(telemetryOutcome)
			attrs := args.Get(1).(telemetryOperationAttributes)
			suite.Assert().NotEmpty(attrs.node)
			suite.Assert().Contains(kvNodes, attrs.node)
			if suite.SupportsFeature(TestFeatureNodeUuid) {
				suite.Assert().NotEmpty(attrs.nodeUUID)
				suite.Assert().Contains(kvNodeUuids, attrs.nodeUUID)
			} else {
				suite.Assert().Empty(attrs.nodeUUID)
			}
			suite.Assert().Empty(attrs.altNode)
			suite.Assert().Equal(attrs.service, MemdService)
			suite.Assert().Equal(attrs.bucket, suite.BucketName)
			if completionCount != nil {
				suite.Assert().Equal(telemetryOutcomeCanceled, outcome)
				completionCount.Add(1)
				suite.Assert().True(attrs.mutation)
				suite.Assert().True(attrs.durable)
			}
		}).Return()

	mockStore.On("recordOrphanedResponse", mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			attrs := args.Get(0).(telemetryOperationAttributes)
			if runtime.GOOS == `windows` && suite.IsMockServer() {
				// Because of the Windows CI executor having a lower clock precision, ops can sometimes appear to
				// run instantaneously, we allow the duration to be 0s.
				suite.Assert().GreaterOrEqual(attrs.duration, time.Duration(0))
			} else {
				suite.Assert().Positive(attrs.duration)
			}
			suite.Assert().NotEmpty(attrs.node)
			suite.Assert().Contains(kvNodes, attrs.node)
			if suite.SupportsFeature(TestFeatureNodeUuid) {
				suite.Assert().NotEmpty(attrs.nodeUUID)
				suite.Assert().Contains(kvNodeUuids, attrs.nodeUUID)
			} else {
				suite.Assert().Empty(attrs.nodeUUID)
			}
			suite.Assert().Empty(attrs.altNode)
			suite.Assert().Equal(attrs.service, MemdService)
			suite.Assert().Equal(attrs.bucket, suite.BucketName)
			if orphanCount != nil {
				orphanCount.Add(1)
				suite.Assert().True(attrs.mutation)
				suite.Assert().True(attrs.durable)
			}
		}).Return()

	agent, err := createAgentWithTelemetryReporter(&TelemetryReporter{metrics: mockStore, client: &noopTelemetryClient{}})
	suite.Require().NoError(err)
	defer agent.Close()

	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestTelemetryWithKvOps", suite.CollectionName, suite.ScopeName)

	completionCount = new(atomic.Uint32)
	orphanCount = new(atomic.Uint32)

	operationCount := 5

	for i := 0; i < operationCount; i++ {
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
				memdReq.Cancel()
				break
			}
		}
		err = <-errCh
		suite.Require().ErrorIs(err, ErrRequestCanceled)
	}

	suite.Assert().Equal(uint32(operationCount), completionCount.Load())
	suite.Require().Eventually(
		func() bool {
			return orphanCount.Load() == uint32(operationCount)
		},
		20*time.Second,
		10*time.Millisecond,
		"App telemetry reporter did not receive all expected orphaned responses (Expected: %d, Got: %d)",
		operationCount,
		orphanCount.Load())
	suite.Assert().Equal(uint32(operationCount), orphanCount.Load())
}
