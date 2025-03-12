package gocbcore

import (
	"github.com/couchbase/gocbcore/v10/memd"
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
	return CreateAgent(&cfg)
}

func createAgentGroupWithTelemetryReporter(reporter *TelemetryReporter) (*AgentGroup, error) {
	cfg := makeAgentGroupConfig(globalTestConfig)
	cfg.TelemetryConfig.TelemetryReporter = reporter
	return CreateAgentGroup(&cfg)
}

func (suite *StandardTestSuite) TestTelemetryWithKvOps() {
	var counts map[string]*uint32

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOp", mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			attrs := args.Get(0).(telemetryOperationAttributes)
			if runtime.GOOS == `windows` && suite.IsMockServer() {
				// Because of GoCaves being faster than a real server & Windows having a lower clock precision, ops
				// can sometimes appear to run instantaneously, we allow the duration to be 0s.
				suite.Assert().GreaterOrEqual(attrs.duration, time.Duration(0))
			} else {
				suite.Assert().Positive(attrs.duration)
			}
			suite.Assert().NotEmpty(attrs.node)
			suite.Assert().Equal(attrs.service, MemdService)
			suite.Assert().Equal(attrs.bucket, suite.BucketName)
			if counts != nil {
				suite.Assert().Equal(telemetryOutcomeSuccess, attrs.outcome)
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
				suite.Require().NoError(err)
				suite.Require().NotZero(res.Cas)
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
				suite.Require().NoError(err)
				suite.Require().NotZero(res.Cas)
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
	var count atomic.Uint32

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOp", mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
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
			suite.Require().NoError(err)
			suite.Require().NotZero(res.Cas)
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
			suite.Require().NoError(err)
			suite.Require().NotZero(res.Cas)
		})
	}))
	s.Wait(0)

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

	// No operations should have reached the telemetry store.
	suite.Assert().Zero(count.Load())
}

func (suite *StandardTestSuite) TestTelemetryWithQueryOps() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	var queryCount uint32
	expectedQueryCount := uint32(4)

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOp", mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			attrs := args.Get(0).(telemetryOperationAttributes)
			suite.Assert().Positive(attrs.duration)
			suite.Assert().NotEmpty(attrs.node)
			suite.Assert().Equal(attrs.service, N1qlService)
			suite.Assert().Empty(attrs.bucket)
			suite.Assert().Equal(telemetryOutcomeSuccess, attrs.outcome)

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
		s.PushOp(ag.N1QLQuery(N1QLQueryOptions{
			Payload: []byte(`{"statement": "SELECT 1=1"}`),
		}, func(res *N1QLRowReader, err error) {
			s.Wrap(func() {
				suite.Require().NoError(err)
				suite.Require().NotNil(res)

				// One row expected
				suite.Require().NotNil(res.NextRow())
				suite.Require().Nil(res.NextRow())
				suite.Require().NoError(res.Err())
			})
		}))
		s.Wait(0)
	}

	suite.Assert().Equal(expectedQueryCount, queryCount)
}

func (suite *StandardTestSuite) TestTelemetryWithQueryOpsNoTelemetryClient() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	var count atomic.Uint32

	mockStore := new(mockTelemetryStore)
	mockStore.On("recordOp", mock.AnythingOfType("gocbcore.telemetryOperationAttributes")).
		Run(func(args mock.Arguments) {
			count.Add(1)
		}).Return()

	ag, err := createAgentGroupWithTelemetryReporter(&TelemetryReporter{metrics: mockStore})
	suite.Require().NoError(err)
	defer ag.Close()
	s := suite.GetHarness()

	for i := uint32(0); i < 2; i++ {
		s.PushOp(ag.N1QLQuery(N1QLQueryOptions{
			Payload: []byte(`{"statement": "SELECT 1=1"}`),
		}, func(res *N1QLRowReader, err error) {
			s.Wrap(func() {
				suite.Require().NoError(err)
				suite.Require().NotNil(res)

				// One row expected
				suite.Require().NotNil(res.NextRow())
				suite.Require().Nil(res.NextRow())
				suite.Require().NoError(res.Err())
			})
		}))
		s.Wait(0)
	}

	suite.Assert().Zero(count.Load())
}
