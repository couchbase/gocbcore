package gocbcore

import (
	cavescli "github.com/couchbaselabs/gocaves/client"
	"log"
	"sync"
	"testing"
	"time"
)

var (
	globalBenchSuite *BenchTestSuite
	globalBenchLock  sync.Mutex
)

type BenchTestSuite struct {
	*TestConfig
	agent    *Agent
	mockInst *cavescli.Client
	runID    string
}

func GetBenchSuite() *BenchTestSuite {
	globalBenchLock.Lock()
	if globalBenchSuite == nil {
		s := &BenchTestSuite{}
		s.Setup()

		globalBenchSuite = s
	}
	s := globalBenchSuite
	globalBenchLock.Unlock()

	return s
}

func (suite *BenchTestSuite) initAgent(config AgentConfig) (*Agent, error) {
	agent, err := CreateAgent(&config)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(10*time.Second),
		WaitUntilReadyOptions{},
		func(result *WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return nil, err
	}

	err = <-ch
	if err != nil {
		return nil, err
	}

	return agent, nil
}

func (suite *BenchTestSuite) Setup() {
	if globalTestConfig.ConnStr == "" {
		suite.mockInst, suite.runID = setupMock(true)
	}

	suite.TestConfig = globalTestConfig

	config := makeAgentConfig(globalTestConfig)

	config.IoConfig.UseCollections = suite.SupportsFeature(TestFeatureCollections)
	config.BucketName = globalTestConfig.BucketName

	var err error
	suite.agent, err = suite.initAgent(config)
	if err != nil {
		panic(err)
	}
}

func (suite *BenchTestSuite) Close() error {
	if err := suite.agent.Close(); err != nil {
		return err
	}

	if suite.mockInst != nil {
		_, err := suite.mockInst.EndTesting(suite.runID)
		if err != nil {
			log.Printf("Failed to end testing: %v", err)
			return err
		}
		err = suite.mockInst.Shutdown()
		if err != nil {
			return err
		}
	}

	return nil
}

func (suite *BenchTestSuite) GetHarness(b *testing.B) *TestSubHarness {
	return makeTestSubHarness(b)
}

func (suite *BenchTestSuite) GetAgent() *Agent {
	return suite.agent
}

func (suite *BenchTestSuite) GetAgentAndHarness(b *testing.B) (*Agent, *TestSubHarness) {
	h := suite.GetHarness(b)
	return suite.GetAgent(), h
}

func (suite *BenchTestSuite) SupportsFeature(feature TestFeatureCode) bool {
	featureFlagValue := 0
	for _, featureFlag := range suite.FeatureFlags {
		if featureFlag.Feature == feature || featureFlag.Feature == "*" {
			if featureFlag.Enabled {
				featureFlagValue = +1
			} else {
				featureFlagValue = -1
			}
		}
	}
	if featureFlagValue == -1 {
		return false
	} else if featureFlagValue == +1 {
		return true
	}

	switch feature {
	case TestFeatureCollections:
		return !suite.ClusterVersion.Lower(srvVer700)
	case TestFeatureTransactions:
		return !suite.ClusterVersion.Lower(srvVer700)
	}

	panic("found unsupported feature code")
}

func (suite *BenchTestSuite) EnsureSupportsFeature(feature TestFeatureCode, b *testing.B) {
	if !suite.SupportsFeature(feature) {
		b.Skipf("Skipping test due to disabled feature code: %s", feature)
	}
}

func (suite *BenchTestSuite) RunParallel(b *testing.B, runCB func(func(error)) error) {
	agent := suite.GetAgent()
	maxConcurrency := agent.kvMux.queueSize
	buf := make(chan struct{}, maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		buf <- struct{}{}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var wg sync.WaitGroup
		for pb.Next() {
			<-buf
			wg.Add(1)
			err := runCB(func(cbErr error) {
				if cbErr != nil {
					b.Errorf("Operation failed: %v", cbErr)
				}
				wg.Done()
				buf <- struct{}{}
			})
			if err != nil {
				buf <- struct{}{}
				wg.Done()
				b.Fatalf("Failed to run operation: %v", err)
			}
		}
		wg.Wait()
	})
}

func (suite *BenchTestSuite) RunParallelTxn(b *testing.B, config *TransactionsConfig, runCB func(*Transaction, func(error)) error) {
	agent := suite.GetAgent()
	maxConcurrency := agent.kvMux.queueSize
	buf := make(chan struct{}, maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		buf <- struct{}{}
	}

	txns, err := InitTransactions(config)
	if err != nil {
		b.Fatalf("Failed to init transactions: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		txn, err := txns.BeginTransaction(nil)
		if err != nil {
			b.Fatalf("Failed to begin transactios: %v", err)
		}
		err = txn.NewAttempt()
		if err != nil {
			b.Fatalf("Failed to create new attempt: %v", err)
		}

		var wg sync.WaitGroup
		for pb.Next() {
			<-buf
			wg.Add(1)
			err := runCB(txn, func(cbErr error) {
				if cbErr != nil {
					b.Errorf("Operation failed: %v", cbErr)
				}
				wg.Done()
				buf <- struct{}{}
			})
			if err != nil {
				buf <- struct{}{}
				wg.Done()
				b.Fatalf("Failed to run operation: %v", err)
			}
		}
		wg.Wait()
		err = testBlkCommit(txn)
		if err != nil {
			b.Fatalf("Failed to commit attempt: %v", err)
		}
	})
}
