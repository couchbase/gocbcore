package gocbcore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/stretchr/testify/suite"
)

type DCPTestSuite struct {
	suite.Suite

	*DCPTestConfig
	opAgent  *Agent
	dcpAgent *DCPAgent
}

func (suite *DCPTestSuite) SupportsFeature(feature TestFeatureCode) bool {
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
	case TestFeatureDCPExpiry:
		return !suite.ClusterVersion.Lower(srvVer650)
	case TestFeatureDCPDeleteTimes:
		return !suite.ClusterVersion.Lower(srvVer650)
	}

	panic("found unsupported feature code")
}

func (suite *DCPTestSuite) SetupSuite() {
	suite.DCPTestConfig = globalDCPTestConfig
	suite.Require().NotEmpty(suite.DCPTestConfig.ConnStr, "Connection string cannot be empty for testing DCP")

	var err error
	suite.opAgent, err = suite.initAgent(suite.makeOpAgentConfig(suite.DCPTestConfig))
	suite.Require().Nil(err)

	flags := memd.DcpOpenFlagProducer

	if suite.SupportsFeature(TestFeatureDCPDeleteTimes) {
		flags |= memd.DcpOpenFlagIncludeDeleteTimes
	}

	suite.dcpAgent, err = suite.initDCPAgent(
		suite.makeDCPAgentConfig(suite.DCPTestConfig, suite.SupportsFeature(TestFeatureDCPExpiry)),
		flags,
	)
	suite.Require().Nil(err, err)
}

func (suite *DCPTestSuite) TearDownSuite() {
	if suite.opAgent != nil {
		suite.opAgent.Close()
		suite.opAgent = nil
	}

	if suite.dcpAgent != nil {
		suite.dcpAgent.Close()
		suite.dcpAgent = nil
	}
}

func (suite *DCPTestSuite) makeOpAgentConfig(testConfig *DCPTestConfig) AgentConfig {
	config := AgentConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.UseMutationTokens = true
	config.UseCollections = true
	config.BucketName = testConfig.BucketName

	config.Auth = testConfig.Authenticator

	if testConfig.CAProvider != nil {
		config.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func (suite *DCPTestSuite) initAgent(config AgentConfig) (*Agent, error) {
	agent, err := CreateAgent(&config)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(2*time.Second),
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

func (suite *DCPTestSuite) makeDCPAgentConfig(testConfig *DCPTestConfig, expiryEnabled bool) DCPAgentConfig {
	config := DCPAgentConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.UseCollections = true
	config.BucketName = testConfig.BucketName

	if expiryEnabled {
		config.UseExpiryOpcode = true
	}

	config.Auth = testConfig.Authenticator

	if testConfig.CAProvider != nil {
		config.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func (suite *DCPTestSuite) initDCPAgent(config DCPAgentConfig, openFlags memd.DcpOpenFlag) (*DCPAgent, error) {
	agent, err := CreateDcpAgent(&config, "test-stream", openFlags)
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

func TestDCPSuite(t *testing.T) {
	if globalDCPTestConfig == nil {
		t.Skip()
	}
	suite.Run(t, new(DCPTestSuite))
}

func (suite *DCPTestSuite) TestBasic() {
	mutations, deletionKeys := suite.runMutations()

	h := makeTestSubHarness(suite.T())

	var seqnos []VbSeqNoEntry
	snapshot, err := suite.dcpAgent.ConfigSnapshot()
	suite.Require().Nil(err, err)

	numNodes, err := snapshot.NumServers()
	suite.Require().Nil(err, err)

	for i := 1; i < numNodes+1; i++ {
		h.PushOp(suite.dcpAgent.GetVbucketSeqnos(i, memd.VbucketStateActive, GetVbucketSeqnoOptions{},
			func(entries []VbSeqNoEntry, err error) {
				h.Wrap(func() {
					if err != nil {
						h.Fatalf("GetVbucketSeqnos operation failed: %v", err)
						return
					}

					seqnos = append(seqnos, entries...)
				})
			}))
		h.Wait(0)
	}

	suite.T().Logf("Running with seqno map: %v", seqnos)

	so := &TestStreamObserver{
		mutations:   make(map[string]Mutation),
		deletions:   make(map[string]Deletion),
		expirations: make(map[string]Deletion),
	}
	so.endWg.Add(len(seqnos))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var openWg sync.WaitGroup
	openWg.Add(len(seqnos))

	for _, entry := range seqnos {
		go func(en VbSeqNoEntry) {
			ch := make(chan error)
			op, err := suite.dcpAgent.OpenStream(en.VbID, memd.DcpStreamAddFlagActiveOnly, 0, 0, en.SeqNo,
				0, 0, so, OpenStreamOptions{}, func(entries []FailoverEntry, err error) {
					ch <- err
				},
			)
			if err != nil {
				cancel()
				return
			}

			select {
			case err := <-ch:
				if err != nil {
					suite.T().Logf("Error received from open stream: %v", err)
					cancel()
					return
				}
			case <-ctx.Done():
				op.Cancel()
				return
			}

			openWg.Done()
		}(entry)
	}

	wgCh := make(chan struct{}, 1)
	go func() {
		openWg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		suite.T().Fatal("Failed to open streams")
	case <-wgCh:
		cancel()
		// Let any expirations do their thing
		time.Sleep(5 * time.Second)
	}

	suite.T().Logf("All streams open, waiting for streams to complete")

	waitCh := make(chan struct{})
	go func() {
		so.endWg.Wait()
		close(waitCh)
	}()

	select {
	case <-time.After(60 * time.Second):
		suite.T().Fatal("Timed out waiting for streams to complete")
	case <-waitCh:
	}

	suite.T().Logf("All streams complete")

	// Compaction can run and cause expirations to be hidden from us
	suite.Assert().InDelta(suite.NumMutations, len(so.mutations), float64(suite.NumExpirations))
	suite.Assert().Equal(suite.NumDeletions, len(so.deletions))
	suite.Assert().InDelta(suite.NumExpirations, len(so.expirations), float64(suite.NumExpirations))

	for key, val := range mutations {
		if suite.Assert().Contains(so.mutations, key) {
			suite.Assert().Equal(string(so.mutations[key].Value), val)
		}
	}

	for _, key := range deletionKeys {
		suite.Assert().Contains(so.deletions, key)
	}
}

func (suite *DCPTestSuite) runMutations() (map[string]string, []string) {
	expirationsLeft := suite.NumExpirations
	deletionsLeft := int32(suite.NumDeletions)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(suite.NumMutations)

	mutations := make(map[string]string)
	var deletionKeys []string
	lock := new(sync.Mutex)

	for i := 0; i < suite.NumMutations; i++ {
		var expiry uint32
		if expirationsLeft > 0 {
			expiry = 1
			expirationsLeft--
		}

		go func(ex uint32, id int) {
			ch := make(chan error)
			op, err := suite.opAgent.Set(
				SetOptions{
					Key:    []byte(fmt.Sprintf("key-%d", id)),
					Value:  []byte(fmt.Sprintf("value-%d", id)),
					Expiry: ex,
				}, func(result *StoreResult, err error) {
					ch <- err
				},
			)
			if err != nil {
				cancel()
				return
			}

			select {
			case err := <-ch:
				if err != nil {
					cancel()
					return
				}
			case <-ctx.Done():
				op.Cancel()
				return
			}

			if expiry == 0 {
				dLeft := atomic.AddInt32(&deletionsLeft, -1)
				if dLeft >= 0 {
					lock.Lock()
					deletionKeys = append(deletionKeys, fmt.Sprintf("key-%d", id))
					lock.Unlock()
					ch = make(chan error)
					op, err := suite.opAgent.Delete(
						DeleteOptions{
							Key: []byte(fmt.Sprintf("key-%d", id)),
						}, func(result *DeleteResult, err error) {
							ch <- err
						},
					)
					if err != nil {
						cancel()
						return
					}

					select {
					case err := <-ch:
						if err != nil {
							cancel()
							return
						}
					case <-ctx.Done():
						op.Cancel()
						return
					}
				} else {
					lock.Lock()
					mutations[fmt.Sprintf("key-%d", id)] = fmt.Sprintf("value-%d", id)
					lock.Unlock()
				}
			}
			wg.Done()
		}(expiry, i)
	}

	wgCh := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		suite.T().Fatal("Failed to perform mutations")
	case <-wgCh:
		cancel()
		// Let any expirations do their thing
		time.Sleep(5 * time.Second)
	}

	return mutations, deletionKeys
}
