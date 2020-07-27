package gocbcore

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v9/jcbmock"
	"github.com/stretchr/testify/suite"
)

type StandardTestSuite struct {
	suite.Suite

	*TestConfig
	agentGroup *AgentGroup
	mockInst   *jcbmock.Mock
	tracer     *testTracer
}

func (suite *StandardTestSuite) BeforeTest(suiteName, testName string) {
	suite.tracer.Reset()
}

func (suite *StandardTestSuite) SetupSuite() {
	if globalTestConfig.ConnStr == "" {
		mpath, err := jcbmock.GetMockPath()
		suite.Require().Nil(err)

		mock, err := jcbmock.NewMock(mpath, 4, 1, 64, []jcbmock.BucketSpec{
			{Name: "default", Type: jcbmock.BCouchbase},
			{Name: "memd", Type: jcbmock.BMemcached},
		}...)
		suite.Require().Nil(err)

		mock.Control(jcbmock.NewCommand(jcbmock.CSetCCCP,
			map[string]interface{}{"enabled": "true"}))
		mock.Control(jcbmock.NewCommand(jcbmock.CSetSASLMechanisms,
			map[string]interface{}{"mechs": []string{"SCRAM-SHA512"}}))

		suite.mockInst = mock

		// Unfortunately we have to use http with the mock. Config polling has to be done over HTTP for memcached
		// buckets and if we use a non-default port with the couchbase protocol then we won't automatically
		// translate over the http address(es) and so we can never fetch the cluster config for the memd bucket.
		globalTestConfig.ConnStr = fmt.Sprintf("http://127.0.0.1:%d", mock.EntryPort)
		globalTestConfig.BucketName = "default"
		globalTestConfig.MemdBucketName = "memd"
		globalTestConfig.Authenticator = &PasswordAuthProvider{
			Username: "Administrator",
			Password: "password",
		}
	}

	suite.TestConfig = globalTestConfig
	suite.tracer = newTestTracer()
	var err error
	suite.agentGroup, err = suite.initAgentGroup(suite.makeAgentGroupConfig(globalTestConfig))
	suite.Require().Nil(err, err)

	err = suite.agentGroup.OpenBucket(globalTestConfig.BucketName)
	suite.Require().Nil(err, err)

	// If we don't do a wait until ready then it can be difficult to verify tracing behavior on the
	// first test that runs.
	s := suite.GetHarness()
	s.PushOp(suite.DefaultAgent().WaitUntilReady(
		time.Now().Add(5*time.Second),
		WaitUntilReadyOptions{},
		func(result *WaitUntilReadyResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("WaitUntilReady operation failed: %v", err)
				}
			})
		}),
	)
	s.Wait(0)

	if suite.SupportsFeature(TestFeatureMemd) {
		err = suite.agentGroup.OpenBucket(globalTestConfig.MemdBucketName)
		suite.Require().Nil(err, err)

		s := suite.GetHarness()
		s.PushOp(suite.MemdAgent().WaitUntilReady(
			time.Now().Add(5*time.Second),
			WaitUntilReadyOptions{},
			func(result *WaitUntilReadyResult, err error) {
				s.Wrap(func() {
					if err != nil {
						s.Fatalf("WaitUntilReady operation failed: %v", err)
					}
				})
			}),
		)
		s.Wait(0)
	}
}

func (suite *StandardTestSuite) TearDownSuite() {
	err := suite.agentGroup.Close()
	suite.Require().Nil(err, err)
}

func (suite *StandardTestSuite) TimeTravel(waitDura time.Duration) {
	TimeTravel(waitDura, suite.mockInst)
}

func (suite *StandardTestSuite) IsMockServer() bool {
	return suite.mockInst != nil
}

func (suite *StandardTestSuite) SupportsFeature(feature TestFeatureCode) bool {
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
	case TestFeatureSsl:
		return true
	case TestFeatureViews:
		return true
	case TestFeatureErrMap:
		return true
	case TestFeatureReplicas:
		return true
	case TestFeatureMemd:
		return true
	case TestFeatureN1ql:
		return !suite.IsMockServer() && !suite.ClusterVersion.Equal(srvVer650DP)
	case TestFeatureCbas:
		return !suite.IsMockServer() && suite.ClusterVersion.Higher(srvVer600) &&
			!suite.ClusterVersion.Equal(srvVer650DP)
	case TestFeatureFts:
		return !suite.IsMockServer() && !suite.ClusterVersion.Lower(srvVer551)
	case TestFeatureAdjoin:
		return !suite.IsMockServer()
	case TestFeatureCollections:
		return !suite.IsMockServer() && (suite.ClusterVersion.Equal(srvVer650DP) || !suite.ClusterVersion.Lower(srvVer700))
	case TestFeatureGetMeta:
		return !suite.IsMockServer()
	case TestFeatureGCCCP:
		return !suite.IsMockServer() && !suite.ClusterVersion.Lower(srvVer650)
	case TestFeaturePingServices:
		return !suite.IsMockServer()
	case TestFeatureEnhancedDurability:
		return !suite.IsMockServer() && !suite.ClusterVersion.Lower(srvVer650)
	case TestFeatureCreateDeleted:
		return !suite.IsMockServer() && !suite.ClusterVersion.Lower(srvVer660)
	}

	panic("found unsupported feature code")
}

func (suite *StandardTestSuite) DefaultAgent() *Agent {
	return suite.agentGroup.GetAgent(globalTestConfig.BucketName)
}

func (suite *StandardTestSuite) MemdAgent() *Agent {
	return suite.agentGroup.GetAgent(globalTestConfig.MemdBucketName)
}

func (suite *StandardTestSuite) AgentGroup() *AgentGroup {
	return suite.agentGroup
}

func (suite *StandardTestSuite) GetHarness() *TestSubHarness {
	return makeTestSubHarness(suite.T())
}

func (suite *StandardTestSuite) GetAgentAndHarness() (*Agent, *TestSubHarness) {
	h := suite.GetHarness()
	return suite.DefaultAgent(), h
}

func (suite *StandardTestSuite) EnsureSupportsFeature(feature TestFeatureCode) {
	if !suite.SupportsFeature(feature) {
		suite.T().Skipf("Skipping test due to disabled feature code: %s", feature)
	}
}

func (suite *StandardTestSuite) LoadConfigFromFile(filename string) (cfg *cfgBucket) {
	s, err := ioutil.ReadFile(filename)
	if err != nil {
		suite.T().Fatal(err.Error())
	}
	rawCfg, err := parseConfig(s, "localhost")
	if err != nil {
		suite.T().Fatal(err.Error())
	}

	cfg = rawCfg
	return
}

func (suite *StandardTestSuite) makeAgentConfig(testConfig *TestConfig) AgentConfig {
	config := AgentConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.UseMutationTokens = true
	config.UseCollections = true
	config.UseOutOfOrderResponses = true

	config.Auth = testConfig.Authenticator

	if testConfig.CAProvider != nil {
		config.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func (suite *StandardTestSuite) makeAgentGroupConfig(testConfig *TestConfig) AgentGroupConfig {
	config := AgentGroupConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.UseMutationTokens = true
	config.UseCollections = true
	config.UseOutOfOrderResponses = true
	config.Tracer = suite.tracer

	config.Auth = testConfig.Authenticator

	if testConfig.CAProvider != nil {
		config.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func (suite *StandardTestSuite) initAgentGroup(config AgentGroupConfig) (*AgentGroup, error) {
	ag, err := CreateAgentGroup(&config)
	if err != nil {
		return nil, err
	}

	return ag, nil
}

func (suite *StandardTestSuite) tryAtMost(times int, interval time.Duration, fn func() bool) bool {
	i := 0
	for {
		success := fn()
		if success {
			return true
		}

		i++
		if i >= times {
			return false
		}
		time.Sleep(interval)
	}
}

func TestStandardSuite(t *testing.T) {
	if globalTestConfig == nil {
		t.Skip()
	}

	suite.Run(t, new(StandardTestSuite))
}
