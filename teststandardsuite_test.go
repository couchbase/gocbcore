package gocbcore

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v9/jcbmock"
	"github.com/stretchr/testify/suite"
)

type StandardTestSuite struct {
	suite.Suite

	*TestConfig
	defaultAgent *Agent
	memdAgent    *Agent
	mockInst     *jcbmock.Mock
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

		var couchbaseAddrs []string
		for _, mcport := range mock.MemcachedPorts() {
			couchbaseAddrs = append(couchbaseAddrs, fmt.Sprintf("127.0.0.1:%d", mcport))
		}

		globalTestConfig.ConnStr = fmt.Sprintf("couchbase://%s", strings.Join(couchbaseAddrs, ","))
		globalTestConfig.BucketName = "default"
		globalTestConfig.MemdBucketName = "memd"
		globalTestConfig.Username = "Administrator"
		globalTestConfig.Password = "password"
	}

	suite.TestConfig = globalTestConfig
	var err error
	suite.defaultAgent, err = suite.initAgent(suite.makeBaseAgentConfig(globalTestConfig))
	suite.Require().Nil(err)

	if suite.SupportsFeature(TestFeatureMemd) {
		suite.memdAgent, err = suite.initAgent(suite.makeMemdAgentConfig(globalTestConfig))
		suite.Require().Nil(err)
	}
}

func (suite *StandardTestSuite) TearDownSuite() {
	if suite.memdAgent != nil {
		suite.memdAgent.Close()
		suite.memdAgent = nil
	}

	if suite.defaultAgent != nil {
		suite.defaultAgent.Close()
		suite.defaultAgent = nil
	}
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
	case TestFeatureN1ql:
		return !suite.IsMockServer() && !suite.ClusterVersion.Equal(srvVer700)
	case TestFeatureCbas:
		return !suite.IsMockServer() && suite.ClusterVersion.Higher(srvVer600) &&
			!suite.ClusterVersion.Equal(srvVer700)
	case TestFeatureAdjoin:
		return !suite.IsMockServer()
	case TestFeatureCollections:
		return !suite.IsMockServer()
	case TestFeatureMemd:
		return !suite.IsMockServer()
	case TestFeatureGetMeta:
		return !suite.IsMockServer()
	}

	panic("found unsupported feature code")
}

func (suite *StandardTestSuite) DefaultAgent() *Agent {
	return suite.defaultAgent
}

func (suite *StandardTestSuite) MemdAgent() *Agent {
	return suite.memdAgent
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

func (suite *StandardTestSuite) makeBaseAgentConfig(testConfig *TestConfig) AgentConfig {
	config := AgentConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.UseMutationTokens = true
	config.UseCollections = true
	config.BucketName = testConfig.BucketName

	config.Auth = &PasswordAuthProvider{
		Username: testConfig.Username,
		Password: testConfig.Password,
	}

	return config
}

func (suite *StandardTestSuite) makeMemdAgentConfig(testConfig *TestConfig) AgentConfig {
	config := AgentConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.BucketName = testConfig.MemdBucketName

	config.Auth = &PasswordAuthProvider{
		Username: testConfig.Username,
		Password: testConfig.Password,
	}

	return config
}

func (suite *StandardTestSuite) initAgent(config AgentConfig) (*Agent, error) {
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

func TestStandardSuite(t *testing.T) {
	if globalTestConfig == nil {
		t.Skip()
	}

	suite.Run(t, new(StandardTestSuite))
}
