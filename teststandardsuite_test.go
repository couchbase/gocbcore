package gocbcore

import (
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	cavescli "github.com/couchbaselabs/gocaves/client"
	"github.com/stretchr/testify/suite"
)

type StandardTestSuite struct {
	suite.Suite

	*TestConfig
	agentGroup *AgentGroup
	mockInst   *cavescli.Client
	runID      string
	tracer     *testTracer
	meter      *testMeter
}

func (suite *StandardTestSuite) BeforeTest(suiteName, testName string) {
	suite.tracer.Reset()
	suite.meter.Reset()
}

func (suite *StandardTestSuite) SetupSuite() {
	if globalTestConfig.ConnStr == "" {
		m, err := cavescli.NewClient(cavescli.NewClientOptions{
			Version: "v0.0.1-53",
		})
		if err != nil {
			panic(err)
		}

		suite.mockInst = m
		suite.runID = uuid.New().String()

		connstr, err := m.StartTesting(suite.runID, "gocbcore-"+Version())
		if err != nil {
			panic(err)
		}

		globalTestConfig.ConnStr = connstr
		globalTestConfig.BucketName = "default"
		globalTestConfig.MemdBucketName = "memd"
		globalTestConfig.Authenticator = &PasswordAuthProvider{
			Username: "Administrator",
			Password: "password",
		}

		// gocbcore itself doesn't use the default client but the mock downloader does so let's make sure that it
		// doesn't hold any goroutines open which will affect our goroutine leak detector.
		http.DefaultClient.CloseIdleConnections()
	}

	suite.TestConfig = globalTestConfig
	suite.tracer = newTestTracer()
	suite.meter = newTestMeter()
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
}

func (suite *StandardTestSuite) TearDownSuite() {
	err := suite.agentGroup.Close()
	suite.Require().Nil(err, err)

	if suite.mockInst != nil {
		_, err := suite.mockInst.EndTesting(suite.runID)
		if err != nil {
			log.Printf("Failed to end testing: %v", err)
		}
		err = suite.mockInst.Shutdown()
		suite.Require().Nil(err, err)
	}
}

func (suite *StandardTestSuite) TimeTravel(waitDura time.Duration) {
	if suite.mockInst == nil {
		time.Sleep(waitDura)
		return
	}

	err := suite.mockInst.TimeTravelRun(suite.runID, waitDura)
	suite.Require().Nil(err, err)
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
		return !suite.ClusterVersion.Lower(srvVer660)
	case TestFeatureReplaceBodyWithXattr:
		return !suite.IsMockServer() && !suite.ClusterVersion.Lower(srvVer700)
	case TestFeatureExpandMacros:
		return !suite.ClusterVersion.Lower(srvVer450)
	case TestFeatureExpandMacrosSeqNo:
		return !suite.IsMockServer() && !suite.ClusterVersion.Lower(srvVer450)
	case TestFeaturePreserveExpiry:
		return !suite.IsMockServer() && !suite.ClusterVersion.Lower(srvVer700)
	case TestFeatureNSServer:
		return !suite.IsMockServer()
	}

	panic("found unsupported feature code")
}

func (suite *StandardTestSuite) DefaultAgent() *Agent {
	return suite.agentGroup.GetAgent(globalTestConfig.BucketName)
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

type TestSpec struct {
	Agent      *Agent
	Collection string
	Scope      string
	Tracer     *testTracer
	Meter      *testMeter
}

func (suite *StandardTestSuite) StartTest(name TestName) TestSpec {
	spec, err := suite.mockInst.StartTest(suite.runID, string(name))
	suite.Require().Nil(err)

	if spec.ConnStr == "" {
		return TestSpec{
			Agent:      suite.DefaultAgent(),
			Collection: globalTestConfig.CollectionName,
			Scope:      globalTestConfig.ScopeName,
			Tracer:     suite.tracer,
			Meter:      suite.meter,
		}
	}

	baseCfg := globalTestConfig.Clone()
	baseCfg.ConnStr = spec.ConnStr

	tracer := newTestTracer()
	meter := newTestMeter()

	cfg := suite.makeAgentConfig(baseCfg)
	cfg.BucketName = spec.BucketName
	cfg.TracerConfig.Tracer = tracer
	cfg.MeterConfig.Meter = meter

	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)

	return TestSpec{
		Agent:      agent,
		Scope:      spec.ScopeName,
		Collection: spec.CollectionName,
		Tracer:     tracer,
		Meter:      meter,
	}
}

func (suite *StandardTestSuite) EndTest(spec TestSpec) {
	agent := spec.Agent
	if agent == suite.DefaultAgent() {
		return
	}

	err := agent.Close()
	suite.Assert().Nil(err, err)

	err = suite.mockInst.EndTest(suite.runID)
	suite.Require().Nil(err, err)
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

	config.IoConfig = IoConfig{
		UseDurations:           true,
		UseMutationTokens:      true,
		UseCollections:         true,
		UseOutOfOrderResponses: true,
	}

	config.SecurityConfig.Auth = testConfig.Authenticator

	if testConfig.CAProvider != nil {
		config.SecurityConfig.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func (suite *StandardTestSuite) makeAgentGroupConfig(testConfig *TestConfig) AgentGroupConfig {
	config := AgentGroupConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.IoConfig = IoConfig{
		UseDurations:           true,
		UseMutationTokens:      true,
		UseCollections:         true,
		UseOutOfOrderResponses: true,
	}
	config.TracerConfig.Tracer = suite.tracer
	config.MeterConfig.Meter = suite.meter

	config.SecurityConfig.Auth = testConfig.Authenticator

	if config.SecurityConfig.UseTLS {
		if testConfig.CAProvider == nil {
			config.SecurityConfig.TLSRootCAProvider = func() *x509.CertPool {
				return nil
			}
		} else {
			config.SecurityConfig.TLSRootCAProvider = testConfig.CAProvider
		}
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

func (suite *StandardTestSuite) tryUntil(deadline time.Time, interval time.Duration, fn func() bool) bool {
	for {
		success := fn()
		if success {
			return true
		}

		sleepDeadline := time.Now().Add(interval)
		if sleepDeadline.After(deadline) {
			return false
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))
	}
}

func (suite *StandardTestSuite) mustMarshal(content interface{}) []byte {
	b, err := json.Marshal(content)
	suite.Require().Nil(err, err)

	return b
}

func (suite *StandardTestSuite) mustSetDoc(agent *Agent, s *TestSubHarness, key []byte, content interface{}) (casOut Cas) {
	s.PushOp(agent.Set(SetOptions{
		Key:            key,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Value:          suite.mustMarshal(content),
	}, func(result *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Expected error to be nil but was %v", err)
			}

			if result.Cas == 0 {
				s.Fatalf("Expected cas to be non 0")
			}

			casOut = result.Cas
		})
	}))
	s.Wait(0)

	return
}

func (suite *StandardTestSuite) mustGetDoc(agent *Agent, s *TestSubHarness, key []byte) (valOut []byte, casOut Cas) {
	s.PushOp(agent.Get(GetOptions{
		Key:            key,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(result *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Expected error to be nil but was %v", err)
			}

			valOut = result.Value
			casOut = result.Cas
		})
	}))
	s.Wait(0)

	return
}

func (suite *StandardTestSuite) lookupDoc(agent *Agent, s *TestSubHarness, ops []SubDocOp,
	key []byte) (valOut *LookupInResult, errOut error) {
	s.PushOp(agent.LookupIn(LookupInOptions{
		Key:            key,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Ops:            ops,
	}, func(result *LookupInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Expected error to be nil but was %v", err)
			}

			valOut = result
		})
	}))
	s.Wait(0)

	return
}

func (suite *StandardTestSuite) mutateIn(agent *Agent, s *TestSubHarness, ops []SubDocOp, key []byte,
	cas Cas, flags memd.SubdocDocFlag) (casOut Cas, errOut error) {
	s.PushOp(agent.MutateIn(MutateInOptions{
		Key:            key,
		Cas:            cas,
		Ops:            ops,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Flags:          flags,
	}, func(result *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				errOut = err
				return
			}
			casOut = result.Cas
		})
	}))
	s.Wait(0)
	return
}

func (suite *StandardTestSuite) CreateNSAgentConfig() (*AgentConfig, string) {
	defaultAgent := suite.DefaultAgent()
	snapshot, err := defaultAgent.kvMux.PipelineSnapshot()
	suite.Require().Nil(err, err)

	if snapshot.NumPipelines() == 1 {
		suite.T().Skip("Skipping test due to cluster only containing one node")
	}

	srcCfg := suite.makeAgentConfig(globalTestConfig)
	if len(srcCfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}
	seedAddr := srcCfg.SeedConfig.HTTPAddrs[0]
	parts := strings.Split(seedAddr, ":")

	if parts[1] != "8091" && parts[1] != "11210" {
		// This should work with non default ports but it makes the test logic too complicated.
		// This implicitly means that if TLS is enabled then this test won't run.
		suite.T().Skip("Skipping test due to non default ports have been supplied")
	}

	connstr := fmt.Sprintf("ns_server://%s", seedAddr)
	config := &AgentConfig{}
	err = config.FromConnStr(connstr)
	suite.Require().Nil(err, err)

	config.IoConfig = IoConfig{
		UseDurations:           true,
		UseMutationTokens:      true,
		UseCollections:         true,
		UseOutOfOrderResponses: true,
	}

	config.SecurityConfig.Auth = globalTestConfig.Authenticator
	config.SecurityConfig.UseTLS = true

	config.SecurityConfig.TLSRootCAProvider = func() *x509.CertPool {
		return nil
	}

	config.BucketName = globalTestConfig.BucketName

	return config, seedAddr
}

func (suite *StandardTestSuite) VerifyKVMetrics(meter *testMeter, operation string, num int, atLeastNum bool, zeroLenAllowed bool) {
	suite.VerifyMetrics(meter, makeMetricsKey("kv", operation), num, atLeastNum, zeroLenAllowed)
}

func (suite *StandardTestSuite) VerifyMetrics(meter *testMeter, key string, num int, atLeastNum bool, zeroLenAllowed bool) {
	meter.lock.Lock()
	defer meter.lock.Unlock()
	recorders := meter.recorders
	if suite.Assert().Contains(recorders, key) {
		if atLeastNum {
			suite.Assert().GreaterOrEqual(len(recorders[key].values), num)
		} else {
			suite.Assert().Len(recorders[key].values, num)
		}
		for _, val := range recorders[key].values {
			if !zeroLenAllowed {
				suite.Assert().NotZero(val)
			}
		}
	}
}

func TestStandardSuite(t *testing.T) {
	if globalTestConfig == nil {
		t.Skip()
	}

	suite.Run(t, new(StandardTestSuite))
}
