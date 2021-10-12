package gocbcore

import (
	"crypto/x509"
	"errors"
	"strings"
	"time"
)

func (suite *StandardTestSuite) VerifyNSKVListTLS(seed string, endpoints []routeEndpoint) {
	for _, ep := range endpoints {
		epParts := strings.Split(ep.Address, "://")
		hostport := strings.Split(epParts[1], ":")
		if hostport[0] == seed {
			suite.Assert().Equal("couchbase", epParts[0])
			suite.Assert().Equal("11210", hostport[1])
			suite.Assert().True(ep.IsSeedNode)
		} else {
			suite.Assert().Equal("couchbases", epParts[0])
			suite.Assert().Equal("11207", hostport[1])
			suite.Assert().False(ep.IsSeedNode)
		}
	}
}

func (suite *StandardTestSuite) VerifyNSKVListNonTLS(endpoints []routeEndpoint) {
	for _, ep := range endpoints {
		epParts := strings.Split(ep.Address, "://")
		hostport := strings.Split(epParts[1], ":")
		suite.Assert().Equal("couchbase", epParts[0])
		suite.Assert().Equal("11210", hostport[1])
	}
}

func (suite *StandardTestSuite) VerifyNSMgmtListTLS(seed string, endpoints []routeEndpoint) {
	for _, ep := range endpoints {
		epParts := strings.Split(ep.Address, "://")
		hostport := strings.Split(epParts[1], ":")
		if hostport[0] == seed {
			suite.Assert().Equal("http", epParts[0])
			suite.Assert().Equal("8091", hostport[1])
			suite.Assert().True(ep.IsSeedNode)
		} else {
			suite.Assert().Equal("https", epParts[0])
			suite.Assert().Equal("18091", hostport[1])
			suite.Assert().False(ep.IsSeedNode)
		}
	}
}

func (suite *StandardTestSuite) VerifyNSMgmtListNonTLS(endpoints []routeEndpoint) {
	for _, ep := range endpoints {
		epParts := strings.Split(ep.Address, "://")
		hostport := strings.Split(epParts[1], ":")
		suite.Assert().Equal("http", epParts[0])
		suite.Assert().Equal("8091", hostport[1])
	}
}

func (suite *StandardTestSuite) TestReconfigureSecurity() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	// This will create a config with TLS enabled
	config, seedAddr := suite.CreateNSAgentConfig()
	splitSeed := strings.Split(seedAddr, ":")

	agent, err := CreateAgent(config)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "test", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurity")

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: false,
	})
	suite.Require().Nil(err, err)

	success := suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		kvMuxState := agent.kvMux.getState()
		kvEps := kvMuxState.kvServerList

		for _, ep := range kvEps {
			epParts := strings.Split(ep.Address, "://")
			hostport := strings.Split(epParts[1], ":")
			if hostport[1] == "11207" {
				suite.T().Logf("Encrypted endpoint found: %s", ep.Address)
				return false
			}
		}

		return true
	})
	suite.Require().True(success, "One or more endpoints never switched to nonTLS")

	kvMuxState := agent.kvMux.getState()
	httpMuxState := agent.httpMux.Get()
	suite.VerifyNSKVListNonTLS(kvMuxState.kvServerList)
	suite.VerifyNSMgmtListNonTLS(httpMuxState.mgmtEpList)

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurity", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurity")

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: true,
		TLSRootCAProvider: func() *x509.CertPool {
			return nil
		},
	})
	suite.Require().Nil(err, err)

	success = suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		kvMuxState := agent.kvMux.getState()
		kvEps := kvMuxState.kvServerList

		for _, ep := range kvEps {
			epParts := strings.Split(ep.Address, "://")
			hostport := strings.Split(epParts[1], ":")
			if hostport[0] != splitSeed[0] && hostport[1] == "8091" {
				suite.T().Logf("Unencrypted endpoint found: %s", ep.Address)
				return false
			}
		}

		return true
	})
	suite.Require().True(success, "One or more endpoints never switched to TLS")

	kvMuxState = agent.kvMux.getState()
	httpMuxState = agent.httpMux.Get()
	suite.VerifyNSKVListTLS(splitSeed[0], kvMuxState.kvServerList)
	suite.VerifyNSMgmtListTLS(splitSeed[0], httpMuxState.mgmtEpList)

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurity", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurity")
}

func (suite *StandardTestSuite) TestReconfigureSecurityChangeAuthMechanisms() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	// This will create a config with TLS enabled
	config, _ := suite.CreateNSAgentConfig()

	agent, err := CreateAgent(config)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityChangeAuthProvider", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityChangeAuthProvider")

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		AuthMechanisms: []AuthMechanism{PlainAuthMechanism},
	})
	suite.Require().Nil(err, err)

	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitUntilReady failed with error: %v", err)
			}
		})
	}))
	s.Wait(6)

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityChangeAuthMechanisms", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityChangeAuthProvider")
}

func (suite *StandardTestSuite) TestReconfigureSecurityChangeAuthProvider() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	// This will create a config with TLS enabled
	config, _ := suite.CreateNSAgentConfig()
	// Reduce this otherwise our forced auth errors are going to cause bootstrap backoff of 5s.
	config.KVConfig.ServerWaitBackoff = 500 * time.Millisecond

	agent, err := CreateAgent(config)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityChangeAuthProvider", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityChangeAuthProvider")

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: false,
		Auth: PasswordAuthProvider{
			Username: "",
			Password: "",
		},
	})
	suite.Require().Nil(err, err)

	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrAuthenticationFailure) {
				s.Fatalf("WaitUntilReady should have failed with auth error but was: %v", err)
			}
		})
	}))
	s.Wait(6)

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: false,
		Auth:   globalTestConfig.Authenticator,
	})
	suite.Require().Nil(err, err)

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityChangeAuthProvider", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityChangeAuthProvider")

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: true,
		TLSRootCAProvider: func() *x509.CertPool {
			return nil
		},
		Auth: globalTestConfig.Authenticator,
	})
	suite.Require().Nil(err, err)

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityChangeAuthProvider", suite.CollectionName, suite.ScopeName)
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityChangeAuthProvider")
}

func (suite *StandardTestSuite) TestReconfigureSecurityNotNSServer() {
	agent := suite.DefaultAgent()

	err := agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: false,
	})
	suite.Require().NotNil(err, err)
}

func (suite *StandardTestSuite) TestReconfigureSecurityTLSNoProvider() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	// This will create a config with TLS enabled
	config, _ := suite.CreateNSAgentConfig()
	// Reduce this otherwise our forced auth errors are going to cause bootstrap backoff of 5s.
	config.KVConfig.ServerWaitBackoff = 500 * time.Millisecond

	agent, err := CreateAgent(config)
	suite.Require().Nil(err, err)
	defer agent.Close()

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: true,
	})
	suite.Require().NotNil(err, err)
}

func (suite *StandardTestSuite) TestReconfigureSecurityMemd() {
	suite.EnsureSupportsFeature(TestFeatureSsl)
	suite.EnsureSupportsFeature(TestFeatureMemd)

	// This will create a config with TLS enabled
	config, seedAddr := suite.CreateNSAgentConfig()
	splitSeed := strings.Split(seedAddr, ":")
	config.BucketName = globalTestConfig.MemdBucketName

	agent, err := CreateAgent(config)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityMemd", "", "")
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityMemd")

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: false,
	})
	suite.Require().Nil(err, err)

	success := suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		kvMuxState := agent.kvMux.getState()
		kvEps := kvMuxState.kvServerList

		for _, ep := range kvEps {
			epParts := strings.Split(ep.Address, "://")
			hostport := strings.Split(epParts[1], ":")
			if hostport[1] == "11207" {
				suite.T().Logf("Encrypted endpoint found: %s", ep.Address)
				return false
			}
		}

		return true
	})
	suite.Require().True(success, "One or more endpoints never switched to nonTLS")

	kvMuxState := agent.kvMux.getState()
	httpMuxState := agent.httpMux.Get()
	suite.VerifyNSKVListNonTLS(kvMuxState.kvServerList)
	suite.VerifyNSMgmtListNonTLS(httpMuxState.mgmtEpList)

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityMemd", "", "")
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityMemd")

	err = agent.ReconfigureSecurity(ReconfigureSecurityOptions{
		UseTLS: true,
		TLSRootCAProvider: func() *x509.CertPool {
			return nil
		},
	})
	suite.Require().Nil(err, err)

	success = suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		kvMuxState := agent.kvMux.getState()
		kvEps := kvMuxState.kvServerList

		for _, ep := range kvEps {
			epParts := strings.Split(ep.Address, "://")
			hostport := strings.Split(epParts[1], ":")
			if hostport[0] != splitSeed[0] && hostport[1] == "8091" {
				suite.T().Logf("Unencrypted endpoint found: %s", ep.Address)
				return false
			}
		}

		return true
	})
	suite.Require().True(success, "One or more endpoints never switched to TLS")

	kvMuxState = agent.kvMux.getState()
	httpMuxState = agent.httpMux.Get()
	suite.VerifyNSKVListTLS(splitSeed[0], kvMuxState.kvServerList)
	suite.VerifyNSMgmtListTLS(splitSeed[0], httpMuxState.mgmtEpList)

	suite.VerifyConnectedToBucket(agent, s, "TestReconfigureSecurityMemd", "", "")
	suite.VerifyConnectedToBucketHTTP(agent, globalTestConfig.BucketName, s, "TestReconfigureSecurityMemd")
}
