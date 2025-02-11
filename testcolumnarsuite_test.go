package gocbcore

import (
	"crypto/x509"
	"fmt"
	"github.com/couchbase/gocbcore/v10/connstr"
	"github.com/stretchr/testify/suite"
	"net"
	"strings"
	"testing"
)

type ColumnarTestSuite struct {
	suite.Suite

	*ColumnarTestConfig
	agent *ColumnarAgent
}

func (suite *ColumnarTestSuite) SetupSuite() {
	suite.ColumnarTestConfig = globalColumnarConfig
	suite.Require().NotEmpty(suite.ColumnarTestConfig.ConnStr, "Connection string cannot be empty for testing columnar")

	suite.agent = suite.initAgent(
		suite.makeAgentConfig(suite.ColumnarTestConfig),
	)
}

func (suite *ColumnarTestSuite) TearDownSuite() {
	suite.agent.Close()
	suite.agent = nil
}

func (suite *ColumnarTestSuite) initAgent(config ColumnarAgentConfig) *ColumnarAgent {
	agent, err := CreateColumnarAgent(&config)
	suite.Require().NoError(err, err)

	return agent
}

func (suite *ColumnarTestSuite) makeAgentConfig(testConfig *ColumnarTestConfig) ColumnarAgentConfig {
	spec, err := connstr.Parse(testConfig.ConnStr)
	suite.Require().NoError(err)

	srvRecordName := spec.SrvRecordName()

	var srvRecord *SRVRecord
	var addrs []string
	if srvRecordName == "" {
		for _, addr := range spec.Addresses {
			addrs = append(addrs, fmt.Sprintf("%s:%d", addr.Host, addr.Port))
		}
	} else {
		_, srvAddrs, err := net.LookupSRV("couchbases", "tcp", spec.Addresses[0].Host)
		suite.Require().NoError(err)

		srvRecord = &SRVRecord{
			Proto:  "tcp",
			Scheme: "couchbases",
			Host:   spec.Addresses[0].Host,
		}

		for _, srvAddr := range srvAddrs {
			addrs = append(addrs, fmt.Sprintf("%s:%d", strings.TrimSuffix(srvAddr.Target, "."), srvAddr.Port))
		}
	}

	config := ColumnarAgentConfig{
		SeedConfig: ColumnarSeedConfig{
			MemdAddrs: addrs,
			SRVRecord: srvRecord,
		},
		SecurityConfig: ColumnarSecurityConfig{
			Auth: testConfig.Authenticator,
		},
	}

	if testConfig.CAProvider == nil {
		config.SecurityConfig.TLSRootCAProvider = func() *x509.CertPool {
			return nil
		}
	} else {
		config.SecurityConfig.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func TestColumnarSuite(t *testing.T) {
	if globalColumnarConfig == nil {
		t.Skip()
	}
	suite.Run(t, new(ColumnarTestSuite))
}
