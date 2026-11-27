package gocbcore

import (
	"fmt"
	"runtime"
	"strings"
)

func (suite *UnitTestSuite) TestTrimSchemePrefix() {
	type test struct {
		name, address string
	}

	tests := []*test{
		{
			name:    "None",
			address: "hostname:8091",
		},
		{
			name:    "HTTP",
			address: "http://hostname:8091",
		},
		{
			name:    "HTTPS",
			address: "https://hostname:8091",
		},
		{
			name:    "Couchbase",
			address: "couchbase://hostname:8091",
		},
		{
			name:    "Couchbases",
			address: "couchbases://hostname:8091",
		},
	}

	for _, test := range tests {
		suite.Run(test.name, func() {
			suite.Require().Equal(trimSchemePrefix(test.address), "hostname:8091")
		})
	}
}

func (suite *UnitTestSuite) TestRFCUserAgent() {
	coreVersion := strings.TrimPrefix(goCbCoreVersionStr, "v")

	suite.Run("with wrapper", func() {
		expected := fmt.Sprintf("gocb-columnar/1.0.0 (gocbcore/%s; %s/%s; %s)",
			coreVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

		suite.Assert().Equal(expected, rfcUserAgent("gocb-columnar/1.0.0"))
	})

	suite.Run("without wrapper", func() {
		expected := fmt.Sprintf("gocbcore/%s (%s/%s; %s)",
			coreVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

		suite.Assert().Equal(expected, rfcUserAgent(""))
	})
}
