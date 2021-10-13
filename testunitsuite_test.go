package gocbcore

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/suite"
)

type UnitTestSuite struct {
	suite.Suite
}

func TestUnitSuite(t *testing.T) {
	if globalTestConfig == nil {
		t.Skip()
	}

	suite.Run(t, new(UnitTestSuite))
}

func (suite *UnitTestSuite) LoadRawTestDataset(dataset string) ([]byte, error) {
	return ioutil.ReadFile("testdata/" + dataset + ".json")
}

func loadRawTestDataset(dataset string) ([]byte, error) {
	return ioutil.ReadFile("testdata/" + dataset + ".json")
}
