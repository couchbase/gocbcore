package gocbcore

import (
	"context"
	"time"
)

func (suite *ColumnarTestSuite) TestBasicQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := suite.agent.Query(ctx, ColumnarQueryOptions{
		Payload: map[string]interface{}{
			"statement": "FROM RANGE(0, 99) AS i SELECT *",
		},
	})
	suite.Require().NoError(err)

	var rows [][]byte
	for row := res.NextRow(); row != nil; row = res.NextRow() {
		rows = append(rows, row)
	}

	suite.Assert().Len(rows, 100)

	err = res.Err()
	suite.Require().NoError(err)

	_, err = res.MetaData()
	suite.Require().NoError(err)

	err = res.Close()
	suite.Require().NoError(err)
}

func (suite *ColumnarTestSuite) TestDispatchTimeout() {
	// This test purposefully triggers error cases.
	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	cfg := suite.makeAgentConfig(suite.ColumnarTestConfig)
	cfg.DispatchTimeout = 1 * time.Second
	cfg.SeedConfig.SRVRecord = nil
	cfg.SeedConfig.MemdAddrs = []string{"couchbases://utternonsense"}

	agent, err := CreateColumnarAgent(&cfg)
	suite.Require().NoError(err)

	defer agent.Close()

	_, err = agent.Query(context.Background(), ColumnarQueryOptions{
		Payload: map[string]interface{}{
			"statement": "FROM RANGE(0, 99) AS i SELECT *",
		},
	})
	suite.Require().ErrorIs(err, ErrTimeout)

	var columnarError *ColumnarError
	suite.Require().ErrorAs(err, &columnarError)

	suite.Assert().NotEmpty(columnarError.Statement)
}
