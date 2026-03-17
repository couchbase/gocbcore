package gocbcore

import "sync"

func (suite *StandardTestSuite) TestOpenBucketReturnsSameAgent() {
	config := suite.makeAgentGroupConfig(globalTestConfig)
	ag, err := suite.initAgentGroup(config)
	suite.Require().Nil(err, err)
	defer ag.Close()

	bucketName := globalTestConfig.BucketName

	err = ag.OpenBucket(bucketName)
	suite.Require().Nil(err, err)

	agent1 := ag.GetAgent(bucketName)
	suite.Require().NotNil(agent1)

	err = ag.OpenBucket(bucketName)
	suite.Require().Nil(err, err)

	agent2 := ag.GetAgent(bucketName)
	suite.Require().NotNil(agent2)

	suite.Assert().Same(agent1, agent2)
}

func (suite *StandardTestSuite) TestOpenBucketConcurrentReturnsSameAgent() {
	config := suite.makeAgentGroupConfig(globalTestConfig)
	ag, err := suite.initAgentGroup(config)
	suite.Require().Nil(err, err)
	defer ag.Close()

	bucketName := globalTestConfig.BucketName

	numGoroutines := 10
	errs := make([]error, numGoroutines)
	agents := make([]*Agent, numGoroutines)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = ag.OpenBucket(bucketName)
			agents[idx] = ag.GetAgent(bucketName)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < numGoroutines; i++ {
		suite.Require().Nil(errs[i], errs[i])
		suite.Require().NotNil(agents[i])
		suite.Assert().Same(agents[0], agents[i])
	}
}
