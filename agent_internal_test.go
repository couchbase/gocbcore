package gocbcore

func (suite *StandardTestSuite) TestInternalBucketCapabilityStatus() {
	internal := suite.DefaultAgent().Internal()

	agent := suite.DefaultAgent()
	state := agent.kvMux.getState()

	suite.Assert().Equal(state.bucketCapabilities[BucketCapabilityReplaceBodyWithXattr], internal.BucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr))
	suite.Assert().Equal(state.bucketCapabilities[BucketCapabilityCreateAsDeleted], internal.BucketCapabilityStatus(BucketCapabilityCreateAsDeleted))
	suite.Assert().Equal(state.bucketCapabilities[BucketCapabilityDurableWrites], internal.BucketCapabilityStatus(BucketCapabilityDurableWrites))
}
