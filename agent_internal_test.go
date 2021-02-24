package gocbcore

func (suite *StandardTestSuite) TestInternalBucketCapabilityStatus() {
	internal := suite.DefaultAgent().Internal()

	if suite.SupportsFeature(TestFeatureReplaceBodyWithXattr) {
		suite.Assert().Equal(BucketCapabilityStatusSupported, internal.BucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr))
	} else {
		suite.Assert().Equal(BucketCapabilityStatusUnsupported, internal.BucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr))
	}

	if suite.SupportsFeature(TestFeatureCreateDeleted) {
		suite.Assert().Equal(BucketCapabilityStatusSupported, internal.BucketCapabilityStatus(BucketCapabilityCreateAsDeleted))
	} else {
		suite.Assert().Equal(BucketCapabilityStatusUnsupported, internal.BucketCapabilityStatus(BucketCapabilityCreateAsDeleted))
	}

	if suite.SupportsFeature(TestFeatureEnhancedDurability) {
		suite.Assert().Equal(BucketCapabilityStatusSupported, internal.BucketCapabilityStatus(BucketCapabilityDurableWrites))
	} else {
		suite.Assert().Equal(BucketCapabilityStatusUnsupported, internal.BucketCapabilityStatus(BucketCapabilityDurableWrites))
	}
}
