package gocbcore

func (suite *StandardTestSuite) TestInternalHasBucketCapabilityStatus() {
	internal := suite.DefaultAgent().Internal()

	if suite.SupportsFeature(TestFeatureReplaceBodyWithXattr) {
		suite.Assert().True(internal.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusSupported))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnknown))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnsupported))
	} else {
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusSupported))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnknown))
		suite.Assert().True(internal.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnsupported))
	}

	if suite.SupportsFeature(TestFeatureCreateDeleted) {
		suite.Assert().True(internal.HasBucketCapabilityStatus(BucketCapabilityCreateAsDeleted, BucketCapabilityStatusSupported))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityCreateAsDeleted, BucketCapabilityStatusUnknown))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityCreateAsDeleted, BucketCapabilityStatusUnsupported))
	} else {
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityCreateAsDeleted, BucketCapabilityStatusSupported))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityCreateAsDeleted, BucketCapabilityStatusUnknown))
		suite.Assert().True(internal.HasBucketCapabilityStatus(BucketCapabilityCreateAsDeleted, BucketCapabilityStatusUnsupported))
	}

	if suite.SupportsFeature(TestFeatureEnhancedDurability) {
		suite.Assert().True(internal.HasBucketCapabilityStatus(BucketCapabilityDurableWrites, BucketCapabilityStatusSupported))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityDurableWrites, BucketCapabilityStatusUnknown))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityDurableWrites, BucketCapabilityStatusUnsupported))
	} else {
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityDurableWrites, BucketCapabilityStatusSupported))
		suite.Assert().False(internal.HasBucketCapabilityStatus(BucketCapabilityDurableWrites, BucketCapabilityStatusUnknown))
		suite.Assert().True(internal.HasBucketCapabilityStatus(BucketCapabilityDurableWrites, BucketCapabilityStatusUnsupported))
	}
}
