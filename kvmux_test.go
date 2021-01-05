package gocbcore

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusNoState() {
	// No mux state, shouldn't actually happen in practise.
	mux := kvMux{}

	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnsupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnsupported))
}

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusBlankState() {
	cfg := &routeConfig{
		revID: -1,
	}
	// Mux state as if we haven't received a config yet.
	muxState := newKVMuxState(cfg, nil, nil)

	mux := kvMux{}
	mux.updateState(nil, muxState)

	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnsupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnsupported))
}

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusUnsupported() {
	// Mux state as if we have received a config yet.
	muxState := &kvMuxState{
		revID: 1,
		bucketCapabilities: map[BucketCapability]BucketCapabilityStatus{
			BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusUnsupported,
		},
	}

	mux := kvMux{}
	mux.updateState(nil, muxState)

	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnsupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnsupported))
}

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusSupported() {
	// Mux state as if we have received a config yet.
	muxState := &kvMuxState{
		revID: 1,
		bucketCapabilities: map[BucketCapability]BucketCapabilityStatus{
			BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusSupported,
		},
	}

	mux := kvMux{}
	mux.updateState(nil, muxState)

	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnknown))
	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, BucketCapabilityStatusUnsupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, BucketCapabilityStatusUnsupported))
}
