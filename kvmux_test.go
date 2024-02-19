package gocbcore

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusNoState() {
	// No mux state, shouldn't actually happen in practise.
	mux := kvMux{}

	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnsupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnsupported))
}

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusBlankState() {
	cfg := &routeConfig{
		revID: -1,
	}
	// Mux state as if we haven't received a config yet.
	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "", nil, nil)

	mux := kvMux{}
	mux.updateState(nil, muxState)

	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnsupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnsupported))
}

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusUnsupported() {
	// Mux state as if we have received a config yet.
	muxState := &kvMuxState{
		routeCfg: routeConfig{
			revID: 1,
		},
		bucketCapabilities: map[BucketCapability]CapabilityStatus{
			BucketCapabilityReplaceBodyWithXattr: CapabilityStatusUnsupported,
		},
	}

	mux := kvMux{}
	mux.updateState(nil, muxState)

	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnsupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnsupported))
}

func (suite *StandardTestSuite) TestKvMux_HasBucketCapabilityStatusSupported() {
	// Mux state as if we have received a config yet.
	muxState := &kvMuxState{
		routeCfg: routeConfig{
			revID: 1,
		},
		bucketCapabilities: map[BucketCapability]CapabilityStatus{
			BucketCapabilityReplaceBodyWithXattr: CapabilityStatusSupported,
		},
	}

	mux := kvMux{}
	mux.updateState(nil, muxState)

	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnknown))
	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnsupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusSupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnsupported))
}
