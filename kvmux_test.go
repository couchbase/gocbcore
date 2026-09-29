package gocbcore

func (suite *UnitTestSuite) TestKvMux_HasBucketCapabilityStatusNoState() {
	// No mux state, shouldn't actually happen in practise.
	mux := kvMux{}

	suite.Assert().True(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(BucketCapabilityReplaceBodyWithXattr, CapabilityStatusUnsupported))
	suite.Assert().True(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnknown))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusSupported))
	suite.Assert().False(mux.HasBucketCapabilityStatus(9999, CapabilityStatusUnsupported))
}

func (suite *UnitTestSuite) TestKvMux_HasBucketCapabilityStatusBlankState() {
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

func (suite *UnitTestSuite) TestKvMux_HasBucketCapabilityStatusUnsupported() {
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

func (suite *UnitTestSuite) TestKvMux_HasBucketCapabilityStatusSupported() {
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

func (suite *UnitTestSuite) TestKvMux_GetByConnIDNilClient() {
	target := &memdClient{connID: "target"}
	muxState := &kvMuxState{
		pipelines: []*memdPipeline{
			{
				clients: []*memdPipelineClient{
					{client: nil},
					{client: &memdClient{connID: "other"}},
				},
			},
			{
				clients: []*memdPipelineClient{
					{client: nil},
					{client: target},
				},
			},
		},
	}

	mux := kvMux{}
	mux.updateState(nil, muxState)

	cli, err := mux.GetByConnID("target")
	suite.Require().NoError(err)
	suite.Assert().Same(target, cli)

	cli, err = mux.GetByConnID("missing")
	suite.Assert().ErrorIs(err, errConnectionIDInvalid)
	suite.Assert().Nil(cli)
}
