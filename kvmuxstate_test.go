package gocbcore

func (suite *StandardTestSuite) TestKvMuxState_BucketCapabilities_InitialConfigNoBucket() {
	cfg := &routeConfig{
		revID: -1,
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "", nil, nil)

	suite.Assert().Equal(map[BucketCapability]BucketCapabilityStatus{
		BucketCapabilityDurableWrites:        BucketCapabilityStatusUnknown,
		BucketCapabilityCreateAsDeleted:      BucketCapabilityStatusUnknown,
		BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusUnknown,
		BucketCapabilityRangeScan:            BucketCapabilityStatusUnknown,
		BucketCapabilityReplicaRead:          BucketCapabilityStatusUnknown,
		BucketCapabilityNonDedupedHistory:    BucketCapabilityStatusUnknown,
	}, muxState.bucketCapabilities)
}

func (suite *StandardTestSuite) TestKvMuxState_BucketCapabilities_InitialConfigBucket() {
	cfg := &routeConfig{
		revID: -1,
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "default", nil, nil)

	suite.Assert().Equal(map[BucketCapability]BucketCapabilityStatus{
		BucketCapabilityDurableWrites:        BucketCapabilityStatusUnknown,
		BucketCapabilityCreateAsDeleted:      BucketCapabilityStatusUnknown,
		BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusUnknown,
		BucketCapabilityRangeScan:            BucketCapabilityStatusUnknown,
		BucketCapabilityReplicaRead:          BucketCapabilityStatusUnknown,
		BucketCapabilityNonDedupedHistory:    BucketCapabilityStatusUnknown,
	}, muxState.bucketCapabilities)
}

func (suite *StandardTestSuite) TestNewKvMuxState_BucketCapabilitiesNoBucket() {
	cfg := &routeConfig{
		revID: 1,
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "", nil, nil)

	suite.Assert().Equal(map[BucketCapability]BucketCapabilityStatus{
		BucketCapabilityDurableWrites:        BucketCapabilityStatusUnsupported,
		BucketCapabilityCreateAsDeleted:      BucketCapabilityStatusUnsupported,
		BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusUnsupported,
		BucketCapabilityRangeScan:            BucketCapabilityStatusUnsupported,
		BucketCapabilityReplicaRead:          BucketCapabilityStatusUnsupported,
		BucketCapabilityNonDedupedHistory:    BucketCapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}

func (suite *StandardTestSuite) TestNewKvMuxState_BucketCapabilitiesBucket() {
	cfg := &routeConfig{
		revID:              1,
		bucketCapabilities: []string{"durableWrite"},
		name:               "default",
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "default", nil, nil)

	suite.Assert().Equal(map[BucketCapability]BucketCapabilityStatus{
		BucketCapabilityDurableWrites:        BucketCapabilityStatusSupported,
		BucketCapabilityCreateAsDeleted:      BucketCapabilityStatusUnsupported,
		BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusUnsupported,
		BucketCapabilityRangeScan:            BucketCapabilityStatusUnsupported,
		BucketCapabilityReplicaRead:          BucketCapabilityStatusUnsupported,
		BucketCapabilityNonDedupedHistory:    BucketCapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}
