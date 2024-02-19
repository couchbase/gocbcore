package gocbcore

func (suite *StandardTestSuite) TestKvMuxState_BucketCapabilities_InitialConfigNoBucket() {
	cfg := &routeConfig{
		revID: -1,
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "", nil, nil)

	suite.Assert().Equal(map[BucketCapability]CapabilityStatus{
		BucketCapabilityDurableWrites:        CapabilityStatusUnknown,
		BucketCapabilityCreateAsDeleted:      CapabilityStatusUnknown,
		BucketCapabilityReplaceBodyWithXattr: CapabilityStatusUnknown,
		BucketCapabilityRangeScan:            CapabilityStatusUnknown,
		BucketCapabilityReplicaRead:          CapabilityStatusUnknown,
		BucketCapabilityNonDedupedHistory:    CapabilityStatusUnknown,
	}, muxState.bucketCapabilities)
}

func (suite *StandardTestSuite) TestKvMuxState_BucketCapabilities_InitialConfigBucket() {
	cfg := &routeConfig{
		revID: -1,
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "default", nil, nil)

	suite.Assert().Equal(map[BucketCapability]CapabilityStatus{
		BucketCapabilityDurableWrites:        CapabilityStatusUnknown,
		BucketCapabilityCreateAsDeleted:      CapabilityStatusUnknown,
		BucketCapabilityReplaceBodyWithXattr: CapabilityStatusUnknown,
		BucketCapabilityRangeScan:            CapabilityStatusUnknown,
		BucketCapabilityReplicaRead:          CapabilityStatusUnknown,
		BucketCapabilityNonDedupedHistory:    CapabilityStatusUnknown,
	}, muxState.bucketCapabilities)
}

func (suite *StandardTestSuite) TestNewKvMuxState_BucketCapabilitiesNoBucket() {
	cfg := &routeConfig{
		revID: 1,
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "", nil, nil)

	suite.Assert().Equal(map[BucketCapability]CapabilityStatus{
		BucketCapabilityDurableWrites:        CapabilityStatusUnsupported,
		BucketCapabilityCreateAsDeleted:      CapabilityStatusUnsupported,
		BucketCapabilityReplaceBodyWithXattr: CapabilityStatusUnsupported,
		BucketCapabilityRangeScan:            CapabilityStatusUnsupported,
		BucketCapabilityReplicaRead:          CapabilityStatusUnsupported,
		BucketCapabilityNonDedupedHistory:    CapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}

func (suite *StandardTestSuite) TestNewKvMuxState_BucketCapabilitiesBucket() {
	cfg := &routeConfig{
		revID:              1,
		bucketCapabilities: []string{"durableWrite"},
		name:               "default",
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "default", nil, nil)

	suite.Assert().Equal(map[BucketCapability]CapabilityStatus{
		BucketCapabilityDurableWrites:        CapabilityStatusSupported,
		BucketCapabilityCreateAsDeleted:      CapabilityStatusUnsupported,
		BucketCapabilityReplaceBodyWithXattr: CapabilityStatusUnsupported,
		BucketCapabilityRangeScan:            CapabilityStatusUnsupported,
		BucketCapabilityReplicaRead:          CapabilityStatusUnsupported,
		BucketCapabilityNonDedupedHistory:    CapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}
