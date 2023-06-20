package gocbcore

func (suite *StandardTestSuite) TestKvMuxState_BucketCapabilities_InitialConfig() {
	cfg := &routeConfig{
		revID: -1,
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, nil, nil)

	suite.Assert().Equal(map[BucketCapability]BucketCapabilityStatus{
		BucketCapabilityDurableWrites:        BucketCapabilityStatusUnknown,
		BucketCapabilityCreateAsDeleted:      BucketCapabilityStatusUnknown,
		BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusUnknown,
		BucketCapabilityRangeScan:            BucketCapabilityStatusUnknown,
		BucketCapabilityReplicaRead:          BucketCapabilityStatusUnknown,
	}, muxState.bucketCapabilities)
}

func (suite *StandardTestSuite) TestKvMuxState_BucketCapabilities() {
	cfg := &routeConfig{
		revID:              1,
		bucketCapabilities: []string{"durableWrite"},
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, nil, nil)

	suite.Assert().Equal(map[BucketCapability]BucketCapabilityStatus{
		BucketCapabilityDurableWrites:        BucketCapabilityStatusSupported,
		BucketCapabilityCreateAsDeleted:      BucketCapabilityStatusUnsupported,
		BucketCapabilityReplaceBodyWithXattr: BucketCapabilityStatusUnsupported,
		BucketCapabilityRangeScan:            BucketCapabilityStatusUnsupported,
		BucketCapabilityReplicaRead:          BucketCapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}
