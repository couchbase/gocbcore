package gocbcore

func (suite *UnitTestSuite) TestKvMuxState_BucketCapabilities_InitialConfigNoBucket() {
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
		BucketCapabilityReviveDocument:       CapabilityStatusUnknown,
	}, muxState.bucketCapabilities)
}

func (suite *UnitTestSuite) TestKvMuxState_BucketCapabilities_InitialConfigBucket() {
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
		BucketCapabilityReviveDocument:       CapabilityStatusUnknown,
	}, muxState.bucketCapabilities)
}

func (suite *UnitTestSuite) TestNewKvMuxState_BucketCapabilitiesNoBucket() {
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
		BucketCapabilityReviveDocument:       CapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}

func (suite *UnitTestSuite) TestNewKvMuxState_BucketCapabilitiesBucket() {
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
		BucketCapabilityReviveDocument:       CapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}

func (suite *UnitTestSuite) TestKvMuxState_BucketCapabilitiesUnsupported() {
	cfg := &routeConfig{
		revID:              1,
		bucketCapabilities: []string{},
		name:               "default",
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "default", nil, nil)

	suite.Assert().Equal(map[BucketCapability]CapabilityStatus{
		BucketCapabilityDurableWrites:        CapabilityStatusUnsupported,
		BucketCapabilityCreateAsDeleted:      CapabilityStatusUnsupported,
		BucketCapabilityReplaceBodyWithXattr: CapabilityStatusUnsupported,
		BucketCapabilityRangeScan:            CapabilityStatusUnsupported,
		BucketCapabilityReplicaRead:          CapabilityStatusUnsupported,
		BucketCapabilityNonDedupedHistory:    CapabilityStatusUnsupported,
		BucketCapabilityReviveDocument:       CapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}

func (suite *UnitTestSuite) TestKvMuxState_BucketCapabilitiesSupported() {
	cfg := &routeConfig{
		revID: 1,
		name:  "default",
		bucketCapabilities: []string{"durableWrite", "tombstonedUserXAttrs", "rangeScan", "subdoc.ReplicaRead",
			"subdoc.ReplaceBodyWithXattr", "subdoc.ReviveDocument", "nonDedupedHistory"},
	}

	muxState := newKVMuxState(cfg, nil, nil, nil, nil, "default", nil, nil)

	suite.Assert().Equal(map[BucketCapability]CapabilityStatus{
		BucketCapabilityDurableWrites:        CapabilityStatusSupported,
		BucketCapabilityCreateAsDeleted:      CapabilityStatusSupported,
		BucketCapabilityReplaceBodyWithXattr: CapabilityStatusSupported,
		BucketCapabilityRangeScan:            CapabilityStatusSupported,
		BucketCapabilityReplicaRead:          CapabilityStatusSupported,
		BucketCapabilityNonDedupedHistory:    CapabilityStatusSupported,
		BucketCapabilityReviveDocument:       CapabilityStatusSupported,
	}, muxState.bucketCapabilities)
}
