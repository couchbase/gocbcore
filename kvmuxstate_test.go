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
		BucketCapabilityBinaryXattr:          CapabilityStatusUnknown,
		BucketCapabilitySubdocAccessDeleted:  CapabilityStatusUnknown,
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
		BucketCapabilityBinaryXattr:          CapabilityStatusUnknown,
		BucketCapabilitySubdocAccessDeleted:  CapabilityStatusUnknown,
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
		BucketCapabilityBinaryXattr:          CapabilityStatusUnsupported,
		BucketCapabilitySubdocAccessDeleted:  CapabilityStatusUnsupported,
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
		BucketCapabilityBinaryXattr:          CapabilityStatusUnsupported,
		BucketCapabilitySubdocAccessDeleted:  CapabilityStatusUnsupported,
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
		BucketCapabilityBinaryXattr:          CapabilityStatusUnsupported,
		BucketCapabilitySubdocAccessDeleted:  CapabilityStatusUnsupported,
	}, muxState.bucketCapabilities)
}

func (suite *UnitTestSuite) TestKvMuxState_BucketCapabilitiesSupported() {
	cfg := &routeConfig{
		revID: 1,
		name:  "default",
		bucketCapabilities: []string{"durableWrite", "tombstonedUserXAttrs", "rangeScan", "subdoc.ReplicaRead",
			"subdoc.ReplaceBodyWithXattr", "subdoc.ReviveDocument", "nonDedupedHistory", "subdoc.BinaryXattr", "subdoc.AccessDeleted"},
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
		BucketCapabilityBinaryXattr:          CapabilityStatusSupported,
		BucketCapabilitySubdocAccessDeleted:  CapabilityStatusSupported,
	}, muxState.bucketCapabilities)
}
