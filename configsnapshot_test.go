package gocbcore

import "math"

func (suite *UnitTestSuite) TestConfigSnapshotCollectionsManifestUID() {
	type testCase struct {
		name        string
		manifestUID string
		expectedUID uint64
		expectedErr error
	}

	testCases := []testCase{
		{
			name:        "single_digit",
			manifestUID: "1",
			expectedUID: 1,
		},
		{
			name:        "hex_value",
			manifestUID: "1f",
			expectedUID: 31,
		},
		{
			name:        "max_uint64",
			manifestUID: "ffffffffffffffff",
			expectedUID: math.MaxUint64,
		},
		{
			name:        "missing",
			manifestUID: "",
			expectedErr: ErrUnsupportedOperation,
		},
		{
			name:        "invalid",
			manifestUID: "not-hex",
			expectedErr: ErrUnsupportedOperation,
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_rev_epoch.json")
			suite.Require().Equal("1", cfgBk.CollectionsManifestUID)
			cfgBk.CollectionsManifestUID = tc.manifestUID

			routeCfg := cfgBk.BuildRouteConfig(false, "default", false, nil)
			snapshot := ConfigSnapshot{
				state: newKVMuxState(routeCfg, nil, nil, nil, nil, cfgBk.Name, nil, nil),
			}

			uid, err := snapshot.CollectionsManifestUID()
			if tc.expectedErr != nil {
				suite.Assert().ErrorIs(err, tc.expectedErr)
				return
			}

			suite.Require().NoError(err)
			suite.Assert().Equal(tc.expectedUID, uid)
		})
	}
}

func (suite *UnitTestSuite) TestConfigSnapshotCollectionsManifestUIDNotInConfig() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses.json")
	suite.Require().Empty(cfgBk.CollectionsManifestUID)

	routeCfg := cfgBk.BuildRouteConfig(false, "default", false, nil)
	snapshot := ConfigSnapshot{
		state: newKVMuxState(routeCfg, nil, nil, nil, nil, cfgBk.Name, nil, nil),
	}

	_, err := snapshot.CollectionsManifestUID()
	suite.Assert().ErrorIs(err, ErrUnsupportedOperation)
}
