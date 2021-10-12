package gocbcore

import (
	"encoding/json"
	"testing"
)

type testRouteWatcher struct {
	receivedConfig *routeConfig
}

func (trw *testRouteWatcher) OnNewRouteConfig(cfg *routeConfig) {
	trw.receivedConfig = cfg
}

func (suite *UnitTestSuite) TestConfigComponentRevEpoch() {
	data, err := suite.LoadRawTestDataset("bucket_config_with_rev_epoch")
	suite.Require().Nil(err)

	var cfg *cfgBucket
	suite.Require().Nil(json.Unmarshal(data, &cfg))

	type tCase struct {
		name         string
		prevRevID    int64
		prevRevEpoch int64
		newRevID     int64
		newRevEpoch  int64
		expectUpdate bool
	}

	testCases := []tCase{
		{
			name:         "no_epoch_newer_rev",
			prevRevID:    1,
			prevRevEpoch: 0,
			newRevID:     2,
			newRevEpoch:  0,
			expectUpdate: true,
		},
		{
			name:         "no_epoch_same_rev",
			prevRevID:    1,
			prevRevEpoch: 0,
			newRevID:     1,
			newRevEpoch:  0,
			expectUpdate: false,
		},
		{
			name:         "no_epoch_older_rev",
			prevRevID:    2,
			prevRevEpoch: 0,
			newRevID:     1,
			newRevEpoch:  0,
			expectUpdate: false,
		},
		{
			name:         "same_epoch_newer_rev",
			prevRevID:    1,
			prevRevEpoch: 5,
			newRevID:     2,
			newRevEpoch:  5,
			expectUpdate: true,
		},
		{
			name:         "same_epoch_same_rev",
			prevRevID:    1,
			prevRevEpoch: 5,
			newRevID:     1,
			newRevEpoch:  5,
			expectUpdate: false,
		},
		{
			name:         "same_epoch_older_rev",
			prevRevID:    2,
			prevRevEpoch: 5,
			newRevID:     1,
			newRevEpoch:  5,
			expectUpdate: false,
		},
		{
			name:         "newer_epoch_newer_rev",
			prevRevID:    1,
			prevRevEpoch: 5,
			newRevID:     2,
			newRevEpoch:  6,
			expectUpdate: true,
		},
		{
			name:         "newer_epoch_same_rev",
			prevRevID:    1,
			prevRevEpoch: 5,
			newRevID:     1,
			newRevEpoch:  6,
			expectUpdate: true,
		},
		{
			name:         "newer_epoch_older_rev",
			prevRevID:    2,
			prevRevEpoch: 5,
			newRevID:     1,
			newRevEpoch:  6,
			expectUpdate: true,
		},
		{
			name:         "older_epoch_newer_rev",
			prevRevID:    1,
			prevRevEpoch: 5,
			newRevID:     2,
			newRevEpoch:  4,
			expectUpdate: false,
		},
		{
			name:         "older_epoch_same_rev",
			prevRevID:    1,
			prevRevEpoch: 5,
			newRevID:     1,
			newRevEpoch:  4,
			expectUpdate: false,
		},
		{
			name:         "older_epoch_older_rev",
			prevRevID:    2,
			prevRevEpoch: 5,
			newRevID:     1,
			newRevEpoch:  4,
			expectUpdate: false,
		},
	}

	for _, tCase := range testCases {
		suite.T().Run(tCase.name, func(te *testing.T) {
			oldCfg := *cfg
			oldCfg.Rev = tCase.prevRevID
			oldCfg.RevEpoch = tCase.prevRevEpoch

			watcher := &testRouteWatcher{}
			cmpt := configManagementComponent{
				useSSL:            0,
				networkType:       "default",
				cfgChangeWatchers: []routeConfigWatcher{watcher},
				currentConfig:     oldCfg.BuildRouteConfig(false, "default", false),
			}

			newCfg := *cfg
			newCfg.Rev = tCase.newRevID
			newCfg.RevEpoch = tCase.newRevEpoch
			cmpt.OnNewConfig(&newCfg)

			if tCase.expectUpdate {
				if watcher.receivedConfig == nil {
					te.Fatalf("Watcher didn't receive config")
				}
			} else {
				if watcher.receivedConfig != nil {
					te.Fatalf("Watcher did receive config")
				}
			}
		})
	}
}
