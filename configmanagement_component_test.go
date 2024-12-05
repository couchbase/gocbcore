package gocbcore

import (
	"encoding/json"
	"fmt"
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
				useSSL:            false,
				networkType:       "default",
				cfgChangeWatchers: []routeConfigWatcher{watcher},
				currentConfig:     oldCfg.BuildRouteConfig(false, "default", false, nil),
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

type testAlternateAddressesRouteConfigMgr struct {
	cfg       *routeConfig
	cfgCalled bool
}

func (taa *testAlternateAddressesRouteConfigMgr) OnNewRouteConfig(cfg *routeConfig) {
	taa.cfgCalled = true
	taa.cfg = cfg
}

func (suite *StandardTestSuite) TestAlternateAddressesEmptyStringConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesAutoConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "auto",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesAutoInternalConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "auto",
		SrcMemdAddrs: []routeEndpoint{{Address: "172.17.0.4:11210"}},
		UseTLS:       false,
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		suite.T().Fatalf("Expected agent networkType to be default, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesDefaultConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "default",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		suite.T().Fatalf("Expected agent networkType to be default, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesExternalConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "external",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesExternalConfigNoPorts() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses_without_ports.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "external",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesInvalidConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "invalid",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "invalid" {
		suite.T().Fatalf("Expected agent networkType to be invalid, was %s", networkType)
	}

	if mgr.cfgCalled {
		suite.T().Fatalf("Expected route config to not be propagated, was propagated")
	}
}
