package gocbcore

import (
	"encoding/json"
	"fmt"
	"net/url"
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

type testRouteConfigMgr struct {
	cfg       *routeConfig
	cfgCalled bool
}

func (tcm *testRouteConfigMgr) OnNewRouteConfig(cfg *routeConfig) {
	tcm.cfgCalled = true
	tcm.cfg = cfg
}

func (suite *UnitTestSuite) TestAlternateAddressesEmptyStringConfig() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testRouteConfigMgr{}
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
		canonicalCfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, cfgBkNode.Services.Kv)
		if server.CanonicalAddress != canonicalCfgBkServer {
			suite.T().Fatalf("Expected canonical kv server to be %s but was %s", canonicalCfgBkServer, server.CanonicalAddress)
		}
		suite.Require().NotEqual(server.Address, server.CanonicalAddress)
	}
}

func (suite *UnitTestSuite) TestAlternateAddressesAutoConfig() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testRouteConfigMgr{}
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
		canonicalCfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, cfgBkNode.Services.Kv)
		if server.CanonicalAddress != canonicalCfgBkServer {
			suite.T().Fatalf("Expected canonical kv server to be %s but was %s", canonicalCfgBkServer, server.CanonicalAddress)
		}
		suite.Require().NotEqual(server.Address, server.CanonicalAddress)
	}
}

func (suite *UnitTestSuite) TestAlternateAddressesAutoInternalConfig() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testRouteConfigMgr{}
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
		canonicalCfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, cfgBkNode.Services.Kv)
		if server.CanonicalAddress != canonicalCfgBkServer {
			suite.T().Fatalf("Expected canonical kv server to be %s but was %s", canonicalCfgBkServer, server.CanonicalAddress)
		}
		suite.Require().Equal(server.Address, server.CanonicalAddress)
	}
}

func (suite *UnitTestSuite) TestAlternateAddressesDefaultConfig() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testRouteConfigMgr{}
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
		canonicalCfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, cfgBkNode.Services.Kv)
		if server.CanonicalAddress != canonicalCfgBkServer {
			suite.T().Fatalf("Expected canonical kv server to be %s but was %s", canonicalCfgBkServer, server.CanonicalAddress)
		}
		suite.Require().Equal(server.Address, server.CanonicalAddress)
	}
}

func (suite *UnitTestSuite) TestAlternateAddressesExternalConfig() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testRouteConfigMgr{}
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
		canonicalCfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, cfgBkNode.Services.Kv)
		if server.CanonicalAddress != canonicalCfgBkServer {
			suite.T().Fatalf("Expected canonical kv server to be %s but was %s", canonicalCfgBkServer, server.CanonicalAddress)
		}
		suite.Require().NotEqual(server.Address, server.CanonicalAddress)
	}
}

func (suite *UnitTestSuite) TestAlternateAddressesExternalConfigNoPorts() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses_without_ports.json")

	mgr := &testRouteConfigMgr{}
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
		canonicalCfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, cfgBkNode.Services.Kv)
		if server.CanonicalAddress != canonicalCfgBkServer {
			suite.T().Fatalf("Expected canonical kv server to be %s but was %s", canonicalCfgBkServer, server.CanonicalAddress)
		}
		suite.Require().NotEqual(server.Address, server.CanonicalAddress)
	}
}

func (suite *UnitTestSuite) TestAlternateAddressesInvalidConfig() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testRouteConfigMgr{}
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

func (suite *UnitTestSuite) TestConfigWithNodeUUIDs() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_node_uuids.json")

	mgr := &testRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "default",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.106.129:11210"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	expectedUUIDs := map[string]string{
		"192.168.106.128": "2ef565f7abff0a3f827cb2ab31dfcdc4",
		"192.168.106.129": "9a5e59ac41a1cc006c4850805e43cfe0",
		"192.168.106.130": "badcf695fc523f63bf55cc56d3a1c787",
	}

	endpointLists := []routeEndpoints{
		mgr.cfg.kvServerList,
		mgr.cfg.capiEpList,
		mgr.cfg.mgmtEpList,
		mgr.cfg.n1qlEpList,
		mgr.cfg.ftsEpList,
		mgr.cfg.cbasEpList,
		mgr.cfg.eventingEpList,
		mgr.cfg.gsiEpList,
		mgr.cfg.backupEpList,
	}

	for _, endpointList := range endpointLists {
		for _, server := range endpointList.NonSSLEndpoints {
			parsedURL, err := url.Parse(server.Address)
			suite.Require().NoError(err)
			host := parsedURL.Hostname()

			suite.Assert().Equal(expectedUUIDs[host], server.NodeUUID)
			suite.Assert().NotEmpty(server.NodeUUID)
		}

		for _, server := range endpointList.SSLEndpoints {
			parsedURL, err := url.Parse(server.Address)
			suite.Require().NoError(err)
			host := parsedURL.Hostname()

			suite.Assert().Equal(expectedUUIDs[host], server.NodeUUID)
			suite.Assert().NotEmpty(server.NodeUUID)
		}
	}
}

func (suite *UnitTestSuite) TestConfigWithAppTelemetry() {
	cfgBk := suite.loadConfigFromFile("testdata/bucket_config_with_app_telemetry.json")

	mgr := &testRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "default",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.106.129:11210"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	expectedNonSSLEndpoints := []routeEndpoint{
		{
			Address:          "ws://192.168.106.128:8091/_appTelemetry",
			ServerGroup:      "Group 1",
			NodeUUID:         "2ef565f7abff0a3f827cb2ab31dfcdc4",
			CanonicalAddress: "ws://192.168.106.128:8091/_appTelemetry",
		},
		{
			Address:          "ws://192.168.106.129:8091/_appTelemetry",
			ServerGroup:      "Group 1",
			NodeUUID:         "9a5e59ac41a1cc006c4850805e43cfe0",
			CanonicalAddress: "ws://192.168.106.129:8091/_appTelemetry",
		},
		{
			Address:          "ws://192.168.106.130:8091/_appTelemetry",
			ServerGroup:      "Group 1",
			NodeUUID:         "badcf695fc523f63bf55cc56d3a1c787",
			CanonicalAddress: "ws://192.168.106.130:8091/_appTelemetry",
		},
	}
	expectedSSLEndpoints := []routeEndpoint{
		{
			Address:          "wss://192.168.106.128:18091/_appTelemetry",
			ServerGroup:      "Group 1",
			NodeUUID:         "2ef565f7abff0a3f827cb2ab31dfcdc4",
			CanonicalAddress: "wss://192.168.106.128:18091/_appTelemetry",
		},
		{
			Address:          "wss://192.168.106.129:18091/_appTelemetry",
			ServerGroup:      "Group 1",
			NodeUUID:         "9a5e59ac41a1cc006c4850805e43cfe0",
			CanonicalAddress: "wss://192.168.106.129:18091/_appTelemetry",
		},
		{
			Address:          "wss://192.168.106.130:18091/_appTelemetry",
			ServerGroup:      "Group 1",
			NodeUUID:         "badcf695fc523f63bf55cc56d3a1c787",
			CanonicalAddress: "wss://192.168.106.130:18091/_appTelemetry",
		},
	}

	suite.Assert().ElementsMatch(expectedNonSSLEndpoints, mgr.cfg.appTelemetryEpList.NonSSLEndpoints)
	suite.Assert().ElementsMatch(expectedSSLEndpoints, mgr.cfg.appTelemetryEpList.SSLEndpoints)
}
