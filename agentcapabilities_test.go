package gocbcore

import "testing"

func TestNoClusterCapabilities(t *testing.T) {
	cfg := loadConfigFromFile(t, "testdata/full_25.json")

	capsMgr := newClusterCapabilitiesManager(&configManager{})
	capsMgr.OnNewRouteConfig(cfg.BuildRouteConfig(false, "default", false))
	capabilities := capsMgr.clusterCapabilities
	if capabilities != 0 {
		t.Fatalf("Expected no capabilities to be returned but was %v", capabilities)
	}
}

func TestClusterCapabilitiesEnhancedPreparedStatements(t *testing.T) {
	cfg := loadConfigFromFile(t, "testdata/full_65.json")

	capsMgr := newClusterCapabilitiesManager(&configManager{})
	capsMgr.OnNewRouteConfig(cfg.BuildRouteConfig(false, "default", false))

	if !capsMgr.SupportsClusterCapability(ClusterCapabilityEnhancedPreparedStatements) {
		t.Fatalf("Expected agent to support enhanced prepared statements")
	}
}
