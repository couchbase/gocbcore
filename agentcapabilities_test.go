package gocbcore

import "testing"

func TestNoClusterCapabilities(t *testing.T) {
	agent, _ := testGetAgentAndHarness(t)

	cfg := loadConfigFromFile(t, "testdata/full_25.json")
	capabilities := agent.buildClusterCapabilities(cfg)
	if capabilities != 0 {
		t.Fatalf("Expected no capabilities to be returned but was %v", capabilities)
	}
}

func TestClusterCapabilitiesEnhancedPreparedStatements(t *testing.T) {
	agent, _ := testGetAgentAndHarness(t)

	cfg := loadConfigFromFile(t, "testdata/full_65.json")
	agent.updateClusterCapabilities(cfg)

	if !agent.SupportsClusterCapability(ClusterCapabilityEnhancedPreparedStatements) {
		t.Fatalf("Expected agent to support enhanced prepared statements")
	}
}
