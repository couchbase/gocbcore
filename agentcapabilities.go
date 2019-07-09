package gocbcore

import "sync/atomic"

func (agent *Agent) buildClusterCapabilities(cfg cfgObj) ClusterCapability {
	caps := cfg.ClusterCaps()
	capsVer := cfg.ClusterCapsVer()
	if capsVer == nil || len(capsVer) == 0 || caps == nil {
		return 0
	}

	var agentCapabilities ClusterCapability
	if capsVer[0] == 1 {
		for category, catCapabilities := range caps {
			switch category {
			case "n1ql":
				for _, capability := range catCapabilities {
					switch capability {
					case "enhancedPreparedStatements":
						agentCapabilities |= ClusterCapabilityEnhancedPreparedStatements
					}
				}
			}
		}
	}

	return agentCapabilities
}

func (agent *Agent) updateClusterCapabilities(cfg cfgObj) {
	agentCapabilities := agent.buildClusterCapabilities(cfg)
	if agentCapabilities == 0 {
		return
	}

	atomic.StoreUint32(&agent.clusterCapabilities, uint32(agentCapabilities))
}

// SupportsClusterCapability returns whether or not the cluster supports a given capability.
func (agent *Agent) SupportsClusterCapability(capability ClusterCapability) bool {
	capabilities := ClusterCapability(atomic.LoadUint32(&agent.clusterCapabilities))

	return capabilities&capability != 0
}
