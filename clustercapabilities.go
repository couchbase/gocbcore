package gocbcore

import "sync/atomic"

type clusterCapabilitiesManager struct {
	clusterCapabilities uint32
	cfgMgr              *configManager
}

func newClusterCapabilitiesManager(cfgMgr *configManager) *clusterCapabilitiesManager {
	mgr := &clusterCapabilitiesManager{
		cfgMgr: cfgMgr,
	}
	cfgMgr.AddConfigWatcher(mgr)

	return mgr
}

// SupportsClusterCapability returns whether or not the cluster supports a given capability.
func (ccm *clusterCapabilitiesManager) SupportsClusterCapability(capability ClusterCapability) bool {
	capabilities := ClusterCapability(atomic.LoadUint32(&ccm.clusterCapabilities))

	return capabilities&capability != 0
}

func (ccm *clusterCapabilitiesManager) OnNewRouteConfig(cfg *routeConfig) {
	capabilities := ccm.buildClusterCapabilities(cfg)
	if capabilities == 0 {
		return
	}

	atomic.StoreUint32(&ccm.clusterCapabilities, uint32(capabilities))
}

func (ccm *clusterCapabilitiesManager) buildClusterCapabilities(cfg *routeConfig) ClusterCapability {
	caps := cfg.clusterCapabilities
	capsVer := cfg.clusterCapabilitiesVer
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

func (ccm *clusterCapabilitiesManager) Close() {
	ccm.cfgMgr.RemoveConfigWatcher(ccm)
}
