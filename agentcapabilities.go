package gocbcore

// SupportsClusterCapability returns whether or not the cluster supports a given capability.
func (agent *Agent) SupportsClusterCapability(capability ClusterCapability) bool {
	return agent.clusterCapsMgr.SupportsClusterCapability(capability)
}
