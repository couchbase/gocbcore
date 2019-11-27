package gocbcore

type routeConfig struct {
	revID        int64
	uuid         string
	bktType      bucketType
	kvServerList []string
	capiEpList   []string
	mgmtEpList   []string
	n1qlEpList   []string
	ftsEpList    []string
	cbasEpList   []string
	vbMap        *vbucketMap
	ketamaMap    *ketamaContinuum
}

func (config *routeConfig) IsValid() bool {
	if len(config.kvServerList) == 0 || len(config.mgmtEpList) == 0 {
		return false
	}
	switch config.bktType {
	case bktTypeCouchbase:
		return config.vbMap != nil && config.vbMap.IsValid()
	case bktTypeMemcached:
		return config.ketamaMap != nil && config.ketamaMap.IsValid()
	case bktTypeNone:
		return true
	default:
		return false
	}
}

func buildRouteConfig(config cfgObj, useSsl bool, networkType string, firstConnect bool) *routeConfig {
	return config.BuildRouteConfig(useSsl, networkType, firstConnect)
}
