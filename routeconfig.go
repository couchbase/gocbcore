package gocbcore

import "fmt"

type routeConfig struct {
	revID        int64
	uuid         string
	name         string
	bktType      bucketType
	kvServerList []string
	capiEpList   []string
	mgmtEpList   []string
	n1qlEpList   []string
	ftsEpList    []string
	cbasEpList   []string
	vbMap        *vbucketMap
	ketamaMap    *ketamaContinuum

	clusterCapabilitiesVer []int
	clusterCapabilities    map[string][]string
}

func (config *routeConfig) DebugString() string {
	var outStr string

	outStr += fmt.Sprintf("Revision ID: %d\n", config.revID)

	outStr += "Capi Eps:\n"
	for _, ep := range config.capiEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "Mgmt Eps:\n"
	for _, ep := range config.mgmtEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "N1ql Eps:\n"
	for _, ep := range config.n1qlEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "FTS Eps:\n"
	for _, ep := range config.ftsEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "CBAS Eps:\n"
	for _, ep := range config.cbasEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	if config.vbMap != nil {
		outStr += "VBMap:\n"
		outStr += fmt.Sprintf("%+v\n", config.vbMap)
	} else {
		outStr += "VBMap: not-used\n"
	}

	if config.ketamaMap != nil {
		outStr += "KetamaMap:\n"
		outStr += fmt.Sprintf("%+v\n", config.ketamaMap)
	} else {
		outStr += "KetamaMap: not-used\n"
	}

	// outStr += "Source Data: *"
	//outStr += fmt.Sprintf("  Source Data: %v", rd.source)

	return outStr
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

func (config *routeConfig) IsGCCCPConfig() bool {
	return config.bktType == bktTypeNone
}
