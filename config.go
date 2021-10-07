package gocbcore

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
)

// A Node is a computer in a cluster running the couchbase software.
type cfgNode struct {
	ClusterCompatibility int                `json:"clusterCompatibility"`
	ClusterMembership    string             `json:"clusterMembership"`
	CouchAPIBase         string             `json:"couchApiBase"`
	Hostname             string             `json:"hostname"`
	InterestingStats     map[string]float64 `json:"interestingStats,omitempty"`
	MCDMemoryAllocated   float64            `json:"mcdMemoryAllocated"`
	MCDMemoryReserved    float64            `json:"mcdMemoryReserved"`
	MemoryFree           float64            `json:"memoryFree"`
	MemoryTotal          float64            `json:"memoryTotal"`
	OS                   string             `json:"os"`
	Ports                map[string]int     `json:"ports"`
	Status               string             `json:"status"`
	Uptime               int                `json:"uptime,string"`
	Version              string             `json:"version"`
	ThisNode             bool               `json:"thisNode,omitempty"`
}

type cfgNodeServices struct {
	Kv          uint16 `json:"kv"`
	Capi        uint16 `json:"capi"`
	Mgmt        uint16 `json:"mgmt"`
	N1ql        uint16 `json:"n1ql"`
	Fts         uint16 `json:"fts"`
	Cbas        uint16 `json:"cbas"`
	Eventing    uint16 `json:"eventingAdminPort"`
	GSI         uint16 `json:"indexHttp"`
	Backup      uint16 `json:"backupAPI"`
	KvSsl       uint16 `json:"kvSSL"`
	CapiSsl     uint16 `json:"capiSSL"`
	MgmtSsl     uint16 `json:"mgmtSSL"`
	N1qlSsl     uint16 `json:"n1qlSSL"`
	FtsSsl      uint16 `json:"ftsSSL"`
	CbasSsl     uint16 `json:"cbasSSL"`
	EventingSsl uint16 `json:"eventingSSL"`
	GSISsl      uint16 `json:"indexHttps"`
	BackupSsl   uint16 `json:"backupAPIHTTPS"`
}

type cfgNodeAltAddress struct {
	Ports    *cfgNodeServices `json:"ports,omitempty"`
	Hostname string           `json:"hostname"`
}

type cfgNodeExt struct {
	Services     cfgNodeServices              `json:"services"`
	Hostname     string                       `json:"hostname"`
	ThisNode     bool                         `json:"thisNode"`
	AltAddresses map[string]cfgNodeAltAddress `json:"alternateAddresses"`
}

// VBucketServerMap is the a mapping of vbuckets to nodes.
type cfgVBucketServerMap struct {
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
	VBucketMap    [][]int  `json:"vBucketMap"`
}

// Bucket is the primary entry point for most data operations.
type cfgBucket struct {
	Rev                 int64 `json:"rev"`
	RevEpoch            int64 `json:"revEpoch"`
	SourceHostname      string
	Capabilities        []string `json:"bucketCapabilities"`
	CapabilitiesVersion string   `json:"bucketCapabilitiesVer"`
	Name                string   `json:"name"`
	NodeLocator         string   `json:"nodeLocator"`
	URI                 string   `json:"uri"`
	StreamingURI        string   `json:"streamingUri"`
	UUID                string   `json:"uuid"`
	DDocs               struct {
		URI string `json:"uri"`
	} `json:"ddocs,omitempty"`

	// These are used for JSON IO, but isn't used for processing
	// since it needs to be swapped out safely.
	VBucketServerMap       cfgVBucketServerMap `json:"vBucketServerMap"`
	Nodes                  []cfgNode           `json:"nodes"`
	NodesExt               []cfgNodeExt        `json:"nodesExt,omitempty"`
	ClusterCapabilitiesVer []int               `json:"clusterCapabilitiesVer,omitempty"`
	ClusterCapabilities    map[string][]string `json:"clusterCapabilities,omitempty"`
}

func (cfg *cfgBucket) BuildRouteConfig(useSsl bool, networkType string, firstConnect bool, noSourceSSL bool) *routeConfig {
	var (
		kvServerList   []routeEndpoint
		capiEpList     []routeEndpoint
		mgmtEpList     []routeEndpoint
		n1qlEpList     []routeEndpoint
		ftsEpList      []routeEndpoint
		cbasEpList     []routeEndpoint
		eventingEpList []routeEndpoint
		gsiEpList      []routeEndpoint
		backupEpList   []routeEndpoint
		bktType        bucketType
	)

	switch cfg.NodeLocator {
	case "ketama":
		bktType = bktTypeMemcached
	case "vbucket":
		bktType = bktTypeCouchbase
	default:
		if cfg.UUID == "" {
			bktType = bktTypeNone
		} else {
			logDebugf("Invalid nodeLocator %s", cfg.NodeLocator)
			bktType = bktTypeInvalid
		}
	}

	if cfg.NodesExt != nil {
		lenNodes := len(cfg.Nodes)
		for i, node := range cfg.NodesExt {
			hostname := node.Hostname
			ports := node.Services

			if networkType != "default" {
				if altAddr, ok := node.AltAddresses[networkType]; ok {
					hostname = altAddr.Hostname
					if altAddr.Ports != nil {
						ports = *altAddr.Ports
					}
				} else {
					if !firstConnect {
						logDebugf("Invalid config network type %s", networkType)
					}
					continue
				}
			}

			useSSLThisNode := useSsl
			if noSourceSSL && (node.ThisNode || len(node.Hostname) == 0) {
				useSSLThisNode = false
			}
			hostname = getHostname(hostname, cfg.SourceHostname)

			endpoints := endpointsFromPorts(useSSLThisNode, ports, hostname)
			if endpoints.kvServer.Address != "" {
				if bktType > bktTypeInvalid && i >= lenNodes {
					logDebugf("KV node present in nodesext but not in nodes for %s", endpoints.kvServer)
				} else {
					kvServerList = append(kvServerList, endpoints.kvServer)
				}
			}
			if endpoints.capiEp.Address != "" {
				capiEpList = append(capiEpList, endpoints.capiEp)
			}
			if endpoints.mgmtEp.Address != "" {
				mgmtEpList = append(mgmtEpList, endpoints.mgmtEp)
			}
			if endpoints.n1qlEp.Address != "" {
				n1qlEpList = append(n1qlEpList, endpoints.n1qlEp)
			}
			if endpoints.ftsEp.Address != "" {
				ftsEpList = append(ftsEpList, endpoints.ftsEp)
			}
			if endpoints.cbasEp.Address != "" {
				cbasEpList = append(cbasEpList, endpoints.cbasEp)
			}
			if endpoints.eventingEp.Address != "" {
				eventingEpList = append(eventingEpList, endpoints.eventingEp)
			}
			if endpoints.gsiEp.Address != "" {
				gsiEpList = append(gsiEpList, endpoints.gsiEp)
			}
			if endpoints.backupEp.Address != "" {
				backupEpList = append(backupEpList, endpoints.backupEp)
			}
		}
	} else {
		if useSsl {
			logErrorf("Received config without nodesExt while SSL is enabled.  Generating invalid config.")
			return &routeConfig{}
		}

		if bktType == bktTypeCouchbase {
			for _, s := range cfg.VBucketServerMap.ServerList {
				kvServerList = append(kvServerList, routeEndpoint{
					Address: s,
				})
			}
		}

		for _, node := range cfg.Nodes {
			if node.CouchAPIBase != "" {
				// Slice off the UUID as Go's HTTP client cannot handle being passed URL-Encoded path values.
				capiEp := strings.SplitN(node.CouchAPIBase, "%2B", 2)[0]

				capiEpList = append(capiEpList, routeEndpoint{
					Address: capiEp,
				})
			}
			if node.Hostname != "" {
				mgmtEpList = append(mgmtEpList, routeEndpoint{
					Address: fmt.Sprintf("http://%s", node.Hostname),
				})
			}

			if bktType == bktTypeMemcached {
				// Get the data port. No VBucketServerMap.
				host, err := hostFromHostPort(node.Hostname)
				if err != nil {
					logErrorf("Encountered invalid memcached host/port string. Ignoring node.")
					continue
				}

				curKvHost := fmt.Sprintf("%s:%d", host, node.Ports["direct"])
				kvServerList = append(kvServerList, routeEndpoint{
					Address: curKvHost,
				})
			}
		}
	}

	rc := &routeConfig{
		revID:                  cfg.Rev,
		revEpoch:               cfg.RevEpoch,
		uuid:                   cfg.UUID,
		name:                   cfg.Name,
		kvServerList:           kvServerList,
		capiEpList:             capiEpList,
		mgmtEpList:             mgmtEpList,
		n1qlEpList:             n1qlEpList,
		ftsEpList:              ftsEpList,
		cbasEpList:             cbasEpList,
		eventingEpList:         eventingEpList,
		gsiEpList:              gsiEpList,
		backupEpList:           backupEpList,
		bktType:                bktType,
		clusterCapabilities:    cfg.ClusterCapabilities,
		clusterCapabilitiesVer: cfg.ClusterCapabilitiesVer,
		bucketCapabilities:     cfg.Capabilities,
		bucketCapabilitiesVer:  cfg.CapabilitiesVersion,
	}

	if bktType == bktTypeCouchbase {
		vbMap := cfg.VBucketServerMap.VBucketMap
		numReplicas := cfg.VBucketServerMap.NumReplicas
		rc.vbMap = newVbucketMap(vbMap, numReplicas)
	} else if bktType == bktTypeMemcached {
		rc.ketamaMap = newKetamaContinuum(kvServerList)
	}

	return rc
}

type serverEps struct {
	kvServer   routeEndpoint
	capiEp     routeEndpoint
	mgmtEp     routeEndpoint
	n1qlEp     routeEndpoint
	ftsEp      routeEndpoint
	cbasEp     routeEndpoint
	eventingEp routeEndpoint
	gsiEp      routeEndpoint
	backupEp   routeEndpoint
}

func getHostname(hostname, sourceHostname string) string {
	// Hostname blank means to use the same one as was connected to
	if hostname == "" {
		// Note that the SourceHostname will already be IPv6 wrapped
		hostname = sourceHostname
	} else {
		// We need to detect an IPv6 address here and wrap it in the appropriate
		// [] block to indicate its IPv6 for the rest of the system.
		if strings.Contains(hostname, ":") {
			hostname = "[" + hostname + "]"
		}
	}

	return hostname
}

func endpointsFromPorts(useSsl bool, ports cfgNodeServices, hostname string) *serverEps {
	lists := &serverEps{}

	if useSsl {
		if ports.KvSsl > 0 {
			lists.kvServer = routeEndpoint{
				Address:   fmt.Sprintf("couchbases://%s:%d", hostname, ports.KvSsl),
				Encrypted: true,
			}
		}
		if ports.CapiSsl > 0 {
			lists.capiEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.CapiSsl),
				Encrypted: true,
			}
		}
		if ports.MgmtSsl > 0 {
			lists.mgmtEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.MgmtSsl),
				Encrypted: true,
			}
		}
		if ports.N1qlSsl > 0 {
			lists.n1qlEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.N1qlSsl),
				Encrypted: true,
			}
		}
		if ports.FtsSsl > 0 {
			lists.ftsEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.FtsSsl),
				Encrypted: true,
			}
		}
		if ports.CbasSsl > 0 {
			lists.cbasEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.CbasSsl),
				Encrypted: true,
			}
		}
		if ports.EventingSsl > 0 {
			lists.eventingEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.EventingSsl),
				Encrypted: true,
			}
		}
		if ports.GSISsl > 0 {
			lists.gsiEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.GSISsl),
				Encrypted: true,
			}
		}
		if ports.BackupSsl > 0 {
			lists.backupEp = routeEndpoint{
				Address:   fmt.Sprintf("https://%s:%d", hostname, ports.BackupSsl),
				Encrypted: true,
			}
		}
	} else {
		if ports.Kv > 0 {
			lists.kvServer = routeEndpoint{
				Address:   fmt.Sprintf("couchbase://%s:%d", hostname, ports.Kv),
				Encrypted: false,
			}
		}
		if ports.Capi > 0 {
			lists.capiEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.Capi),
				Encrypted: false,
			}
		}
		if ports.Mgmt > 0 {
			lists.mgmtEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.Mgmt),
				Encrypted: false,
			}
		}
		if ports.N1ql > 0 {
			lists.n1qlEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.N1ql),
				Encrypted: false,
			}
		}
		if ports.Fts > 0 {
			lists.ftsEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.Fts),
				Encrypted: false,
			}
		}
		if ports.Cbas > 0 {
			lists.cbasEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.Cbas),
				Encrypted: false,
			}
		}
		if ports.Eventing > 0 {
			lists.eventingEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.Eventing),
				Encrypted: false,
			}
		}
		if ports.GSI > 0 {
			lists.gsiEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.GSI),
				Encrypted: false,
			}
		}
		if ports.Backup > 0 {
			lists.backupEp = routeEndpoint{
				Address:   fmt.Sprintf("http://%s:%d", hostname, ports.Backup),
				Encrypted: false,
			}
		}
	}
	return lists
}

func hostFromHostPort(hostport string) (string, error) {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", err
	}

	// If this is an IPv6 address, we need to rewrap it in []
	if strings.Contains(host, ":") {
		return "[" + host + "]", nil
	}

	return host, nil
}

func parseConfig(config []byte, srcHost string) (*cfgBucket, error) {
	configStr := strings.Replace(string(config), "$HOST", srcHost, -1)

	bk := new(cfgBucket)
	err := json.Unmarshal([]byte(configStr), bk)
	if err != nil {
		return nil, err
	}

	bk.SourceHostname = srcHost
	return bk, nil
}
