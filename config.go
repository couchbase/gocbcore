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
	Services         cfgNodeServices              `json:"services"`
	Hostname         string                       `json:"hostname"`
	ThisNode         bool                         `json:"thisNode"`
	AltAddresses     map[string]cfgNodeAltAddress `json:"alternateAddresses"`
	ServerGroup      string                       `json:"serverGroup"`
	NodeUUID         string                       `json:"nodeUUID"`
	AppTelemetryPath string                       `json:"appTelemetryPath"`
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
	ClusterUUID            string              `json:"clusterUUID,omitempty"`
	ClusterName            string              `json:"clusterName,omitempty"`
}

type localLoopbackAddress struct {
	LoopbackAddr string
	Identifier   string
}

// BuildRouteConfig builds a new route config from this config.
// overwriteSeedNode indicates that we should set the hostname for a node to the cfg.SourceHostname when the config has
// been sourced from that node.
func (cfg *cfgBucket) BuildRouteConfig(useSsl bool, networkType string, firstConnect bool, loopbackAddr *localLoopbackAddress) *routeConfig {
	var (
		kvServerList       = routeEndpoints{}
		capiEpList         = routeEndpoints{}
		mgmtEpList         = routeEndpoints{}
		n1qlEpList         = routeEndpoints{}
		ftsEpList          = routeEndpoints{}
		cbasEpList         = routeEndpoints{}
		eventingEpList     = routeEndpoints{}
		gsiEpList          = routeEndpoints{}
		backupEpList       = routeEndpoints{}
		appTelemetryEpList = routeEndpoints{}
		bktType            bucketType
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
			canonicalHostname := node.Hostname
			canonicalPorts := node.Services
			serverGroup := node.ServerGroup
			nodeUUID := node.NodeUUID

			hostname := canonicalHostname
			ports := canonicalPorts

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

			var isSeedNode bool
			if loopbackAddr == nil {
				hostname = getHostname(hostname, cfg.SourceHostname)
				canonicalHostname = getHostname(canonicalHostname, cfg.SourceHostname)
			} else {
				isSeedNode = fmt.Sprintf("%s:%d", node.Hostname, node.Services.Mgmt) == loopbackAddr.Identifier
				if isSeedNode {
					logDebugf("Seed node detected and set to overwrite, setting hostname to %s", loopbackAddr.LoopbackAddr)
					hostname = loopbackAddr.LoopbackAddr
					canonicalHostname = hostname
				} else {
					hostname = getHostname(hostname, cfg.SourceHostname)
					canonicalHostname = getHostname(canonicalHostname, cfg.SourceHostname)
				}
			}

			endpoints := endpointsFromPorts(ports, hostname, canonicalPorts, canonicalHostname, isSeedNode, serverGroup, nodeUUID)
			if endpoints.kvServer.Address != "" {
				if bktType > bktTypeInvalid && i >= lenNodes {
					logDebugf("KV node present in nodesext but not in nodes for %s", endpoints.kvServer.Address)
				} else {
					kvServerList.NonSSLEndpoints = append(kvServerList.NonSSLEndpoints, endpoints.kvServer)
				}
			}
			if endpoints.capiEp.Address != "" {
				capiEpList.NonSSLEndpoints = append(capiEpList.NonSSLEndpoints, endpoints.capiEp)
			}
			if endpoints.mgmtEp.Address != "" {
				mgmtEpList.NonSSLEndpoints = append(mgmtEpList.NonSSLEndpoints, endpoints.mgmtEp)
				if node.AppTelemetryPath != "" {
					ep := endpoints.mgmtEp
					ep.Address = strings.Replace(ep.Address, "http://", "ws://", 1) + node.AppTelemetryPath
					ep.CanonicalAddress = strings.Replace(ep.CanonicalAddress, "http://", "ws://", 1) + node.AppTelemetryPath
					appTelemetryEpList.NonSSLEndpoints = append(appTelemetryEpList.NonSSLEndpoints, ep)
				}
			}
			if endpoints.n1qlEp.Address != "" {
				n1qlEpList.NonSSLEndpoints = append(n1qlEpList.NonSSLEndpoints, endpoints.n1qlEp)
			}
			if endpoints.ftsEp.Address != "" {
				ftsEpList.NonSSLEndpoints = append(ftsEpList.NonSSLEndpoints, endpoints.ftsEp)
			}
			if endpoints.cbasEp.Address != "" {
				cbasEpList.NonSSLEndpoints = append(cbasEpList.NonSSLEndpoints, endpoints.cbasEp)
			}
			if endpoints.eventingEp.Address != "" {
				eventingEpList.NonSSLEndpoints = append(eventingEpList.NonSSLEndpoints, endpoints.eventingEp)
			}
			if endpoints.gsiEp.Address != "" {
				gsiEpList.NonSSLEndpoints = append(gsiEpList.NonSSLEndpoints, endpoints.gsiEp)
			}
			if endpoints.backupEp.Address != "" {
				backupEpList.NonSSLEndpoints = append(backupEpList.NonSSLEndpoints, endpoints.backupEp)
			}

			if endpoints.kvServerSSL.Address != "" {
				if bktType > bktTypeInvalid && i >= lenNodes {
					logDebugf("KV node present in nodesext but not in nodes for %s", endpoints.kvServerSSL.Address)
				} else {
					kvServerList.SSLEndpoints = append(kvServerList.SSLEndpoints, endpoints.kvServerSSL)
				}
			}
			if endpoints.capiEpSSL.Address != "" {
				capiEpList.SSLEndpoints = append(capiEpList.SSLEndpoints, endpoints.capiEpSSL)
			}
			if endpoints.mgmtEpSSL.Address != "" {
				mgmtEpList.SSLEndpoints = append(mgmtEpList.SSLEndpoints, endpoints.mgmtEpSSL)
				if node.AppTelemetryPath != "" {
					ep := endpoints.mgmtEpSSL
					ep.Address = strings.Replace(ep.Address, "https://", "wss://", 1) + node.AppTelemetryPath
					ep.CanonicalAddress = strings.Replace(ep.CanonicalAddress, "https://", "wss://", 1) + node.AppTelemetryPath
					appTelemetryEpList.SSLEndpoints = append(appTelemetryEpList.SSLEndpoints, ep)
				}
			}
			if endpoints.n1qlEpSSL.Address != "" {
				n1qlEpList.SSLEndpoints = append(n1qlEpList.SSLEndpoints, endpoints.n1qlEpSSL)
			}
			if endpoints.ftsEpSSL.Address != "" {
				ftsEpList.SSLEndpoints = append(ftsEpList.SSLEndpoints, endpoints.ftsEpSSL)
			}
			if endpoints.cbasEpSSL.Address != "" {
				cbasEpList.SSLEndpoints = append(cbasEpList.SSLEndpoints, endpoints.cbasEpSSL)
			}
			if endpoints.eventingEpSSL.Address != "" {
				eventingEpList.SSLEndpoints = append(eventingEpList.SSLEndpoints, endpoints.eventingEpSSL)
			}
			if endpoints.gsiEpSSL.Address != "" {
				gsiEpList.SSLEndpoints = append(gsiEpList.SSLEndpoints, endpoints.gsiEpSSL)
			}
			if endpoints.backupEpSSL.Address != "" {
				backupEpList.SSLEndpoints = append(backupEpList.SSLEndpoints, endpoints.backupEpSSL)
			}
		}
	} else {
		if useSsl {
			logErrorf("Received config without nodesExt while SSL is enabled.  Generating invalid config.")
			return &routeConfig{}
		}

		if bktType == bktTypeCouchbase {
			for _, s := range cfg.VBucketServerMap.ServerList {
				kvServerList.NonSSLEndpoints = append(kvServerList.NonSSLEndpoints, routeEndpoint{
					Address: s,
				})
			}
		}

		for _, node := range cfg.Nodes {
			if node.CouchAPIBase != "" {
				// Slice off the UUID as Go's HTTP client cannot handle being passed URL-Encoded path values.
				capiEp := strings.SplitN(node.CouchAPIBase, "%2B", 2)[0]

				capiEpList.NonSSLEndpoints = append(capiEpList.NonSSLEndpoints, routeEndpoint{
					Address: capiEp,
				})
			}
			if node.Hostname != "" {
				mgmtEpList.NonSSLEndpoints = append(mgmtEpList.NonSSLEndpoints, routeEndpoint{
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
				kvServerList.NonSSLEndpoints = append(kvServerList.NonSSLEndpoints, routeEndpoint{
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
		appTelemetryEpList:     appTelemetryEpList,
		bktType:                bktType,
		clusterCapabilities:    cfg.ClusterCapabilities,
		clusterCapabilitiesVer: cfg.ClusterCapabilitiesVer,
		bucketCapabilities:     cfg.Capabilities,
		bucketCapabilitiesVer:  cfg.CapabilitiesVersion,
		clusterUUID:            cfg.ClusterUUID,
		clusterName:            cfg.ClusterName,
	}

	if bktType == bktTypeCouchbase {
		vbMap := cfg.VBucketServerMap.VBucketMap
		numReplicas := cfg.VBucketServerMap.NumReplicas
		rc.vbMap = newVbucketMap(vbMap, numReplicas)
	} else if bktType == bktTypeMemcached {
		var endpoints []routeEndpoint
		if useSsl {
			endpoints = kvServerList.SSLEndpoints
		} else {
			endpoints = kvServerList.NonSSLEndpoints
		}
		rc.ketamaMap = newKetamaContinuum(endpoints)
	}

	return rc
}

type serverEps struct {
	kvServerSSL   routeEndpoint
	capiEpSSL     routeEndpoint
	mgmtEpSSL     routeEndpoint
	n1qlEpSSL     routeEndpoint
	ftsEpSSL      routeEndpoint
	cbasEpSSL     routeEndpoint
	eventingEpSSL routeEndpoint
	gsiEpSSL      routeEndpoint
	backupEpSSL   routeEndpoint
	kvServer      routeEndpoint
	capiEp        routeEndpoint
	mgmtEp        routeEndpoint
	n1qlEp        routeEndpoint
	ftsEp         routeEndpoint
	cbasEp        routeEndpoint
	eventingEp    routeEndpoint
	gsiEp         routeEndpoint
	backupEp      routeEndpoint
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

func endpointsFromPorts(ports cfgNodeServices, hostname string, canonicalPorts cfgNodeServices, canonicalHostname string,
	isSeedNode bool, serverGroup string, nodeUUID string) *serverEps {
	lists := &serverEps{}

	if ports.KvSsl > 0 {
		lists.kvServerSSL = routeEndpoint{
			Address:          fmt.Sprintf("couchbases://%s:%d", hostname, ports.KvSsl),
			CanonicalAddress: fmt.Sprintf("couchbases://%s:%d", canonicalHostname, canonicalPorts.KvSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.CapiSsl > 0 {
		lists.capiEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.CapiSsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.CapiSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.MgmtSsl > 0 {
		lists.mgmtEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.MgmtSsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.MgmtSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.N1qlSsl > 0 {
		lists.n1qlEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.N1qlSsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.N1qlSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.FtsSsl > 0 {
		lists.ftsEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.FtsSsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.FtsSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.CbasSsl > 0 {
		lists.cbasEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.CbasSsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.CbasSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.EventingSsl > 0 {
		lists.eventingEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.EventingSsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.EventingSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.GSISsl > 0 {
		lists.gsiEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.GSISsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.GSISsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.BackupSsl > 0 {
		lists.backupEpSSL = routeEndpoint{
			Address:          fmt.Sprintf("https://%s:%d", hostname, ports.BackupSsl),
			CanonicalAddress: fmt.Sprintf("https://%s:%d", canonicalHostname, canonicalPorts.BackupSsl),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.Kv > 0 {
		lists.kvServer = routeEndpoint{
			Address:          fmt.Sprintf("couchbase://%s:%d", hostname, ports.Kv),
			CanonicalAddress: fmt.Sprintf("couchbase://%s:%d", canonicalHostname, canonicalPorts.Kv),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.Capi > 0 {
		lists.capiEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.Capi),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.Capi),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.Mgmt > 0 {
		lists.mgmtEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.Mgmt),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.Mgmt),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.N1ql > 0 {
		lists.n1qlEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.N1ql),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.N1ql),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.Fts > 0 {
		lists.ftsEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.Fts),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.Fts),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.Cbas > 0 {
		lists.cbasEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.Cbas),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.Cbas),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.Eventing > 0 {
		lists.eventingEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.Eventing),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.Eventing),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.GSI > 0 {
		lists.gsiEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.GSI),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.GSI),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
		}
	}
	if ports.Backup > 0 {
		lists.backupEp = routeEndpoint{
			Address:          fmt.Sprintf("http://%s:%d", hostname, ports.Backup),
			CanonicalAddress: fmt.Sprintf("http://%s:%d", canonicalHostname, canonicalPorts.Backup),
			IsSeedNode:       isSeedNode,
			ServerGroup:      serverGroup,
			NodeUUID:         nodeUUID,
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
