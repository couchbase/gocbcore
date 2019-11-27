package gocbcore

import (
	"crypto/tls"
	"encoding/json"
	"sort"
	"time"
)

type memdInitFunc func(*syncClient, time.Time, *Agent) error

func checkSupportsFeature(srvFeatures []HelloFeature, feature HelloFeature) bool {
	for _, srvFeature := range srvFeatures {
		if srvFeature == feature {
			return true
		}
	}
	return false
}

func (agent *Agent) dialMemdClient(address string, deadline time.Time) (*memdClient, error) {
	// Copy the tls configuration since we need to provide the hostname for each
	// server that we connect to so that the certificate can be validated properly.
	var tlsConfig *tls.Config
	if agent.tlsConfig != nil {
		host, err := hostFromHostPort(address)
		if err != nil {
			logErrorf("Failed to parse address for TLS config (%s)", err)
		}

		tlsConfig = cloneTLSConfig(agent.tlsConfig)
		tlsConfig.ServerName = host
	}

	conn, err := dialMemdConn(address, tlsConfig, deadline)
	if err != nil {
		logDebugf("Failed to connect. %v", err)
		return nil, err
	}
	client := newMemdClient(agent, conn)

	return client, err
}

func continueAfterAuth(sclient *syncClient, bucketName string, continueAuthCh chan bool, deadline time.Time) chan BytesAndError {
	if bucketName == "" {
		return nil
	}

	selectCh := make(chan BytesAndError, 1)
	go func() {
		success := <-continueAuthCh
		if !success {
			selectCh <- BytesAndError{}
			return
		}
		execCh, err := sclient.ExecSelectBucket([]byte(bucketName), deadline)
		if err != nil {
			logDebugf("Failed to execute select bucket (%v)", err)
			selectCh <- BytesAndError{Err: err}
			return
		}

		execResp := <-execCh
		selectCh <- execResp
	}()

	return selectCh
}

func findNextAuthMechanism(authMechanisms []AuthMechanism, serverAuthMechanisms []AuthMechanism) (bool, AuthMechanism, []AuthMechanism) {
	for {
		authMechanisms = authMechanisms[1:]
		if len(authMechanisms) == 0 {
			break
		}
		mech := authMechanisms[0]
		for _, serverMech := range serverAuthMechanisms {
			if mech == serverMech {
				return true, mech, authMechanisms
			}
		}
	}

	return false, "", authMechanisms
}

func (agent *Agent) storeErrorMap(mapBytes []byte, client *memdClient) {
	errMap, err := parseKvErrorMap(mapBytes)
	if err != nil {
		logDebugf("Failed to parse kv error map (%s)", err)
		return
	}

	logDebugf("Fetched error map: %+v", errMap)

	// Tell the local client to use this error map
	client.SetErrorMap(errMap)

	// Check if we need to switch the agent itself to a better
	//  error map revision.
	for {
		origMap := agent.kvErrorMap.Get()
		if origMap != nil && errMap.Revision < origMap.Revision {
			break
		}

		if agent.kvErrorMap.Update(origMap, errMap) {
			break
		}
	}
}

func (agent *Agent) bootstrap(client *memdClient, authMechanisms []AuthMechanism,
	nextAuth func(mechanism AuthMechanism), deadline time.Time) error {

	sclient := syncClient{
		client: client,
	}

	logDebugf("Fetching cluster client data")

	bucket := agent.bucket()
	features := agent.helloFeatures()
	clientInfoStr := agent.clientInfoString(client.connID)

	helloCh, err := sclient.ExecHello(clientInfoStr, features, deadline)
	if err != nil {
		logDebugf("Failed to execute HELLO (%v)", err)
		return err
	}

	errMapCh, err := sclient.ExecGetErrorMap(1, deadline)
	if err != nil {
		// GetErrorMap isn't integral to bootstrap succeeding
		logDebugf("Failed to execute get error map (%v)", err)
	}

	var listMechsCh chan SaslListMechsCompleted
	if nextAuth != nil {
		listMechsCh = make(chan SaslListMechsCompleted)
		// We only need to list mechs if there's more than 1 way to do auth.
		err = sclient.SaslListMechs(deadline, func(mechs []AuthMechanism, err error) {
			if err != nil {
				logDebugf("Failed to fetch list auth mechs (%v)", err)
			}
			listMechsCh <- SaslListMechsCompleted{
				Err:   err,
				Mechs: mechs,
			}
		})
		if err != nil {
			logDebugf("Failed to execute list auth mechs (%v)", err)
		}
	}

	var completedAuthCh chan BytesAndError
	var continueAuthCh chan bool
	if agent.authHandler != nil {
		completedAuthCh, continueAuthCh, err = agent.authHandler(&sclient, deadline)
		if err != nil {
			logDebugf("Failed to execute auth (%v)", err)
			return err
		}
	}

	var selectCh chan BytesAndError
	if continueAuthCh == nil {
		if bucket != "" {
			selectCh, err = sclient.ExecSelectBucket([]byte(bucket), deadline)
			if err != nil {
				logDebugf("Failed to execute select bucket (%v)", err)
				return err
			}
		}
	} else {
		selectCh = continueAfterAuth(&sclient, bucket, continueAuthCh, deadline)
	}

	helloResp := <-helloCh
	if helloResp.Err != nil {
		logDebugf("Failed to hello with server (%v)", helloResp.Err)
		return helloResp.Err
	}

	errMapResp := <-errMapCh
	if errMapResp.Err == nil {
		agent.storeErrorMap(errMapResp.Bytes, client)
	} else {
		logDebugf("Failed to fetch kv error map (%s)", errMapResp.Err)
	}

	var serverAuthMechanisms []AuthMechanism
	if listMechsCh != nil {
		listMechsResp := <-listMechsCh
		if listMechsResp.Err == nil {
			serverAuthMechanisms = listMechsResp.Mechs
			logDebugf("Server supported auth mechanisms: %v", serverAuthMechanisms)
		} else {
			logDebugf("Failed to fetch auth mechs from server (%v)", listMechsResp.Err)
		}
	}

	if completedAuthCh != nil {
		authResp := <-completedAuthCh
		if authResp.Err != nil {
			logDebugf("Failed to perform auth against server (%v)", authResp.Err)
			if nextAuth == nil || ErrorCause(authResp.Err) != ErrAuthError {
				return authResp.Err
			}

			for {
				var found bool
				var mech AuthMechanism
				found, mech, authMechanisms = findNextAuthMechanism(authMechanisms, serverAuthMechanisms)
				if !found {
					logDebugf("Failed to authenticate, all options exhausted")
					return authResp.Err
				}

				nextAuth(mech)
				completedAuthCh, continueAuthCh, err = agent.authHandler(&sclient, deadline)
				if err != nil {
					logDebugf("Failed to execute auth (%v)", err)
					return err
				}
				if continueAuthCh == nil {
					if bucket != "" {
						selectCh, err = sclient.ExecSelectBucket([]byte(bucket), deadline)
						if err != nil {
							logDebugf("Failed to execute select bucket (%v)", err)
							return err
						}
					}
				} else {
					selectCh = continueAfterAuth(&sclient, bucket, continueAuthCh, deadline)
				}
				authResp = <-completedAuthCh
				if authResp.Err == nil {
					break
				}

				logDebugf("Failed to perform auth against server (%v)", authResp.Err)
				if ErrorCause(authResp.Err) != ErrAuthError {
					return authResp.Err
				}
			}
		}
		logDebugf("Authenticated successfully")
	}

	if selectCh != nil {
		selectResp := <-selectCh
		if selectResp.Err != nil {
			logDebugf("Failed to perform select bucket against server (%v)", selectResp.Err)
			return selectResp.Err
		}
	}

	client.features = helloResp.SrvFeatures

	logDebugf("Client Features: %+v", features)
	logDebugf("Server Features: %+v", client.features)

	if client.SupportsFeature(FeatureCollections) {
		client.conn.EnableCollections(true)
	}

	if client.SupportsFeature(FeatureDurations) {
		client.conn.EnableFramingExtras(true)
	}

	err = agent.initFn(&sclient, deadline, agent)
	if err != nil {
		return err
	}

	return nil
}

func (agent *Agent) clientInfoString(connID string) string {
	agentName := "gocbcore/" + goCbCoreVersionStr
	if agent.userString != "" {
		agentName += " " + agent.userString
	}

	clientInfo := struct {
		Agent  string `json:"a"`
		ConnID string `json:"i"`
	}{
		Agent:  agentName,
		ConnID: connID,
	}
	clientInfoBytes, err := json.Marshal(clientInfo)
	if err != nil {
		logDebugf("Failed to generate client info string: %s", err)
	}

	return string(clientInfoBytes)
}

func (agent *Agent) helloFeatures() []HelloFeature {
	var features []HelloFeature

	// Send the TLS flag, which has unknown effects.
	features = append(features, FeatureTLS)

	// Indicate that we understand XATTRs
	features = append(features, FeatureXattr)

	// Indicates that we understand select buckets.
	features = append(features, FeatureSelectBucket)

	// If the user wants to use KV Error maps, lets enable them
	if agent.useKvErrorMaps {
		features = append(features, FeatureXerror)
	}

	// If the user wants to use mutation tokens, lets enable them
	if agent.useMutationTokens {
		features = append(features, FeatureSeqNo)
	}

	// If the user wants on-the-wire compression, lets try to enable it
	if agent.useCompression {
		features = append(features, FeatureSnappy)
	}

	if agent.useDurations {
		features = append(features, FeatureDurations)
	}

	if agent.useCollections {
		features = append(features, FeatureCollections)
	}

	// These flags are informational so don't actually enable anything
	// but the enhanced durability flag tells us if the server supports
	// the feature
	features = append(features, FeatureAltRequests)
	features = append(features, FeatureEnhancedDurability)

	return features
}

func (agent *Agent) slowDialMemdClient(address string) (*memdClient, error) {
	cached := agent.getCachedClient(address)
	if cached != nil {
		logDebugf("Returning cached client %p for %s", cached, address)
		return cached, nil
	}

	agent.serverFailuresLock.Lock()
	failureTime := agent.serverFailures[address]
	agent.serverFailuresLock.Unlock()

	if !failureTime.IsZero() {
		waitedTime := time.Since(failureTime)
		if waitedTime < agent.serverWaitTimeout {
			time.Sleep(agent.serverWaitTimeout - waitedTime)
		}
	}

	deadline := time.Now().Add(agent.serverConnectTimeout)
	client, err := agent.dialMemdClient(address, deadline)
	if err != nil {
		agent.serverFailuresLock.Lock()
		agent.serverFailures[address] = time.Now()
		agent.serverFailuresLock.Unlock()

		return nil, err
	}

	err = agent.bootstrap(client, nil, nil, deadline)
	if err != nil {
		closeErr := client.Close()
		if closeErr != nil {
			logWarnf("Failed to close authentication client (%s)", closeErr)
		}
		agent.serverFailuresLock.Lock()
		agent.serverFailures[address] = time.Now()
		agent.serverFailuresLock.Unlock()

		return nil, err
	}

	return client, nil
}

type memdQRequestSorter []*memdQRequest

func (list memdQRequestSorter) Len() int {
	return len(list)
}

func (list memdQRequestSorter) Less(i, j int) bool {
	return list[i].dispatchTime.Before(list[j].dispatchTime)
}

func (list memdQRequestSorter) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

// Accepts a cfgBucket object representing a cluster configuration and rebuilds the server list
//  along with any routing information for the Client.  Passing no config will refresh the existing one.
//  This method MUST NEVER BLOCK due to its use from various contention points.
func (agent *Agent) applyRoutingConfig(cfg *routeConfig) bool {
	// Check some basic things to ensure consistency!
	if cfg.vbMap != nil && cfg.vbMap.NumVbuckets() != agent.numVbuckets {
		logErrorf("Received a configuration with a different number of vbuckets.  Ignoring.")
		return false
	}

	// Only a single thing can modify the config at any time
	agent.configLock.Lock()
	defer agent.configLock.Unlock()

	newRouting := &routeData{
		revID:      cfg.revID,
		uuid:       cfg.uuid,
		capiEpList: cfg.capiEpList,
		mgmtEpList: cfg.mgmtEpList,
		n1qlEpList: cfg.n1qlEpList,
		ftsEpList:  cfg.ftsEpList,
		cbasEpList: cfg.cbasEpList,
		vbMap:      cfg.vbMap,
		ketamaMap:  cfg.ketamaMap,
		bktType:    cfg.bktType,
		source:     cfg,
	}

	newRouting.clientMux = agent.newMemdClientMux(cfg.kvServerList)

	oldRouting := agent.routingInfo.Get()
	if oldRouting == nil {
		return false
	}

	// Check that the new config data is newer than the current one, in the case where we've done a select bucket
	// against an existing connection then the revisions could be the same. In that case the configuration still
	// needs to be applied.
	if newRouting.revID == 0 {
		logDebugf("Unversioned configuration data, ")
	} else if newRouting.revID == oldRouting.revID {
		logDebugf("Ignoring configuration with identical revision number")
		return false
	} else if newRouting.revID < oldRouting.revID {
		logDebugf("Ignoring new configuration as it has an older revision id")
		return false
	}

	// Attempt to atomically update the routing data
	if !agent.routingInfo.Update(oldRouting, newRouting) {
		logErrorf("Someone preempted the config update, skipping update")
		return false
	}

	logDebugf("Switching routing data (update)...")
	logDebugf("New Routing Data:\n%s", newRouting.DebugString())

	if oldRouting.clientMux == nil {
		// This is a new agent so there is no existing muxer.  We can
		// simply start the new muxer.
		newRouting.clientMux.Start()
	} else {
		// Get the new muxer to takeover the pipelines from the older one
		newRouting.clientMux.Takeover(oldRouting.clientMux)

		// Gather all the requests from all the old pipelines and then
		//  sort and redispatch them (which will use the new pipelines)
		var requestList []*memdQRequest
		oldRouting.clientMux.Drain(func(req *memdQRequest) {
			requestList = append(requestList, req)
		})

		sort.Sort(memdQRequestSorter(requestList))

		for _, req := range requestList {
			agent.stopCmdTrace(req)
			agent.requeueDirect(req, false)
		}
	}

	return true
}

func (agent *Agent) updateRoutingConfig(cfg cfgObj) bool {
	if cfg == nil {
		// Use the existing config if none was passed.
		oldRouting := agent.routingInfo.Get()
		if oldRouting == nil {
			// If there is no previous config, we can't do anything
			return false
		}

		return agent.applyRoutingConfig(oldRouting.source)
	}

	// Normalize the cfgBucket to a routeConfig and apply it.
	routeCfg := buildRouteConfig(cfg, agent.IsSecure(), agent.networkType, false)
	if !routeCfg.IsValid() {
		// We received an invalid configuration, lets shutdown.
		err := agent.Close()
		if err != nil {
			logErrorf("Invalid config caused agent close failure (%s)", err)
		}

		return false
	}

	return agent.applyRoutingConfig(routeCfg)
}

func (agent *Agent) routeRequest(req *memdQRequest) (*memdPipeline, error) {
	routingInfo := agent.routingInfo.Get()
	if routingInfo == nil {
		return nil, ErrShutdown
	}

	var srvIdx int
	repIdx := req.ReplicaIdx

	// Route to specific server
	if repIdx < 0 {
		srvIdx = -repIdx - 1
	} else {
		var err error

		if routingInfo.bktType == bktTypeCouchbase {
			if req.Key != nil {
				req.Vbucket = routingInfo.vbMap.VbucketByKey(req.Key)
			}

			srvIdx, err = routingInfo.vbMap.NodeByVbucket(req.Vbucket, uint32(repIdx))
			if err != nil {
				return nil, err
			}
		} else if routingInfo.bktType == bktTypeMemcached {
			if repIdx > 0 {
				// Error. Memcached buckets don't understand replicas!
				return nil, ErrInvalidReplica
			}

			if len(req.Key) == 0 {
				// Non-broadcast keyless Memcached bucket request
				return nil, ErrCliInternalError
			}

			srvIdx, err = routingInfo.ketamaMap.NodeByKey(req.Key)
			if err != nil {
				return nil, err
			}
		}
	}

	return routingInfo.clientMux.GetPipeline(srvIdx), nil
}

func (agent *Agent) dispatchDirect(req *memdQRequest) error {
	agent.startCmdTrace(req)

	for {
		pipeline, err := agent.routeRequest(req)
		if err != nil {
			return err
		}

		err = pipeline.SendRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err == errPipelineFull {
			return ErrOverload
		} else if err != nil {
			return err
		}

		break
	}

	return nil
}

func (agent *Agent) dispatchDirectToAddress(req *memdQRequest, address string) error {
	agent.startCmdTrace(req)

	// We set the ReplicaIdx to a negative number to ensure it is not redispatched
	// and we check that it was 0 to begin with to ensure it wasn't miss-used.
	if req.ReplicaIdx != 0 {
		return ErrInvalidReplica
	}
	req.ReplicaIdx = -999999999

	for {
		routingInfo := agent.routingInfo.Get()
		if routingInfo == nil {
			return ErrShutdown
		}

		var foundPipeline *memdPipeline
		for _, pipeline := range routingInfo.clientMux.pipelines {
			if pipeline.Address() == address {
				foundPipeline = pipeline
				break
			}
		}

		if foundPipeline == nil {
			return ErrInvalidServer
		}

		err := foundPipeline.SendRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err == errPipelineFull {
			return ErrOverload
		} else if err != nil {
			return err
		}

		break
	}

	return nil
}

func (agent *Agent) requeueDirect(req *memdQRequest, isRetry bool) {
	agent.startCmdTrace(req)
	handleError := func(err error) {
		// We only want to log an error on retries if the error isn't cancelled.
		if !isRetry || (isRetry && err != ErrCancelled) {
			logErrorf("Reschedule failed, failing request (%s)", err)
		}

		req.tryCallback(nil, err)
	}

	logDebugf("Request being requeued, Opaque=%d", req.Opaque)

	for {
		pipeline, err := agent.routeRequest(req)
		if err != nil {
			handleError(err)
			return
		}

		err = pipeline.RequeueRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err != nil {
			handleError(err)
			return
		}

		break
	}
}
