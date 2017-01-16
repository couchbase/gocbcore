package gocbcore

import (
	"container/list"
	"crypto/tls"
	"net"
	"sort"
	"time"
)

type memdInitFunc func(*syncClient, time.Time) error

func (agent *Agent) dialMemdClient(address string) (*memdClient, error) {
	// Copy the tls configuration since we need to provide the hostname for each
	// server that we connect to so that the certificate can be validated properly.
	var tlsConfig *tls.Config = nil
	if agent.tlsConfig != nil {
		host, _, _ := net.SplitHostPort(address)
		tlsConfig = &tls.Config{
			InsecureSkipVerify: agent.tlsConfig.InsecureSkipVerify,
			RootCAs:            agent.tlsConfig.RootCAs,
			ServerName:         host,
		}
	}

	deadline := time.Now().Add(agent.serverConnectTimeout)

	memdConn, err := DialMemdConn(address, tlsConfig, deadline)
	if err != nil {
		logDebugf("Failed to connect. %v", err)
		return nil, err
	}

	client := newMemdClient(memdConn)

	sclient := syncClient{
		client: client,
	}

	logDebugf("Authenticating...")
	err = agent.initFn(&sclient, deadline)
	if err != nil {
		logDebugf("Failed to authenticate. %v", err)
		memdConn.Close()
		return nil, err
	}

	return client, nil
}

func (agent *Agent) slowDialMemdClient(address string) (*memdClient, error) {
	agent.serverFailuresLock.Lock()
	failureTime := agent.serverFailures[address]
	agent.serverFailuresLock.Unlock()

	if !failureTime.IsZero() {
		waitedTime := time.Since(failureTime)
		if waitedTime < agent.serverWaitTimeout {
			time.Sleep(agent.serverWaitTimeout - waitedTime)
		}
	}

	client, err := agent.dialMemdClient(address)
	if err != nil {
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
func (agent *Agent) applyConfig(cfg *routeConfig) {
	// Check some basic things to ensure consistency!
	if len(cfg.vbMap) != agent.numVbuckets {
		panic("Received a configuration with a different number of vbuckets.")
	}

	// Only a single thing can modify the config at any time
	agent.configLock.Lock()
	defer agent.configLock.Unlock()

	newRouting := &routeData{
		revId:      cfg.revId,
		capiEpList: cfg.capiEpList,
		mgmtEpList: cfg.mgmtEpList,
		n1qlEpList: cfg.n1qlEpList,
		ftsEpList:  cfg.ftsEpList,
		vbMap:      cfg.vbMap,
		bktType:    cfg.bktType,
		source:     cfg,
	}

	kvPoolSize := agent.kvPoolSize
	maxQueueSize := agent.maxQueueSize
	for _, hostPort := range cfg.kvServerList {
		hostPort := hostPort

		getClientFn := func() (*memdClient, error) {
			return agent.slowDialMemdClient(hostPort)
		}
		pipeline := newPipeline(hostPort, kvPoolSize, maxQueueSize, getClientFn)

		newRouting.kvPipelines = append(newRouting.kvPipelines, pipeline)
	}

	newRouting.deadPipe = newDeadPipeline(maxQueueSize)

	oldRouting := agent.routingInfo.get()
	if oldRouting == nil {
		return
	}

	if newRouting.revId == 0 {
		logDebugf("Unversioned configuration data, ")
	} else if newRouting.revId == oldRouting.revId {
		logDebugf("Ignoring configuration with identical revision number")
		return
	} else if newRouting.revId <= oldRouting.revId {
		logDebugf("Ignoring new configuration as it has an older revision id")
		return
	}

	// Attempt to atomically update the routing data
	if !agent.routingInfo.update(oldRouting, newRouting) {
		logErrorf("Someone preempted the config update, skipping update")
		return
	}

	logDebugf("Switching routing data (update)...")
	logDebugf("New Routing Data:\n%s", newRouting.debugString())

	// Gather all our old pipelines up for takeover and what not
	oldPipelines := list.New()
	for _, pipeline := range oldRouting.kvPipelines {
		oldPipelines.PushBack(pipeline)
	}

	// Build a function to find an existing pipeline
	stealPipeline := func(address string) *memdPipeline {
		for e := oldPipelines.Front(); e != nil; e = e.Next() {
			pipeline := e.Value.(*memdPipeline)

			if pipeline.Address() == address {
				oldPipelines.Remove(e)
				return pipeline
			}
		}

		return nil
	}

	// Initialize new pipelines (possibly with a takeover)
	for _, pipeline := range newRouting.kvPipelines {
		oldPipeline := stealPipeline(pipeline.Address())
		if oldPipeline != nil {
			pipeline.Takeover(oldPipeline)
		}

		pipeline.StartClients()
	}

	// Shut down any pipelines that were not taken over
	for e := oldPipelines.Front(); e != nil; e = e.Next() {
		pipeline := e.Value.(*memdPipeline)
		pipeline.Close()
	}
	if oldRouting.deadPipe != nil {
		oldRouting.deadPipe.Close()
	}

	// Gather all the requests from all the old pipelines and then
	//  sort and redispatch them (which will use the new pipelines)
	var requestList []*memdQRequest
	for _, pipeline := range oldRouting.kvPipelines {
		logDebugf("Draining queue %+v", pipeline)

		pipeline.Drain(func(req *memdQRequest) {
			requestList = append(requestList, req)
		})
	}
	if oldRouting.deadPipe != nil {
		oldRouting.deadPipe.Drain(func(req *memdQRequest) {
			requestList = append(requestList, req)
		})
	}

	sort.Sort(memdQRequestSorter(requestList))

	for _, req := range requestList {
		agent.requeueDirect(req)
	}
}

func (agent *Agent) updateConfig(bk *cfgBucket) {
	if bk == nil {
		// Use the existing config if none was passed.
		oldRouting := agent.routingInfo.get()
		if oldRouting == nil {
			// If there is no previous config, we can't do anything
			return
		}

		agent.applyConfig(oldRouting.source)
	} else {
		// Normalize the cfgBucket to a routeConfig and apply it.
		routeCfg := buildRouteConfig(bk, agent.IsSecure())
		if !routeCfg.IsValid() {
			// We received an invalid configuration, lets shutdown.
			agent.Close()
			return
		}

		agent.applyConfig(routeCfg)
	}
}

func (agent *Agent) routeRequest(req *memdQRequest) (*memdPipeline, error) {
	routingInfo := agent.routingInfo.get()
	if routingInfo == nil {
		return nil, ErrShutdown
	}

	var srvIdx int
	repId := req.ReplicaIdx

	// Route to specific server
	if repId < 0 {
		srvIdx = -repId - 1
	} else {

		if routingInfo.bktType == BktTypeCouchbase {
			// Targeting a specific replica; repId >= 0
			if repId >= len(routingInfo.vbMap[0]) {
				return nil, ErrInvalidReplica
			}

			if req.Key != nil {
				srvIdx, req.Vbucket = routingInfo.MapKeyVBucket(req.Key, repId)
			} else {
				// Filter explicit vBucket input. Really only used in OBSERVE
				if int(req.Vbucket) >= len(routingInfo.vbMap) {
					return nil, ErrInvalidVBucket
				}

				srvIdx = routingInfo.vbMap[req.Vbucket][repId]
			}
		} else if routingInfo.bktType == BktTypeMemcached {
			if repId > 0 {
				// Error. Memcached buckets don't understand replicas!
				return nil, ErrInvalidReplica
			}

			if req.Key == nil {
				// Non-broadcast keyless Memcached bucket request
				return nil, ErrInvalidArgs
			}

			srvIdx = routingInfo.MapKetama(req.Key)
		}
	}

	if srvIdx < 0 {
		return routingInfo.deadPipe, nil
	} else if srvIdx >= len(routingInfo.kvPipelines) {
		return nil, ErrInvalidServer
	}

	return routingInfo.kvPipelines[srvIdx], nil
}

func (agent *Agent) dispatchDirect(req *memdQRequest) error {
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

func (agent *Agent) requeueDirect(req *memdQRequest) {
	handleError := func(err error) {
		logErrorf("Reschedule failed, failing request (%s)", err)

		req.tryCallback(nil, err)
	}

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
