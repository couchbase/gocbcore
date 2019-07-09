package gocbcore

import (
	"math/rand"
	"time"
)

func (agent *Agent) gcccpLooper() {
	tickTime := agent.confCccpPollPeriod
	maxWaitTime := agent.confCccpMaxWait
	cancelled := false

	logDebugf("GCCCP Looper starting.")

	nodeIdx := -1
	for {
		// Wait for either the agent to be shut down, or our tick time to expire
		select {
		case <-time.After(tickTime):
		case <-agent.closeNotify:
		case <-agent.gcccpLooperStopSig:
			logDebugf("GCCCP poller received shutdown signal")
			cancelled = true
		}

		if cancelled {
			break
		}

		routingInfo := agent.routingInfo.Get()
		if routingInfo == nil {
			// If we have a blank routingInfo, it indicates the client is shut down.
			break
		}

		numNodes := routingInfo.clientMux.NumPipelines()
		if numNodes == 0 {
			logDebugf("GCCCPPOLL: No nodes available to poll")
			continue
		}

		if nodeIdx < 0 {
			nodeIdx = rand.Intn(numNodes)
		}

		var foundConfig *cfgCluster
		for nodeOff := 0; nodeOff < numNodes; nodeOff++ {
			nodeIdx = (nodeIdx + 1) % numNodes

			pipeline := routingInfo.clientMux.GetPipeline(nodeIdx)

			client := syncClient{
				client: &memdPipelineSenderWrap{
					pipeline: pipeline,
				},
			}
			cccpBytes, err := client.ExecGetClusterConfig(time.Now().Add(maxWaitTime))
			if err != nil {
				logDebugf("GCCCPPOLL: Failed to retrieve CCCP config. %v", err)
				continue
			}

			hostName, err := hostFromHostPort(pipeline.Address())
			if err != nil {
				logErrorf("GCCCPPOLL: Failed to parse source address. %v", err)
				continue
			}

			cfg, err := parseClusterConfig(cccpBytes, hostName)
			if err != nil {
				logDebugf("GCCCPPOLL: Failed to parse CCCP config. %v", err)
				continue
			}

			foundConfig = cfg
			break
		}

		if foundConfig == nil {
			logDebugf("GCCCPPOLL: Failed to retrieve config from any node.")
			continue
		}

		logDebugf("GCCCPPOLL: Received new config")
		agent.updateConfig(foundConfig)
	}

	close(agent.gcccpLooperDoneSig)
}
