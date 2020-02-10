package gocbcore

import (
	"math/rand"
	"time"
)

func (agent *Agent) cccpLooper() {
	tickTime := agent.confCccpPollPeriod
	maxWaitTime := agent.confCccpMaxWait
	paused := false

	logDebugf("CCCP Looper starting.")

	nodeIdx := -1
	for {
		// Wait for either the agent to be shut down, or our tick time to expire
		select {
		case pause := <-agent.cccpLooperPauseSig:
			paused = pause
		case <-time.After(tickTime):
		case <-agent.closeNotify:
		}

		if paused {
			continue
		}

		routingInfo := agent.routingInfo.Get()
		if routingInfo == nil {
			// If we have a blank routingInfo, it indicates the client is shut down.
			break
		}

		numNodes := routingInfo.clientMux.NumPipelines()
		if numNodes == 0 {
			logDebugf("CCCPPOLL: No nodes available to poll")
			continue
		}

		if nodeIdx < 0 {
			nodeIdx = rand.Intn(numNodes)
		}

		var foundConfig *cfgBucket
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
				logDebugf("CCCPPOLL: Failed to retrieve CCCP config. %v", err)
				continue
			}

			hostName, err := hostFromHostPort(pipeline.Address())
			if err != nil {
				logErrorf("CCCPPOLL: Failed to parse source address. %v", err)
				continue
			}

			bk, err := parseBktConfig(cccpBytes, hostName)
			if err != nil {
				logDebugf("CCCPPOLL: Failed to parse CCCP config. %v", err)
				continue
			}

			foundConfig = bk
			break
		}

		if foundConfig == nil {
			logDebugf("CCCPPOLL: Failed to retrieve config from any node.")
			continue
		}

		logDebugf("CCCPPOLL: Received new config")
		agent.updateConfig(foundConfig)
	}

	close(agent.cccpLooperDoneSig)
}
