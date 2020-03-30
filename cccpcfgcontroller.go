package gocbcore

import (
	"errors"
	"math/rand"
	"time"
)

type cccpConfigController struct {
	muxer              *kvMux
	cfgMgr             *configManagementComponent
	confCccpPollPeriod time.Duration
	confCccpMaxWait    time.Duration

	// Used exclusively for testing to overcome GOCBC-780. It allows a test to pause the cccp looper preventing
	// unwanted requests from being sent to the mock once it has been setup for error map testing.
	looperPauseSig chan bool

	looperStopSig chan struct{}
	looperDoneSig chan struct{}
}

func newCCCPConfigController(props cccpPollerProperties, muxer *kvMux, cfgMgr *configManagementComponent) *cccpConfigController {
	return &cccpConfigController{
		muxer:              muxer,
		cfgMgr:             cfgMgr,
		confCccpPollPeriod: props.confCccpPollPeriod,
		confCccpMaxWait:    props.confCccpMaxWait,

		looperPauseSig: make(chan bool),
		looperStopSig:  make(chan struct{}),
		looperDoneSig:  make(chan struct{}),
	}
}

type cccpPollerProperties struct {
	confCccpPollPeriod time.Duration
	confCccpMaxWait    time.Duration
}

func (ccc *cccpConfigController) Pause(paused bool) {
	ccc.looperPauseSig <- paused
}

func (ccc *cccpConfigController) Stop() {
	close(ccc.looperStopSig)
}

func (ccc *cccpConfigController) Done() chan struct{} {
	return ccc.looperDoneSig
}

func (ccc *cccpConfigController) DoLoop() error {
	tickTime := ccc.confCccpPollPeriod
	paused := false

	logDebugf("CCCP Looper starting.")

	nodeIdx := -1

Looper:
	for {
		if !paused {
			iter, err := ccc.muxer.PipelineIterator()
			if err != nil {
				// If we have an error it indicates the client is shut down.
				break
			}

			numNodes := iter.Len()
			if numNodes == 0 {
				logDebugf("CCCPPOLL: No nodes available to poll")
				continue
			}

			if nodeIdx < 0 {
				nodeIdx = rand.Intn(numNodes)
			}

			iter.Offset(nodeIdx)

			var foundConfig *cfgBucket
			// Until this gets a valid config the pipeline addresses will be the ones from the connection string, there is
			// an assumed contract between the looper and the upstream config manager that this is the case. This allows
			// the config manager to setup its network type correctly.
			for iter.Next() {
				pipeline := iter.Pipeline()
				cccpBytes, err := ccc.getClusterConfig(pipeline)
				if err != nil {
					logDebugf("CCCPPOLL: Failed to retrieve CCCP config. %v", err)
					if errors.Is(err, ErrDocumentNotFound) {
						// This error is indicative of a memcached bucket which we can't handle so return the error.
						logDebugf("CCCPPOLL: Document not found error detecting, returning error upstream.")
						return err
					}
					continue
				}

				hostName, err := hostFromHostPort(pipeline.Address())
				if err != nil {
					logErrorf("CCCPPOLL: Failed to parse source address. %v", err)
					continue
				}

				bk, err := parseConfig(cccpBytes, hostName)
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
			ccc.cfgMgr.OnNewConfig(foundConfig)
		}

		// Wait for either the agent to be shut down, or our tick time to expire
		select {
		case <-ccc.looperStopSig:
			break Looper
		case pause := <-ccc.looperPauseSig:
			paused = pause
		case <-time.After(tickTime):
		}
	}

	close(ccc.looperDoneSig)
	return nil
}

func (ccc *cccpConfigController) getClusterConfig(pipeline *memdPipeline) (cfgOut []byte, errOut error) {
	signal := make(chan struct{}, 1)
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:  reqMagic,
			Opcode: cmdGetClusterConfig,
		},
		Callback: func(resp *memdQResponse, _ *memdQRequest, err error) {
			if resp != nil {
				cfgOut = resp.memdPacket.Value
			}
			errOut = err
			signal <- struct{}{}
		},
		RetryStrategy: newFailFastRetryStrategy(),
	}
	err := pipeline.SendRequest(req)
	if err != nil {
		return nil, err
	}

	timeoutTmr := AcquireTimer(ccc.confCccpMaxWait)
	select {
	case <-signal:
		ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		ReleaseTimer(timeoutTmr, true)
		req.Cancel(errAmbiguousTimeout)
		<-signal
		return
	}
}
