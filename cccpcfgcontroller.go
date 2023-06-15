package gocbcore

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

type cccpConfigController struct {
	muxer              dispatcher
	cfgMgr             *configManagementComponent
	confCccpPollPeriod time.Duration
	confCccpMaxWait    time.Duration

	looperStopSig chan struct{}
	looperDoneSig chan struct{}

	fetchErr error
	errLock  sync.Mutex

	isFallbackErrorFn func(error) bool
	noConfigFoundFn   func(error)
}

func newCCCPConfigController(props cccpPollerProperties, muxer dispatcher, cfgMgr *configManagementComponent,
	isFallbackErrorFn func(error) bool, noConfigFoundFn func(error)) *cccpConfigController {
	return &cccpConfigController{
		muxer:              muxer,
		cfgMgr:             cfgMgr,
		confCccpPollPeriod: props.confCccpPollPeriod,
		confCccpMaxWait:    props.confCccpMaxWait,

		looperStopSig: make(chan struct{}),
		looperDoneSig: make(chan struct{}),

		isFallbackErrorFn: isFallbackErrorFn,
		noConfigFoundFn:   noConfigFoundFn,
	}
}

type cccpPollerProperties struct {
	confCccpPollPeriod time.Duration
	confCccpMaxWait    time.Duration
}

func (ccc *cccpConfigController) Error() error {
	ccc.errLock.Lock()
	defer ccc.errLock.Unlock()
	return ccc.fetchErr
}

func (ccc *cccpConfigController) setError(err error) {
	ccc.errLock.Lock()
	ccc.fetchErr = err
	ccc.errLock.Unlock()
}

func (ccc *cccpConfigController) Stop() {
	logInfof("CCCP Looper stopping")
	close(ccc.looperStopSig)
}

func (ccc *cccpConfigController) Done() chan struct{} {
	return ccc.looperDoneSig
}

// Reset must never be called concurrently with the Stop or whilst the poll loop is running.
func (ccc *cccpConfigController) Reset() {
	ccc.looperStopSig = make(chan struct{})
	ccc.looperDoneSig = make(chan struct{})
}

func (ccc *cccpConfigController) DoLoop() error {
	if err := ccc.doLoop(); err != nil {
		return err
	}

	logInfof("CCCP Looper stopped")
	close(ccc.looperDoneSig)
	return nil
}

func (ccc *cccpConfigController) doLoop() error {
	tickTime := ccc.confCccpPollPeriod

	logInfof("CCCP Looper starting.")
	nodeIdx := -1
	// The first time that we loop we want to skip any sleep so that we can try get a config and bootstrapped ASAP.
	firstLoop := true

	for {
		if !firstLoop {
			// Wait for either the agent to be shut down, or our tick time to expire
			select {
			case <-ccc.looperStopSig:
				return nil
			case <-time.After(tickTime):
			}
		}
		firstLoop = false

		iter, err := ccc.muxer.PipelineSnapshot()
		if err != nil {
			// If we have an error it indicates the client is shut down.
			break
		}

		numNodes := iter.NumPipelines()
		if numNodes == 0 {
			logInfof("CCCPPOLL: No nodes available to poll, returning upstream")
			return errNoCCCPHosts
		}

		if nodeIdx < 0 || nodeIdx > numNodes {
			nodeIdx = rand.Intn(numNodes) // #nosec G404
		}

		var foundConfig *cfgBucket
		var fallbackErr error
		var wasCancelled bool
		iter.Iterate(nodeIdx, func(pipeline *memdPipeline) bool {
			nodeIdx = (nodeIdx + 1) % numNodes
			cccpBytes, err := ccc.getClusterConfig(pipeline)
			if err != nil {
				if ccc.isFallbackErrorFn(err) {
					fallbackErr = err
					return false
				}

				// Only log the error at warn if it's unexpected.
				// If we cancelled the request or we're shutting down the connection then it's not really unexpected.
				if errors.Is(err, ErrRequestCanceled) || errors.Is(err, ErrShutdown) {
					wasCancelled = true
					logDebugf("CCCPPOLL: CCCP request was cancelled or connection was shutdown: %v", err)
					return true
				}

				// This error is checked by WaitUntilReady when no config has been seen.
				ccc.setError(err)

				logWarnf("CCCPPOLL: Failed to retrieve CCCP config. %s", err)
				return false
			}
			fallbackErr = nil
			ccc.setError(nil)

			logDebugf("CCCPPOLL: Got Block: %s", string(cccpBytes))

			hostName, err := hostFromHostPort(pipeline.Address())
			if err != nil {
				logWarnf("CCCPPOLL: Failed to parse source address. %s", err)
				return false
			}

			bk, err := parseConfig(cccpBytes, hostName)
			if err != nil {
				logWarnf("CCCPPOLL: Failed to parse CCCP config. %v", err)
				return false
			}

			foundConfig = bk
			return true
		})
		if fallbackErr != nil {
			// This error is indicative of a memcached bucket which we can't handle so return the error.
			logInfof("CCCPPOLL: CCCP not supported, returning error upstream.")
			return fallbackErr
		}

		if foundConfig == nil {
			// Only log the error at warn if it's unexpected.
			// If we cancelled the request then we're shutting down or request was requeued and this isn't unexpected.
			if wasCancelled {
				logDebugf("CCCPPOLL: CCCP request was cancelled.")
			} else {
				logWarnf("CCCPPOLL: Failed to retrieve config from any node.")
				ccc.noConfigFoundFn(err)
			}
			continue
		}

		logDebugf("CCCPPOLL: Received new config")
		ccc.cfgMgr.OnNewConfig(foundConfig)
	}

	return nil
}

func (ccc *cccpConfigController) getClusterConfig(pipeline *memdPipeline) (cfgOut []byte, errOut error) {
	signal := make(chan struct{}, 1)
	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:   memd.CmdMagicReq,
			Command: memd.CmdGetClusterConfig,
		},
		Callback: func(resp *memdQResponse, _ *memdQRequest, err error) {
			if resp != nil {
				cfgOut = resp.Packet.Value
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

		// We've timed out so lets check underlying connections to see if they're responsible.
		clients := pipeline.Clients()
		for _, cli := range clients {
			err := cli.Error()
			if err != nil {
				logDebugf("Found error in pipeline client %p/%s: %v", cli, cli.address, err)
				req.cancelWithCallback(err)
				<-signal
				return
			}
		}
		req.cancelWithCallback(errUnambiguousTimeout)
		<-signal
		return
	case <-ccc.looperStopSig:
		ReleaseTimer(timeoutTmr, false)
		req.cancelWithCallback(errRequestCanceled)
		<-signal
		return

	}
}
