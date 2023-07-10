package gocbcore

import (
	"errors"
	"sync"
	"sync/atomic"
)

type pollerController struct {
	activeController configPoller
	controllerLock   sync.Mutex
	stopped          bool
	bucketConfigSeen uint32

	cccpPoller *cccpConfigController
	httpPoller *httpConfigController
	cfgMgr     configManager

	isFallbackErrorFn func(error) bool
}

type configPollerController interface {
	Run()
	Stop()
	PollerError() error
	ForceHTTPPoller()
}

type configPoller interface {
	Stop()
	Reset()
	Error() error
}

func newPollerController(cccpPoller *cccpConfigController, httpPoller *httpConfigController, cfgMgr configManager,
	errorFn func(error) bool) *pollerController {
	pc := &pollerController{
		cccpPoller:        cccpPoller,
		httpPoller:        httpPoller,
		cfgMgr:            cfgMgr,
		isFallbackErrorFn: errorFn,
	}
	cfgMgr.AddConfigWatcher(pc)

	return pc
}

// OnNewRouteConfig listens out for every config that comes in so that we (re)start the cccp if applicable.
func (pc *pollerController) OnNewRouteConfig(cfg *routeConfig) {
	if cfg.bktType != bktTypeCouchbase && cfg.bktType != bktTypeMemcached {
		return
	}
	atomic.SwapUint32(&pc.bucketConfigSeen, 1)

	if cfg.bktType == bktTypeMemcached {
		return
	}

	go func() {
		pc.controllerLock.Lock()
		if pc.stopped {
			pc.controllerLock.Unlock()
			return
		}
		if pc.activeController == pc.httpPoller {
			logInfof("Found couchbase bucket and HTTP poller in use. Restarting poller run loop to start cccp.")
			pc.activeController = nil
			pc.controllerLock.Unlock()

			// Stopping the poller will trigger the run loop to loop again.
			pc.httpPoller.Stop()
			return
		}
		pc.controllerLock.Unlock()
	}()
}

func (pc *pollerController) Run() {
	for {
		logInfof("Starting poller controller loop")
		pc.controllerLock.Lock()
		if pc.stopped {
			pc.controllerLock.Unlock()
			logInfof("Poller controller stopped, exiting")
			return
		}

		if pc.httpPoller != nil {
			pc.httpPoller.Reset()
		}
		pc.cccpPoller.Reset()

		atomic.SwapUint32(&pc.bucketConfigSeen, 0)
		pc.activeController = pc.cccpPoller
		pc.controllerLock.Unlock()

		err := pc.cccpPoller.DoLoop()
		if err != nil {
			logDebugf("CCCP poller has exited with err: %v", err)
		}
		if atomic.LoadUint32(&pc.bucketConfigSeen) == 1 {
			logInfof("Config seen but CCCP poller exited, restarting CCCP poller.")
			// CCCP managed to fetch a config whilst we were waiting for shutdown, in this case we want to just
			// start CCCP again as the bucket must exist and be a couchbase bucket.
			continue
		}
		pc.controllerLock.Lock()
		if pc.stopped {
			pc.controllerLock.Unlock()
			logDebugf("Poller controller stopped, exiting")
			return
		}

		if pc.httpPoller == nil {
			pc.controllerLock.Unlock()
			logErrorf("CCCP poller has exited for http fallback but no http poller is configured, retrying CCCP")
			continue
		}

		pc.activeController = pc.httpPoller
		pc.controllerLock.Unlock()
		pc.httpPoller.DoLoop()
	}
}

// Stop should never be called more than once.
func (pc *pollerController) Stop() {
	logInfof("Stopping poller controller")
	pc.controllerLock.Lock()
	pc.stopped = true
	controller := pc.activeController
	pc.controllerLock.Unlock()

	if controller != nil {
		controller.Stop()
	}
}

type pollerErrorProvider interface {
	PollerError() error
}

// PollerError surfaces any error of the underlying poller is currently in an error state.
func (pc *pollerController) PollerError() error {
	pc.controllerLock.Lock()
	controller := pc.activeController
	pc.controllerLock.Unlock()

	if controller == nil {
		return nil
	}

	return controller.Error()
}

func (pc *pollerController) ForceHTTPPoller() {
	if pc.httpPoller == nil {
		logErrorf("Attempting to force http poller but no http poller is configured")
		return
	}
	if !pc.httpPoller.CanPoll() {
		logDebugf("Attempting to force http poller but there are no http endpoints to poll")
		return
	}
	go func() {
		if atomic.LoadUint32(&pc.bucketConfigSeen) == 1 {
			logInfof("Config already seen, not forcing HTTP")
			// If we've seen a config already then either cccp or http polling have managed to fetch a config and
			// bucket type can't have changed so there's no reason to fallback.
			return
		}

		pc.controllerLock.Lock()
		if pc.stopped || pc.activeController == nil {
			// If active controller is nil at this point then something strange is happening, we're trying to force
			// http polling at the same time as we've received a config via http polling and are attempting to reset to
			// use cccp polling (which means that the server must support cccp). If this happens let's just let
			// cccp start up.
			pc.controllerLock.Unlock()
			return
		}
		if pc.activeController == pc.cccpPoller {
			logInfof("Stopping CCCP poller for HTTP polling takeover")
			pc.activeController = nil
			pc.cccpPoller.Stop()
			pc.controllerLock.Unlock()

			return
		}
		pc.controllerLock.Unlock()
	}()
}

func isPollingFallbackError(err error, bucket string) bool {
	if bucket == "" {
		return false
	}
	return errors.Is(err, ErrDocumentNotFound) || errors.Is(err, ErrUnsupportedOperation) ||
		errors.Is(err, errNoCCCPHosts) || errors.Is(err, ErrBucketNotFound)
}
