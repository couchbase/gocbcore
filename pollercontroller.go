package gocbcore

import (
	"errors"
	"sync"
)

type pollerController struct {
	activeController configPollerController
	controllerLock   sync.Mutex

	cccpPoller *cccpConfigController
	httpPoller *httpConfigController
}

type configPollerController interface {
	Pause(paused bool)
	Done() chan struct{}
	Stop()
}

func newPollerController(cccpPoller *cccpConfigController, httpPoller *httpConfigController) *pollerController {
	return &pollerController{
		cccpPoller: cccpPoller,
		httpPoller: httpPoller,
	}
}

func (pc *pollerController) Start() {
	if pc.cccpPoller == nil {
		pc.controllerLock.Lock()
		pc.activeController = pc.httpPoller
		pc.controllerLock.Unlock()
		pc.httpPoller.DoLoop()
		return
	}
	pc.controllerLock.Lock()
	pc.activeController = pc.cccpPoller
	pc.controllerLock.Unlock()
	err := pc.cccpPoller.DoLoop()
	if err != nil {
		if pc.httpPoller == nil {
			logErrorf("CCCP poller has exited for http fallback but no http poller is configured")
			return
		}
		if errors.Is(err, ErrDocumentNotFound) {
			pc.controllerLock.Lock()
			pc.activeController = pc.httpPoller
			pc.controllerLock.Unlock()
			pc.httpPoller.DoLoop()
		}
	}
}

func (pc *pollerController) Pause(paused bool) {
	pc.controllerLock.Lock()
	controller := pc.activeController
	pc.controllerLock.Unlock()
	if controller != nil {
		controller.Pause(paused)
	}
}

func (pc *pollerController) Stop() {
	pc.controllerLock.Lock()
	controller := pc.activeController
	pc.controllerLock.Unlock()

	if controller != nil {
		controller.Stop()
	}
}

func (pc *pollerController) Done() chan struct{} {
	pc.controllerLock.Lock()
	controller := pc.activeController
	pc.controllerLock.Unlock()

	if controller == nil {
		return nil
	}
	return controller.Done()
}
