package gocbcore

type waitUntilConfigComponent struct {
	configSeenCh chan struct{}
	cfgMgr       *configManager
}

type waitOp struct {
	cancelCh chan struct{}
}

func (op *waitOp) Cancel(err error) {
	op.cancelCh <- struct{}{}
}

func newWaitUntilConfigComponent(cfgMgr *configManager) *waitUntilConfigComponent {
	w := &waitUntilConfigComponent{
		cfgMgr:       cfgMgr,
		configSeenCh: make(chan struct{}),
	}

	cfgMgr.AddConfigWatcher(w)
	return w
}

func (wum *waitUntilConfigComponent) OnNewRouteConfig(_ *routeConfig) {
	wum.cfgMgr.RemoveConfigWatcher(wum)
	close(wum.configSeenCh)
}

func (wum *waitUntilConfigComponent) WaitUntilFirstConfig(cb func()) (PendingOp, error) {
	op := &waitOp{
		cancelCh: make(chan struct{}),
	}

	go func() {
		select {
		case <-wum.configSeenCh:
			cb()
		case <-op.cancelCh:
		}
	}()

	return op, nil
}
