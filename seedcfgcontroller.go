package gocbcore

type seedConfigController struct {
	*baseHTTPConfigController
	seed    string
	iterNum uint64
}

func newSeedConfigController(seed, bucketName string, props httpPollerProperties,
	cfgMgr *configManagementComponent) *seedConfigController {
	scc := &seedConfigController{
		seed: seed,
	}
	scc.baseHTTPConfigController = newBaseHTTPConfigController(bucketName, props, cfgMgr, scc.GetEndpoint)

	return scc
}

func (scc *seedConfigController) GetEndpoint(iterNum uint64) string {
	if scc.iterNum == iterNum {
		return ""
	}

	scc.iterNum = iterNum
	return scc.seed
}

// Pause was added solely for testing purposes and we don't need to do anything with it for this.
// Once we move to Gocaves for mocking then Pause will go away.
func (scc *seedConfigController) Pause(paused bool) {
}

func (scc *seedConfigController) Start() {
	scc.DoLoop()
}

func (scc *seedConfigController) PollerError() error {
	return scc.Error()
}

// We're already a http poller so do nothing
func (scc *seedConfigController) ForceHTTPPoller() {
}
