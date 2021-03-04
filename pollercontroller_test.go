package gocbcore

// This test tests that after calling stop then force http poller will not attempt to do work.
func (suite *UnitTestSuite) TestPollerControllerForceHTTPAndStopRace() {
	ccp := &cccpConfigController{
		looperStopSig: make(chan struct{}),
		looperDoneSig: make(chan struct{}),
	}
	htt := &httpConfigController{
		looperStopSig: make(chan struct{}),
		looperDoneSig: make(chan struct{}),
	}

	poller := newPollerController(ccp, htt, &configManagementComponent{})
	poller.activeController = ccp

	poller.Stop()
	poller.ForceHTTPPoller()
}
