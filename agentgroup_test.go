package gocbcore

import (
	"errors"
	"sync"
	"time"
)

func (suite *StandardTestSuite) TestOpenBucketReturnsSameAgent() {
	config := suite.makeAgentGroupConfig(globalTestConfig)
	ag, err := suite.initAgentGroup(config)
	suite.Require().Nil(err, err)
	defer ag.Close()

	bucketName := globalTestConfig.BucketName

	err = ag.OpenBucket(bucketName)
	suite.Require().Nil(err, err)

	agent1 := ag.GetAgent(bucketName)
	suite.Require().NotNil(agent1)

	err = ag.OpenBucket(bucketName)
	suite.Require().Nil(err, err)

	agent2 := ag.GetAgent(bucketName)
	suite.Require().NotNil(agent2)

	suite.Assert().Same(agent1, agent2)
}

func (suite *StandardTestSuite) TestOpenBucketConcurrentReturnsSameAgent() {
	config := suite.makeAgentGroupConfig(globalTestConfig)
	ag, err := suite.initAgentGroup(config)
	suite.Require().Nil(err, err)
	defer ag.Close()

	bucketName := globalTestConfig.BucketName

	numGoroutines := 10
	errs := make([]error, numGoroutines)
	agents := make([]*Agent, numGoroutines)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = ag.OpenBucket(bucketName)
			agents[idx] = ag.GetAgent(bucketName)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < numGoroutines; i++ {
		suite.Require().Nil(errs[i], errs[i])
		suite.Require().NotNil(agents[i])
		suite.Assert().Same(agents[0], agents[i])
	}
}

// This test was added as part of GOCBC-1803 to ensure that we don't change our behavior in various invalid credential scenarios.
// It's not behavior that's explicitly specced for all test cases, but we should ensure we don't introduce untintentional changes.
func (suite *StandardTestSuite) TestAgentGroupWaitUntilReadyInvalidCredentials() {
	type testCase struct {
		name          string
		username      string
		password      string
		expectedError error
	}

	suite.EnsureSupportsFeature(TestFeatureCavesUnreliable)

	// This test purposefully triggers error cases.
	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	var validUsername string
	{
		userPass, err := globalTestConfig.Authenticator.Credentials(AuthCredsRequest{})
		suite.Require().NoError(err)
		suite.Require().Len(userPass, 1)
		validUsername = userPass[0].Username
	}

	testCases := []testCase{
		{
			name:          "InvalidUser",
			username:      "i_dont_exist",
			password:      "this_is_a_password",
			expectedError: ErrAuthenticationFailure,
		},
		{
			name:     "EmptyCredentials",
			username: "",
			password: "",
			// SASL auth succeeds anonymously but CCCP polling fails with EACCESS, so WaitUntilReady times out with NotReadyRetryReason.
			// TBD whether we should improve this behaviour.
			expectedError: ErrUnambiguousTimeout,
		},
		{
			name:          "IncorrectPassword",
			username:      validUsername,
			password:      "this_is_a_wrong_password",
			expectedError: ErrAuthenticationFailure,
		},
		{
			name:          "ValidUserEmptyPassword",
			username:      validUsername,
			password:      "",
			expectedError: ErrAuthenticationFailure,
		},
		{
			name:          "InvalidUserEmptyPassword",
			username:      "i_dont_exist",
			password:      "",
			expectedError: ErrAuthenticationFailure,
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			cfg := makeAgentGroupConfig(globalTestConfig)
			cfg.SecurityConfig.Auth = PasswordAuthProvider{
				Username: tc.username,
				Password: tc.password,
			}
			ag, err := CreateAgentGroup(&cfg)
			suite.Require().NoError(err)
			defer ag.Close()
			s := suite.GetHarness()

			s.PushOp(ag.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
				s.Wrap(func() {
					if !errors.Is(err, tc.expectedError) {
						s.Fatalf("WaitUntilReady failed with unexpected error: %v", err)
					}
				})
			}))
			s.Wait(6)
		})
	}
}
