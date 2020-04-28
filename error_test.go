package gocbcore

import (
	"errors"

	"github.com/couchbase/gocbcore/v9/memd"

	"github.com/couchbase/gocbcore/v9/jcbmock"
)

func (suite *StandardTestSuite) TestEnhancedErrors() {
	agent, s := suite.GetAgentAndHarness()

	var err1, err2 error

	s.PushOp(agent.Get(GetOptions{
		Key: []byte("keyThatWontExist"),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			err1 = err
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get(GetOptions{
		Key: []byte("keyThatWontExist"),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			err2 = err
		})
	}))
	s.Wait(0)

	if err1 == err2 {
		suite.T().Fatalf("Operation error results should never return equivalent values")
	}
}

func (suite *StandardTestSuite) TestEnhancedErrorOp() {
	suite.EnsureSupportsFeature(TestFeatureErrMap)

	agent, h := suite.GetAgentAndHarness()
	if !suite.IsMockServer() {
		suite.T().Skipf("only supported when testing against mock server")
	}

	suite.mockInst.Control(jcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": true,
		"bucket":  suite.BucketName,
	}))

	h.PushOp(agent.GetAndLock(GetAndLockOptions{
		Key:      []byte("testEnhancedErrs"),
		LockTime: 10,
	}, func(res *GetAndLockResult, err error) {
		h.Wrap(func() {
			typedErr, ok := err.(*KeyValueError)
			if !ok {
				h.Fatalf("error should be a KeyValueError: %v", err)
			}

			if typedErr.Context == "" {
				h.Fatalf("error should have a context")
			}

			if typedErr.ErrorName == "" {
				h.Fatalf("error should have a name")
			}

			if typedErr.ErrorDescription == "" {
				h.Fatalf("error should have a description")
			}

			if typedErr.StatusCode != memd.StatusKeyNotFound {
				h.Fatalf("status code should have been StatusKeyNotFound")
			}

			if !errors.Is(err, ErrDocumentNotFound) {
				h.Fatalf("error cause should have been ErrDocumentNotFound")
			}
		})
	}))
	h.Wait(0)

	suite.mockInst.Control(jcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": false,
		"bucket":  suite.BucketName,
	}))
}
