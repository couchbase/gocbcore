package gocbcore

import (
	"testing"

	"gopkg.in/couchbaselabs/gojcbmock.v1"
)

func TestBasicErrors(t *testing.T) {
	globalAgent.useKvErrorMaps = false
	globalAgent.useEnhancedErrors = false

	checkTwice := func(code StatusCode) {
		err1 := globalAgent.makeBasicMemdError(code)
		err2 := globalAgent.makeBasicMemdError(code)
		if err1 != err2 {
			t.Fatalf("Status code %d should return a constant error", code)
		}
	}

	checkTwice(StatusKeyNotFound)
	checkTwice(StatusSubDocSuccessDeleted)

	globalAgent.useKvErrorMaps = true
	globalAgent.useEnhancedErrors = true
}

func TestEnhancedErrors(t *testing.T) {
	checkTwice := func(code StatusCode) {
		err1 := globalAgent.makeBasicMemdError(code)
		err2 := globalAgent.makeBasicMemdError(code)
		if err1 == err2 {
			t.Fatalf("Status code %d should not return a constant error", code)
		}
	}

	checkTwice(StatusKeyNotFound)
	checkTwice(StatusSubDocSuccessDeleted)
}

func TestEnhancedErrorOp(t *testing.T) {
	if !globalAgent.SupportsFeature(TestErrMapFeature) {
		t.Skip("Cannot test enhanced error ops with real server")
	}
	agent, s := getAgentnSignaler(t)

	globalAgent.Mock.Control(gojcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": true,
		"bucket":  globalAgent.bucket,
	}))

	s.PushOp(agent.GetAndLockEx(GetAndLockOptions{
		Key:      []byte("testEnhancedErrs"),
		LockTime: 10,
	}, func(res *GetAndLockResult, err error) {
		s.Wrap(func() {
			typedErr, ok := err.(*KvError)
			if !ok {
				s.Fatalf("error should be a KvError: %v", err)
			}

			if typedErr.Context == "" {
				s.Fatalf("error should have a context")
			}

			if typedErr.Description == "" {
				s.Fatalf("error should have a description")
			}

			if typedErr.Code != StatusKeyNotFound {
				s.Fatalf("status code should have been StatusKeyNotFound")
			}

			if ErrorCause(err) != ErrKeyNotFound {
				s.Fatalf("error cause should have been ErrKeyNotFound")
			}
		})
	}))
	s.Wait(0)

	globalAgent.Mock.Control(gojcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": false,
		"bucket":  globalAgent.bucket,
	}))
}
