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
	agent, s := getAgentnSignaler(t)

	globalMock.Control(gojcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": true,
		"bucket":  globalAgent.bucket,
	}))

	agent.GetAndLock([]byte("testEnhancedErrs"), 10, func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			typedErr, ok := err.(*KvError)
			if !ok {
				t.Fatalf("error should be a KvError")
			}

			if typedErr.Context == "" {
				t.Fatalf("error should have a context")
			}

			if typedErr.Description == "" {
				t.Fatalf("error should have a description")
			}

			if typedErr.Code != StatusKeyNotFound {
				t.Fatalf("status code should have been StatusKeyNotFound")
			}

			if ErrorCause(err) != ErrKeyNotFound {
				t.Fatalf("error cause should have been ErrKeyNotFound")
			}
		})
	})
	s.Wait(0)

	globalMock.Control(gojcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": false,
		"bucket":  globalAgent.bucket,
	}))
}
