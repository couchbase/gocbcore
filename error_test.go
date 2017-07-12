package gocbcore

import "testing"

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
