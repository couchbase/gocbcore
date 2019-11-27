package gocbcore

import (
	"errors"
	"testing"

	"github.com/couchbaselabs/gojcbmock"
)

func TestEnhancedErrors(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	var err1, err2 error

	s.PushOp(agent.GetEx(GetOptions{
		Key: []byte("keyThatWontExist"),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			err1 = err
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetEx(GetOptions{
		Key: []byte("keyThatWontExist"),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			err2 = err
		})
	}))
	s.Wait(0)

	if err1 == err2 {
		t.Fatalf("Operation error results should never return equivalent values")
	}
}

func TestEnhancedErrorOp(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureErrMap)

	agent, h := testGetAgentAndHarness(t)
	if !h.IsMockServer() {
		t.Skipf("only supported when testing against mock server")
	}

	h.mockInst.Control(gojcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": true,
		"bucket":  h.BucketName,
	}))

	h.PushOp(agent.GetAndLockEx(GetAndLockOptions{
		Key:      []byte("testEnhancedErrs"),
		LockTime: 10,
	}, func(res *GetAndLockResult, err error) {
		h.Wrap(func() {
			typedErr, ok := err.(*KeyValueError)
			if !ok {
				h.Fatalf("error should be a KvError: %v", err)
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

			if typedErr.StatusCode != StatusKeyNotFound {
				h.Fatalf("status code should have been StatusKeyNotFound")
			}

			if !errors.Is(err, ErrDocumentNotFound) {
				h.Fatalf("error cause should have been ErrDocumentNotFound")
			}
		})
	}))
	h.Wait(0)

	h.mockInst.Control(gojcbmock.NewCommand("SET_ENHANCED_ERRORS", map[string]interface{}{
		"enabled": false,
		"bucket":  h.BucketName,
	}))
}
