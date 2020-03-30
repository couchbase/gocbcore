package gocbcore

import (
	"testing"
	"time"

	"github.com/couchbaselabs/gojcbmock"
)

func TestKvErrorConstantRetry(t *testing.T) {
	constant := kvErrorMapRetry{
		Strategy:    "constant",
		Interval:    1000,
		After:       2000,
		Ceil:        4000,
		MaxDuration: 3000,
	}
	if constant.CalculateRetryDelay(0) != 2000*time.Millisecond {
		t.Fatalf("failed to respect after for first retry")
	}
	if constant.CalculateRetryDelay(1) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for second retry")
	}
	if constant.CalculateRetryDelay(3) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for minimal retries")
	}
	if constant.CalculateRetryDelay(15000) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for large retry counts")
	}
}

func TestKvErrorLinearRetry(t *testing.T) {
	linear := kvErrorMapRetry{
		Strategy:    "linear",
		Interval:    1000,
		After:       2000,
		Ceil:        60000,
		MaxDuration: 100000,
	}
	if linear.CalculateRetryDelay(0) != 2000*time.Millisecond {
		t.Fatalf("failed to respect after for first retry")
	}
	if linear.CalculateRetryDelay(1) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for second retry")
	}
	if linear.CalculateRetryDelay(3) != 3000*time.Millisecond {
		t.Fatalf("should respect interval for minimal retries")
	}
	if linear.CalculateRetryDelay(150) != 60000*time.Millisecond {
		t.Fatalf("should respect ceiling for large retry counts")
	}
}

func TestKvErrorExponentialRetry(t *testing.T) {
	exponential := kvErrorMapRetry{
		Strategy:    "exponential",
		Interval:    10,
		After:       1000,
		Ceil:        60000,
		MaxDuration: 100000,
	}
	if exponential.CalculateRetryDelay(0) != 1000*time.Millisecond {
		t.Fatalf("failed to respect after for first retry")
	}
	if exponential.CalculateRetryDelay(1) != 10*time.Millisecond {
		t.Fatalf("should respect interval for second retry")
	}
	if exponential.CalculateRetryDelay(3) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for minimal retries")
	}
	if exponential.CalculateRetryDelay(400) != 60000*time.Millisecond {
		t.Fatalf("should respect ceiling for large retry counts")
	}
}

type errMapTestRetryStrategy struct {
	reasons []RetryReason
	retries int
}

func (lrs *errMapTestRetryStrategy) RetryAfter(request RetryRequest, reason RetryReason) RetryAction {
	lrs.retries++
	lrs.reasons = append(lrs.reasons, reason)
	return &WithDurationRetryAction{50 * time.Millisecond}
}

func testKvErrorMapGeneric(t *testing.T, errCode uint16) {
	testEnsureSupportsFeature(t, TestFeatureErrMap)

	agent, h := testGetAgentAndHarness(t)
	if !h.IsMockServer() {
		t.Skipf("only supported when testing against mock server")
	}

	testKey := "hello"

	agent.pollerController.Pause(true)
	defer func() {
		agent.pollerController.Pause(false)
	}()

	h.mockInst.Control(gojcbmock.NewCommand(gojcbmock.COpFail, map[string]interface{}{
		"bucket": h.BucketName,
		"code":   errCode,
		"count":  3,
	}))

	strategy := &errMapTestRetryStrategy{}
	h.PushOp(agent.Get(GetOptions{
		Key:           []byte(testKey),
		RetryStrategy: strategy,
	}, func(res *GetResult, err error) {
		h.Wrap(func() {})
	}))
	h.Wait(0)

	if strategy.retries != 3 {
		t.Fatalf("Expected retries to be 3 but was %d", strategy.retries)
	}

	if len(strategy.reasons) != 3 {
		t.Fatalf("Expected 3 retry reasons but was %v", strategy.reasons)
	}

	for _, reason := range strategy.reasons {
		if reason != KVErrMapRetryReason {
			t.Fatalf("Expected reason to be KVErrMapRetryReason but was %s", reason.Description())
		}
	}

	h.mockInst.Control(gojcbmock.NewCommand(gojcbmock.COpFail, map[string]interface{}{
		"bucket": h.BucketName,
		"code":   errCode,
		"count":  0,
	}))
}

// It doesn't actually matter what strategy the error map specifies, we just test that retries happen as the
// strategy dictates no matter what.
func TestKvErrorMap7ff0(t *testing.T) {
	testKvErrorMapGeneric(t, 0x7ff0)
}

func TestKvErrorMap7ff1(t *testing.T) {
	testKvErrorMapGeneric(t, 0x7ff1)
}

func TestKvErrorMap7ff2(t *testing.T) {
	testKvErrorMapGeneric(t, 0x7ff2)
}
