package gocbcore

import (
	"testing"
	"time"

	"gopkg.in/couchbaselabs/gojcbmock.v1"
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

func testKvErrorMapGeneric(t *testing.T, errCode uint16) {
	if !globalAgent.SupportsFeature(TestErrMapFeature) {
		t.Skip("Cannot test error map with real server")
	}
	agent, s := getAgentnSignaler(t)

	testKey := "hello"
	serverIdx := globalAgent.KeyToServer([]byte(testKey), 0)

	globalAgent.Mock.Control(gojcbmock.NewCommand(gojcbmock.COpFail, map[string]interface{}{
		"bucket": globalAgent.bucket,
		"code":   errCode,
		"count":  -1,
	}))
	globalAgent.Mock.Control(gojcbmock.NewCommand(gojcbmock.CStartRetryVerify, map[string]interface{}{
		"idx":    serverIdx,
		"bucket": globalAgent.bucket,
	}))

	s.PushOp(agent.GetEx(GetOptions{
		Key: []byte(testKey),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {})
	}))
	s.Wait(0)

	resp := globalAgent.Mock.Control(gojcbmock.NewCommand(gojcbmock.CCheckRetryVerify, map[string]interface{}{
		"idx":     serverIdx,
		"bucket":  globalAgent.bucket,
		"opcode":  0,
		"errcode": errCode,
		"fuzz_ms": 20,
	}))
	if !resp.Success() {
		t.Fatalf("Failed to verify retry intervals")
	}

	globalAgent.Mock.Control(gojcbmock.NewCommand(gojcbmock.COpFail, map[string]interface{}{
		"bucket": globalAgent.bucket,
		"code":   errCode,
		"count":  0,
	}))
}

func TestKvErrorMap7ff0(t *testing.T) {
	testKvErrorMapGeneric(t, 0x7ff0)
}

func TestKvErrorMap7ff1(t *testing.T) {
	testKvErrorMapGeneric(t, 0x7ff1)
}

func TestKvErrorMap7ff2(t *testing.T) {
	testKvErrorMapGeneric(t, 0x7ff2)
}
