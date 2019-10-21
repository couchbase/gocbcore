package gocbcore

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestLazyCircuitBreakerSuccessfulCanary(t *testing.T) {
	var canarySent int32
	var breaker *lazyCircuitBreaker
	breaker = newLazyCircuitBreaker(CircuitBreakerConfig{
		VolumeThreshold:          4,
		ErrorThresholdPercentage: 60,
		SleepWindow:              10 * time.Millisecond,
		RollingWindow:            70 * time.Millisecond,
	}, func() {
		atomic.StoreInt32(&canarySent, 1)
		breaker.MarkSuccessful()
	})

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkSuccessful()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkSuccessful()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should not have allowed request")
	}

	// Give time for the sleep window to expire
	time.Sleep(20 * time.Millisecond)

	if breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should not have allowed request")
	}

	// Give time for the canary to be sent
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&canarySent) != 1 {
		t.Fatalf("Circuit breaker should have sent canary")
	}

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	// Give time for rolling window to reset.
	time.Sleep(100 * time.Millisecond)
	breaker.MarkSuccessful()

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}
}

func TestLazyCircuitBreakerFailedCanary(t *testing.T) {
	var canarySent int32
	var breaker *lazyCircuitBreaker
	breaker = newLazyCircuitBreaker(CircuitBreakerConfig{
		VolumeThreshold:          4,
		ErrorThresholdPercentage: 60,
		SleepWindow:              10 * time.Millisecond,
		RollingWindow:            70 * time.Millisecond,
	}, func() {
		atomic.StoreInt32(&canarySent, 1)
		breaker.MarkFailure()
	})

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkSuccessful()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkSuccessful()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should not have allowed request")
	}

	// Give time for the sleep window to expire.
	time.Sleep(20 * time.Millisecond)

	if breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should not have allowed request")
	}

	// Give time for the canary to be sent.
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&canarySent) != 1 {
		t.Fatalf("Circuit breaker should not have sent canary")
	}

	if breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should not have allowed request")
	}

	// Give time for rolling window to reset.
	time.Sleep(100 * time.Millisecond)
	breaker.MarkSuccessful()

	if breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should not have allowed request")
	}
}

func TestLazyCircuitBreakerReset(t *testing.T) {
	var canarySent int32
	var breaker *lazyCircuitBreaker
	breaker = newLazyCircuitBreaker(CircuitBreakerConfig{
		VolumeThreshold:          4,
		ErrorThresholdPercentage: 60,
		SleepWindow:              10 * time.Millisecond,
		RollingWindow:            1 * time.Second,
	}, func() {
		atomic.StoreInt32(&canarySent, 1)
		breaker.MarkFailure()
	})

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	breaker.MarkFailure()
	if breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should not have allowed request")
	}

	breaker.Reset()

	// Give time for the sleep window to expire, in this case we expect things to have been reset
	// so nothing should have happened.
	time.Sleep(20 * time.Millisecond)

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	// Give time for the canary to be sent
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&canarySent) != 0 {
		t.Fatalf("Circuit breaker should not have sent canary")
	}

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}

	// Give time for rolling window to reset.
	time.Sleep(100 * time.Millisecond)
	breaker.MarkSuccessful()

	if !breaker.AllowsRequest() {
		t.Fatalf("Circuit breaker should have allowed request")
	}
}
