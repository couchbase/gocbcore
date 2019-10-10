package gocbcore

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type mockRetryRequest struct {
	attempts   uint32
	identifier string
	idempotent bool
	reasons    []RetryReason
	cancelFunc func() bool
}

func (mgr *mockRetryRequest) RetryAttempts() uint32 {
	return mgr.attempts
}

func (mgr *mockRetryRequest) incrementRetryAttempts() {
	mgr.attempts++
}

func (mgr *mockRetryRequest) Identifier() string {
	return mgr.identifier
}

func (mgr *mockRetryRequest) Idempotent() bool {
	return mgr.idempotent
}

func (mgr *mockRetryRequest) RetryReasons() []RetryReason {
	return mgr.reasons
}

func (mgr *mockRetryRequest) addRetryReason(reason RetryReason) {
	mgr.reasons = append(mgr.reasons, reason)
}

func (mgr *mockRetryRequest) setCancelRetry(cancelFunc func() bool) {
	mgr.cancelFunc = cancelFunc
}

type mockRetryStrategy struct {
	retried bool
	action  RetryAction
}

func (mrs *mockRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	mrs.retried = true
	return mrs.action
}

func mockBackoffCalculator(retryAttempts uint32) time.Duration {
	return time.Millisecond * time.Duration(retryAttempts)
}

func TestRetryOrchestrator(t *testing.T) {
	type test struct {
		name             string
		shouldRetry      bool
		retryReason      RetryReason
		request          RetryRequest
		expectedAttempts uint32
		retryReasonsLen  int
	}
	tests := map[RetryStrategy][]test{
		NewBestEffortRetryStrategy(nil): {
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: false",
				shouldRetry:      false,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 0,
				retryReasonsLen:  0,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: false",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: false",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: false",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
		},
		NewFailFastRetryStrategy(): {
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: false",
				shouldRetry:      false,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 0,
				retryReasonsLen:  0,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: false",
				shouldRetry:      false,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 0,
				retryReasonsLen:  0,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: false",
				shouldRetry:      false,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 0,
				retryReasonsLen:  0,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: false",
				shouldRetry:      false,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 0,
				retryReasonsLen:  0,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 1,
				retryReasonsLen:  1,
			},
		},
	}
	orch := &retryOrchestrator{}
	for strategy, rsTests := range tests {
		stratTyp := reflect.ValueOf(strategy).Type()
		for _, tt := range rsTests {
			t.Run(fmt.Sprintf("%s - %s", stratTyp, tt.name), func(t *testing.T) {
				waitCh := make(chan struct{})
				triggered := uint32(0)
				retried := orch.MaybeRetry(tt.request, tt.retryReason, strategy, func() {
					if atomic.LoadUint32(&triggered) == 0 {
						waitCh <- struct{}{}
					}
				})
				if retried != tt.shouldRetry {
					t.Fatalf("Expected retried to be %v, got %v", tt.shouldRetry, retried)
				}

				timer := time.NewTimer(50 * time.Millisecond)
				select {
				case <-timer.C:
					if tt.shouldRetry {
						atomic.StoreUint32(&triggered, 1)
						t.Fatalf("Timed out waiting for retry")
					}
				case <-waitCh:
					if !tt.shouldRetry {
						t.Fatalf("Should not have retried")
					}
				}

				if tt.expectedAttempts != tt.request.RetryAttempts() {
					t.Fatalf("Expected retries to be %d, was %d", tt.expectedAttempts, tt.request.RetryAttempts())
				}

				if tt.retryReasonsLen != len(tt.request.RetryReasons()) {
					t.Fatalf("Expected retries to be %d, was %d", tt.retryReasonsLen, len(tt.request.RetryReasons()))
				}
			})
		}
	}
}

type cancellationRetryStrategy struct {
}

func (crs *cancellationRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	return &WithDurationRetryAction{WithDuration: 50 * time.Millisecond}
}

func TestRetryOrchestratorCancelRequest(t *testing.T) {
	reason := &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true}
	orch := &retryOrchestrator{}
	req := &mockRetryRequest{idempotent: true}
	retriedCh := make(chan struct{})
	retried := orch.MaybeRetry(req, reason, &cancellationRetryStrategy{}, func() {
		retriedCh <- struct{}{}
	})
	if !retried {
		t.Fatalf("Expected request to be added to retries")
	}

	if req.cancelFunc == nil {
		t.Fatalf("Expected request cancel func to be not nil")
	}

	cancelled := req.cancelFunc()
	if !cancelled {
		t.Fatalf("Expected request to successfully cancel")
	}

	select {
	case <-time.After(60 * time.Millisecond):
	case <-retriedCh:
		t.Fatalf("Expected request to not be retried")
	}
}

func TestControlledBackoff(t *testing.T) {
	type test struct {
		attempts        uint32
		expectedBackoff time.Duration
	}
	tests := []test{
		{
			attempts:        0,
			expectedBackoff: 1 * time.Millisecond,
		},
		{
			attempts:        1,
			expectedBackoff: 10 * time.Millisecond,
		},
		{
			attempts:        2,
			expectedBackoff: 50 * time.Millisecond,
		},
		{
			attempts:        3,
			expectedBackoff: 100 * time.Millisecond,
		},
		{
			attempts:        4,
			expectedBackoff: 500 * time.Millisecond,
		},
		{
			attempts:        5,
			expectedBackoff: 1000 * time.Millisecond,
		},
		{
			attempts:        6,
			expectedBackoff: 1000 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		backoff := ControlledBackoff(tt.attempts)
		if backoff != tt.expectedBackoff {
			t.Fatalf("Expected backoff to be %s but was %s", tt.expectedBackoff.String(), backoff.String())
		}
	}
}
