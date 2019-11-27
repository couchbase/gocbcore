package gocbcore

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type mockRetryRequest struct {
	attempts   uint32
	identifier string
	idempotent bool
	reasons    []RetryReason
	cancelFunc func() bool
	strategy   RetryStrategy
}

func (mgr *mockRetryRequest) retryStrategy() RetryStrategy {
	return mgr.strategy
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
	for _, foundReason := range mgr.reasons {
		if foundReason == reason {
			return
		}
	}
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
		request          *mockRetryRequest
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
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: false",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: false",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
		},
		newFailFastRetryStrategy(): {
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
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: true, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "not idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
			{
				name:             "idempotent request, allowsNonIdempotentRetry: false, alwaysRetry: true",
				shouldRetry:      true,
				retryReason:      &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: true},
				request:          &mockRetryRequest{attempts: 0, idempotent: true},
				expectedAttempts: 3,
				retryReasonsLen:  1,
			},
		},
	}

	for strategy, rsTests := range tests {
		stratTyp := reflect.ValueOf(strategy).Type()
		for _, tt := range rsTests {
			t.Run(fmt.Sprintf("%s - %s", stratTyp, tt.name), func(t *testing.T) {
				// Copy it and add the strategy
				baseReq := *tt.request
				req := &baseReq
				req.strategy = strategy

				totalWaitTime := time.Duration(0)
				for {
					shouldRetry, retryTime := retryOrchMaybeRetry(req, tt.retryReason)
					if shouldRetry != tt.shouldRetry {
						t.Fatalf("Expected retried to be %v, got %v", tt.shouldRetry, shouldRetry)
					}

					// No need to retry, just break
					if !shouldRetry {
						break
					}

					waitDuration := retryTime.Sub(time.Now())
					totalWaitTime += waitDuration

					if totalWaitTime >= 50*time.Millisecond {
						break
					}
				}

				if tt.expectedAttempts != req.RetryAttempts() {
					t.Fatalf("Expected retries to be %d, was %d", tt.expectedAttempts, req.RetryAttempts())
				}

				if tt.retryReasonsLen != len(req.RetryReasons()) {
					t.Fatalf("Expected reasons to be %d, was %d", tt.retryReasonsLen, len(req.RetryReasons()))
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
