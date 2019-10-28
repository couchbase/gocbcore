package gocbcore

import (
	"time"
)

// RetryRequest is a request that can possibly be retried.
type RetryRequest interface {
	RetryAttempts() uint32
	Identifier() string
	Idempotent() bool
	RetryReasons() []RetryReason

	addRetryReason(reason RetryReason)
	incrementRetryAttempts()
	setCancelRetry(cancelFunc func() bool)
}

// RetryReason represents the reason for an operation possibly being retried.
type RetryReason interface {
	AllowsNonIdempotentRetry() bool
	AlwaysRetry() bool
	Description() string
}

type retryReason struct {
	allowsNonIdempotentRetry bool
	alwaysRetry              bool
	description              string
}

func (rr *retryReason) AllowsNonIdempotentRetry() bool {
	return rr.allowsNonIdempotentRetry
}

func (rr *retryReason) AlwaysRetry() bool {
	return rr.alwaysRetry
}

func (rr *retryReason) Description() string {
	return rr.description
}

func (rr *retryReason) String() string {
	return rr.description
}

var (
	// UnknownRetryReason indicates that the operation failed for an unknown reason.
	UnknownRetryReason = &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: false, description: "UNKNOWN"}

	// SocketNotAvailableRetryReason indicates that the operation failed because the underlying socket was not available.
	SocketNotAvailableRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "SOCKET_NOT_AVAILABLE"}

	// ServiceNotAvailableRetryReason indicates that the operation failed because the requested service was not available.
	ServiceNotAvailableRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "SERVICE_NOT_AVAILABLE"}

	// NodeNotAvailableRetryReason indicates that the operation failed because the requested node was not available.
	NodeNotAvailableRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "NODE_NOT_AVAILABLE"}

	// KVNotMyVBucketRetryReason indicates that the operation failed because it was sent to the wrong node for the vbucket.
	KVNotMyVBucketRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true, description: "KV_NOT_MY_VBUCKET"}

	// KVCollectionOutdatedRetryReason indicates that the operation failed because the collection ID on the request is outdated.
	KVCollectionOutdatedRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true, description: "KV_COLLECTION_OUTDATED"}

	// KVErrMapRetryReason indicates that the operation failed for an unsupported reason but the KV error map indicated
	// that the operation can be retried.
	KVErrMapRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "KV_ERROR_MAP_RETRY_INDICATED"}

	// KVLockedRetryReason indicates that the operation failed because the document was locked.
	KVLockedRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "KV_LOCKED"}

	// KVTemporaryFailureRetryReason indicates that the operation failed because of a temporary failure.
	KVTemporaryFailureRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "KV_TEMPORARY_FAILURE"}

	// KVSyncWriteInProgressRetryReason indicates that the operation failed because a sync write is in progress.
	KVSyncWriteInProgressRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "KV_SYNC_WRITE_IN_PROGRESS"}

	// KVSyncWriteRecommitInProgressRetryReason indicates that the operation failed because a sync write recommit is in progress.
	KVSyncWriteRecommitInProgressRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS"}

	// ServiceResponseCodeIndicatedRetryReason indicates that the operation failed and the service responded stating that
	// the request should be retried.
	ServiceResponseCodeIndicatedRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "SERVICE_RESPONSE_CODE_INDICATED"}

	// SocketCloseInFlightRetryReason indicates that the operation failed because the socket was closed whilst the operation
	// was in flight.
	SocketCloseInFlightRetryReason = &retryReason{allowsNonIdempotentRetry: false, alwaysRetry: false, description: "SOCKET_CLOSED_WHILE_IN_FLIGHT"}

	// PipelineOverloadedRetryReason indicates that the operation failed because the pipeline queue was full.
	PipelineOverloadedRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: true, description: "PIPELINE_OVERLOADED"}

	// CircuitBreakerOpenRetryReason indicates that the operation failed because the circuit breaker for the underlying socket was open.
	CircuitBreakerOpenRetryReason = &retryReason{allowsNonIdempotentRetry: true, alwaysRetry: false, description: "CIRCUIT_BREAKER_OPEN"}
)

// MaybeRetryRequest will possibly retry a request according to the strategy belonging to the request.
// It will use the reason to determine whether or not the failure reason is one that can be retried.
func (agent *Agent) MaybeRetryRequest(req RetryRequest, reason RetryReason, retryStrategy RetryStrategy, retryFunc func()) bool {
	return agent.retryOrchestrator.MaybeRetry(req, reason, retryStrategy, retryFunc)
}

// RetryAction is used by a RetryStrategy to calculate the duration to wait before retrying an operation.
// Returning a value of 0 indicates to not retry.
type RetryAction interface {
	Duration() time.Duration
}

// NoRetryRetryAction represents an action that indicates to not retry.
type NoRetryRetryAction struct {
}

// Duration is the length of time to wait before retrying an operation.
func (ra *NoRetryRetryAction) Duration() time.Duration {
	return 0
}

// WithDurationRetryAction represents an action that indicates to retry with a given duration.
type WithDurationRetryAction struct {
	WithDuration time.Duration
}

// Duration is the length of time to wait before retrying an operation.
func (ra *WithDurationRetryAction) Duration() time.Duration {
	return ra.WithDuration
}

// RetryStrategy is to determine if an operation should be retried, and if so how long to wait before retrying.
type RetryStrategy interface {
	RetryAfter(req RetryRequest, reason RetryReason) RetryAction
}

// retryOrchestrator is responsible for handling operation retries.
type retryOrchestrator struct {
}

func (ro *retryOrchestrator) retry(req RetryRequest, duration time.Duration, retryFunc func()) {
	timer := AcquireTimer(duration)
	stopSignal := make(chan struct{})
	stop := func() bool {
		if !timer.Stop() {
			return false
		}
		stopSignal <- struct{}{}
		return true
	}
	req.setCancelRetry(stop)

	go func() {
		select {
		case <-timer.C:
			ReleaseTimer(timer, true)
			retryFunc()
			return
		case <-stopSignal:
			ReleaseTimer(timer, true)
			logInfof("Request retry cancelled, OperationID: %s", req.Identifier())
			return
		}
	}()
}

// MaybeRetry will possibly retry an operation according to the strategy belonging to the request.
// It will use the reason to determine whether or not the failure reason is one that can be retried.
func (ro *retryOrchestrator) MaybeRetry(req RetryRequest, reason RetryReason, retryStrategy RetryStrategy, retryFunc func()) bool {
	if reason.AlwaysRetry() {
		duration := ControlledBackoff(req.RetryAttempts())
		logInfof("Will retry request. Backoff=%s, OperationID=%s. Reason=%s", duration, req.Identifier(), reason)
		ro.retry(req, duration, retryFunc)
		req.addRetryReason(reason)
		req.incrementRetryAttempts()

		return true
	}

	if retryStrategy == nil {
		return false
	}

	action := retryStrategy.RetryAfter(req, reason)
	if action == nil {
		logInfof("Won't retry request.  OperationID=%s. Reason=%s", req.Identifier(), reason)
		return false
	}

	duration := action.Duration()
	if duration == 0 {
		logInfof("Won't retry request.  OperationID=%s. Reason=%s", req.Identifier(), reason)
		return false
	}

	logInfof("Will retry request. Backoff=%s, OperationID=%s. Reason=%s", duration, req.Identifier(), reason)
	ro.retry(req, duration, retryFunc)
	req.addRetryReason(reason)
	req.incrementRetryAttempts()

	return true
}

// FailFastRetryStrategy represents a strategy that will never retry.
type FailFastRetryStrategy struct {
}

// NewFailFastRetryStrategy returns a new FailFastRetryStrategy.
func NewFailFastRetryStrategy() *FailFastRetryStrategy {
	return &FailFastRetryStrategy{}
}

// RetryAfter calculates and returns a RetryAction describing how long to wait before retrying an operation.
func (rs *FailFastRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	return &NoRetryRetryAction{}
}

// BestEffortRetryStrategy represents a strategy that will keep retrying until it succeeds (or the caller times out
// the request).
type BestEffortRetryStrategy struct {
	backoffCalculator func(retryAttempts uint32) time.Duration
}

// NewBestEffortRetryStrategy returns a new BestEffortRetryStrategy which will use the supplied calculator function
// to calculate retry durations. If calculator is nil then ControlledBackoff will be used.
func NewBestEffortRetryStrategy(calculator func(retryAttempts uint32) time.Duration) *BestEffortRetryStrategy {
	if calculator == nil {
		calculator = ControlledBackoff
	}

	return &BestEffortRetryStrategy{backoffCalculator: calculator}
}

// RetryAfter calculates and returns a RetryAction describing how long to wait before retrying an operation.
func (rs *BestEffortRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	if req.Idempotent() || reason.AllowsNonIdempotentRetry() {
		return &WithDurationRetryAction{WithDuration: rs.backoffCalculator(req.RetryAttempts())}
	}

	return &NoRetryRetryAction{}
}

// ControlledBackoff calculates a backoff time duration from the retry attempts on a given request.
func ControlledBackoff(retryAttempts uint32) time.Duration {
	switch retryAttempts {
	case 0:
		return 1 * time.Millisecond
	case 1:
		return 10 * time.Millisecond
	case 2:
		return 50 * time.Millisecond
	case 3:
		return 100 * time.Millisecond
	case 4:
		return 500 * time.Millisecond
	default:
		return 1000 * time.Millisecond
	}
}

var idempotentOps = map[commandCode]bool{
	cmdGet:                    true,
	cmdGetReplica:             true,
	cmdGetMeta:                true,
	cmdSubDocGet:              true,
	cmdSubDocExists:           true,
	cmdSubDocGetCount:         true,
	cmdNoop:                   true,
	cmdStat:                   true,
	cmdGetRandom:              true,
	cmdCollectionsGetID:       true,
	cmdCollectionsGetManifest: true,
	cmdGetClusterConfig:       true,
	cmdObserve:                true,
	cmdObserveSeqNo:           true,
}