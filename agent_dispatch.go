package gocbcore

import (
	"errors"
	"io"
	"sync/atomic"
	"time"
)

// Cas represents a unique revision of a document.  This can be used
// to perform optimistic locking.
type Cas uint64

// VbUUID represents a unique identifier for a particular vbucket history.
type VbUUID uint64

// SeqNo is a sequential mutation number indicating the order and precise
// position of a write that has occurred.
type SeqNo uint64

// MutationToken represents a particular mutation within the cluster.
type MutationToken struct {
	VbID   uint16
	VbUUID VbUUID
	SeqNo  SeqNo
}

// PendingOp represents an outstanding operation within the client.
// This can be used to cancel an operation before it completes.
// This can also be used to Get information about the operation once
// it has completed (cancelled or successful).
type PendingOp interface {
	Cancel(err error)
}

type multiPendingOp struct {
	ops          []PendingOp
	completedOps uint32
	isIdempotent bool
}

func (mp *multiPendingOp) Cancel(err error) {
	for _, op := range mp.ops {
		op.Cancel(err)
	}
}

func (mp *multiPendingOp) CompletedOps() uint32 {
	return atomic.LoadUint32(&mp.completedOps)
}

func (mp *multiPendingOp) IncrementCompletedOps() uint32 {
	return atomic.AddUint32(&mp.completedOps, 1)
}

func (agent *Agent) waitAndRetryOperation(req *memdQRequest, reason RetryReason) bool {
	shouldRetry, retryTime := retryOrchMaybeRetry(req, reason)
	if shouldRetry {
		go func() {
			time.Sleep(retryTime.Sub(time.Now()))
			agent.kvMux.RequeueDirect(req, true)
		}()
		return true
	}

	return false
}

func (agent *Agent) handleNotMyVbucket(resp *memdQResponse, req *memdQRequest) bool {
	// Grab just the hostname from the source address
	sourceHost, err := hostFromHostPort(resp.sourceAddr)
	if err != nil {
		logErrorf("NMV response source address was invalid, skipping config update")
	} else {
		// Try to parse the value as a bucket configuration
		bk, err := parseConfig(resp.Value, sourceHost)
		if err == nil {
			agent.cfgManager.OnNewConfig(bk)
		}
	}

	// Redirect it!  This may actually come back to this server, but I won't tell
	//   if you don't ;)
	return agent.waitAndRetryOperation(req, KVNotMyVBucketRetryReason)
}

func (agent *Agent) handleCollectionUnknown(req *memdQRequest) bool {
	// We cannot retry requests with no collection information
	if req.CollectionName == "" && req.ScopeName == "" {
		return false
	}

	shouldRetry, retryTime := retryOrchMaybeRetry(req, KVCollectionOutdatedRetryReason)
	if shouldRetry {
		go func() {
			time.Sleep(retryTime.Sub(time.Now()))
			agent.cidMgr.requeue(req)
		}()
	}

	return false
}

func (agent *Agent) handleOpRoutingResp(resp *memdQResponse, req *memdQRequest, err error) (bool, error) {
	// If there is no error, we should return immediately
	if err == nil {
		return false, nil
	}

	// If this operation has been cancelled, we just fail immediately.
	if errors.Is(err, ErrRequestCanceled) || errors.Is(err, ErrTimeout) {
		return false, err
	}

	err = translateMemdError(err, req)

	// Handle potentially retrying the operation
	if resp != nil && resp.Status == StatusNotMyVBucket {
		if agent.handleNotMyVbucket(resp, req) {
			return true, nil
		}
	} else if resp != nil && resp.Status == StatusCollectionUnknown {
		if agent.handleCollectionUnknown(req) {
			return true, nil
		}
	} else if errors.Is(err, ErrDocumentLocked) {
		if agent.waitAndRetryOperation(req, KVLockedRetryReason) {
			return true, nil
		}
	} else if errors.Is(err, ErrTemporaryFailure) {
		if agent.waitAndRetryOperation(req, KVTemporaryFailureRetryReason) {
			return true, nil
		}
	} else if errors.Is(err, ErrDurableWriteInProgress) {
		if agent.waitAndRetryOperation(req, KVSyncWriteInProgressRetryReason) {
			return true, nil
		}
	} else if errors.Is(err, ErrDurableWriteReCommitInProgress) {
		if agent.waitAndRetryOperation(req, KVSyncWriteRecommitInProgressRetryReason) {
			return true, nil
		}
	} else if errors.Is(err, io.EOF) {
		if agent.waitAndRetryOperation(req, SocketNotAvailableRetryReason) {
			return true, nil
		}
	}

	if resp != nil && resp.Magic == resMagic {
		shouldRetry := agent.errMapManager.ShouldRetry(resp.Status)
		if shouldRetry {
			if agent.waitAndRetryOperation(req, KVErrMapRetryReason) {
				return true, nil
			}
		}
	}

	err = agent.errMapManager.EnhanceKvError(err, resp, req)

	return false, err
}

func (agent *Agent) dispatchOp(req *memdQRequest) (PendingOp, error) {
	req.owner = agent
	req.dispatchTime = time.Now()

	err := agent.cidMgr.dispatch(req)
	if err != nil {
		shortCircuit, routeErr := agent.handleOpRoutingResp(nil, req, err)
		if shortCircuit {
			return req, nil
		}

		return nil, routeErr
	}

	return req, nil
}

func (agent *Agent) dispatchOpToAddress(req *memdQRequest, address string) (PendingOp, error) {
	req.owner = agent
	req.dispatchTime = time.Now()

	err := agent.kvMux.DispatchDirectToAddress(req, address)
	if err != nil {
		shortCircuit, routeErr := agent.handleOpRoutingResp(nil, req, err)
		if shortCircuit {
			return req, nil
		}

		return nil, routeErr
	}

	return req, nil
}
