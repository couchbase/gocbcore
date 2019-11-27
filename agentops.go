package gocbcore

import (
	"encoding/json"
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

// CancellablePendingOp represents an outstanding operation within the client.
// This can be used to cancel an operation before it completes.
type CancellablePendingOp interface {
	Cancel() bool
}

// PendingOp represents an outstanding operation within the client.
// This can be used to cancel an operation before it completes.
// This can also be used to get information about the operation once
// it has completed (cancelled or successful).
type PendingOp interface {
	CancellablePendingOp

	// The following should only be accessed after completion of the op.
	RetryAttempts() uint32
	Identifier() string
	Idempotent() bool
	RetryReasons() []RetryReason
	LocalEndpoint() string
	RemoteEndpoint() string
	ConnectionID() string
}

type multiPendingOp struct {
	ops          []PendingOp
	completedOps uint32
	isIdempotent bool
}

func (mp *multiPendingOp) Cancel() bool {
	var failedCancels uint32
	for _, op := range mp.ops {
		if !op.Cancel() {
			failedCancels++
		}
	}
	return mp.CompletedOps()-failedCancels == 0
}

func (mp *multiPendingOp) CompletedOps() uint32 {
	return atomic.LoadUint32(&mp.completedOps)
}

func (mp *multiPendingOp) IncrementCompletedOps() uint32 {
	return atomic.AddUint32(&mp.completedOps, 1)
}

func (mp *multiPendingOp) RetryAttempts() uint32 {
	if len(mp.ops) == 0 {
		return 0
	}

	return mp.ops[0].RetryAttempts()
}

func (mp *multiPendingOp) Identifier() string {
	if len(mp.ops) == 0 {
		return ""
	}

	return mp.ops[0].Identifier()
}

func (mp *multiPendingOp) Idempotent() bool {
	return mp.isIdempotent
}

func (mp *multiPendingOp) RetryReasons() []RetryReason {
	if len(mp.ops) == 0 {
		return []RetryReason{}
	}

	return mp.ops[0].RetryReasons()
}

func (mp *multiPendingOp) ConnectionID() string {
	if len(mp.ops) == 0 {
		return ""
	}

	return mp.ops[0].ConnectionID()
}

func (mp *multiPendingOp) LocalEndpoint() string {
	if len(mp.ops) == 0 {
		return ""
	}

	return mp.ops[0].LocalEndpoint()
}

func (mp *multiPendingOp) RemoteEndpoint() string {
	if len(mp.ops) == 0 {
		return ""
	}

	return mp.ops[0].RemoteEndpoint()
}

func (agent *Agent) waitAndRetryOperation(req *memdQRequest, reason RetryReason) bool {
	retried := agent.retryOrchestrator.MaybeRetry(req, reason, req.RetryStrategy, func() {
		agent.requeueDirect(req, true)
	})
	return retried
}

func (agent *Agent) waitAndRetryNmv(req *memdQRequest) bool {
	return agent.waitAndRetryOperation(req, KVNotMyVBucketRetryReason)
}

func (agent *Agent) handleOpNmv(resp *memdQResponse, req *memdQRequest) bool {
	// Grab just the hostname from the source address
	sourceHost, err := hostFromHostPort(resp.sourceAddr)
	if err != nil {
		logErrorf("NMV response source address was invalid, skipping config update")
		return agent.waitAndRetryNmv(req)
	}

	// Try to parse the value as a bucket configuration
	bk, err := parseBktConfig(resp.Value, sourceHost)
	if err == nil {
		agent.updateConfig(bk)
	}

	// Redirect it!  This may actually come back to this server, but I won't tell
	//   if you don't ;)
	return agent.waitAndRetryNmv(req)
}

func (agent *Agent) handleCollectionUnknown(req *memdQRequest) bool {
	retried := agent.retryOrchestrator.MaybeRetry(req, KVCollectionOutdatedRetryReason, req.RetryStrategy, func() {
		agent.cidMgr.requeue(req)
	})
	return retried
}

func (agent *Agent) getKvErrMapData(code StatusCode) *kvErrorMapError {
	if agent.useKvErrorMaps {
		errMap := agent.kvErrorMap.Get()
		if errMap != nil {
			if errData, ok := errMap.Errors[uint16(code)]; ok {
				return &errData
			}
		}
	}
	return nil
}

func (agent *Agent) makeMemdError(code StatusCode, errMapData *kvErrorMapError, opaque uint32, ehData []byte) error {
	if code == StatusSuccess {
		return nil
	}

	if agent.useEnhancedErrors {
		var err *KvError
		if errMapData != nil {
			err = &KvError{
				Code:        code,
				Name:        errMapData.Name,
				Description: errMapData.Description,
			}
		} else {
			err = newSimpleError(code)
		}

		err.Opaque = opaque

		if ehData != nil {
			var enhancedData struct {
				Error struct {
					Context string `json:"context"`
					Ref     string `json:"ref"`
				} `json:"error"`
			}
			if parseErr := json.Unmarshal(ehData, &enhancedData); parseErr == nil {
				err.Context = enhancedData.Error.Context
				err.Ref = enhancedData.Error.Ref
			}
		}

		return err
	}

	if ok, err := findMemdError(code); ok {
		return err
	}

	if errMapData != nil {
		return KvError{
			Code:        code,
			Name:        errMapData.Name,
			Description: errMapData.Description,
		}
	}

	return newSimpleError(code)
}

func (agent *Agent) makeBasicMemdError(code StatusCode, opaque uint32) error {
	if !agent.useKvErrorMaps {
		return agent.makeMemdError(code, nil, opaque, nil)
	}

	errMapData := agent.getKvErrMapData(code)
	return agent.makeMemdError(code, errMapData, opaque, nil)
}

func (agent *Agent) handleOpRoutingResp(resp *memdQResponse, req *memdQRequest, err error) (bool, error) {
	kvErrData := agent.getKvErrMapData(resp.Status)

	if resp.Magic == resMagic {
		switch resp.Status {
		case StatusNotMyVBucket:
			retried := agent.handleOpNmv(resp, req)
			if retried {
				return true, nil
			}
		case StatusCollectionUnknown:
			// Only retry for this if it's an op supporting collections.
			// And if the collection name or scope name are specified.
			// We don't want to retry if the user specified a collection ID directly.
			hasNames := req.CollectionName != "" || req.ScopeName != ""
			if _, ok := cidSupportedOps[req.Opcode]; ok && hasNames {
				retried := agent.handleCollectionUnknown(req)
				if retried {
					return true, nil
				}
			}
		case StatusLocked:
			retried := agent.waitAndRetryOperation(req, KVLockedRetryReason)
			if retried {
				return true, nil
			}
		case StatusTmpFail:
			retried := agent.waitAndRetryOperation(req, KVTemporaryFailureRetryReason)
			if retried {
				return true, nil
			}
		case StatusSyncWriteInProgress:
			retried := agent.waitAndRetryOperation(req, KVSyncWriteInProgressRetryReason)
			if retried {
				return true, nil
			}
		case StatusSyncWriteReCommitInProgress:
			retried := agent.waitAndRetryOperation(req, KVSyncWriteRecommitInProgressRetryReason)
			if retried {
				return true, nil
			}
		case StatusSuccess:
			return false, nil
		default:
			if kvErrData != nil {
				for _, attr := range kvErrData.Attributes {
					if attr == "auto-retry" || attr == "retry-now" || attr == "retry-later" {
						retried := agent.waitAndRetryOperation(req, KVErrMapRetryReason)
						if retried {
							return true, nil
						}

						return false, err
					}
				}
			}
		}

		if DatatypeFlag(resp.Datatype)&DatatypeFlagJSON != 0 {
			err = agent.makeMemdError(resp.Status, kvErrData, resp.Opaque, resp.Value)

			if !IsErrorStatus(err, StatusSuccess) &&
				!IsErrorStatus(err, StatusKeyNotFound) &&
				!IsErrorStatus(err, StatusKeyExists) {
				logDebugf("detailed error: %+v", err)
			}
		} else {
			err = agent.makeMemdError(resp.Status, kvErrData, resp.Opaque, nil)
		}
	}

	return false, err
}

func (agent *Agent) dispatchOp(req *memdQRequest) (PendingOp, error) {
	req.owner = agent
	req.dispatchTime = time.Now()

	err := agent.cidMgr.dispatch(req)

	if err != nil {
		// Creating a goroutine here is more expensive that we'd maybe like but errors here shouldn't
		// happen often.
		go func() {
			for {
				wait := make(chan struct{})
				reason := UnknownRetryReason
				if err == ErrOverload {
					reason = PipelineOverloadedRetryReason
				}

				retried := agent.retryOrchestrator.MaybeRetry(req, reason, req.RetryStrategy, func() {
					wait <- struct{}{}
				})

				// If the request is cancelled then we don't really want to retry it.
				// The worst case here is that it does get retried and then either fails to
				// get dispatched again and is caught here or it is dispatched and gets caught
				// in the dispatch queue. Either way if the user has cancelled the request then this is
				// going to just be hanging around so we need to drop out of the loop ASAP.
				if !retried || req.isCancelled() {
					req.tryCallback(nil, err)
					return
				}

				<-wait
				err = agent.cidMgr.dispatch(req)
				if err == nil {
					return
				}
			}
		}()
	}

	return req, nil
}

func (agent *Agent) dispatchOpToAddress(req *memdQRequest, address string) (PendingOp, error) {
	req.owner = agent
	req.dispatchTime = time.Now()

	err := agent.dispatchDirectToAddress(req, address)
	if err != nil {
		// Creating a goroutine here is more expensive that we'd maybe like but errors here shouldn't
		// happen often.
		go func() {
			for {
				wait := make(chan struct{})
				reason := UnknownRetryReason
				if err == ErrOverload {
					reason = PipelineOverloadedRetryReason
				}

				retried := agent.retryOrchestrator.MaybeRetry(req, reason, req.RetryStrategy, func() {
					wait <- struct{}{}
				})

				if !retried {
					req.tryCallback(nil, err)
					return
				}

				<-wait

				// If the request is cancelled then we don't really want to retry it.
				// The worst case here is that it does get retried and then either fails to
				// get dispatched again and is caught here or it is dispatched and gets caught
				// in the dispatch queue. Either way if the user has cancelled the request then this is
				// going to just be hanging around so we need to drop out of the loop ASAP.
				if req.isCancelled() {
					req.tryCallback(nil, err)
					return
				}

				err = agent.dispatchDirectToAddress(req, address)
				if err == nil {
					return
				}
			}
		}()
	}

	return req, nil
}
