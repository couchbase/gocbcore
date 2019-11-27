package gocbcore

import (
	"encoding/json"
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
// This can also be used to get information about the operation once
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
			agent.requeueDirect(req, true)
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
		bk, err := parseBktConfig(resp.Value, sourceHost)
		if err == nil {
			agent.updateConfig(bk)
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

func (agent *Agent) getKvErrMapData(code StatusCode) *kvErrorMapError {
	errMap := agent.kvErrorMap.Get()
	if errMap != nil {
		if errData, ok := errMap.Errors[uint16(code)]; ok {
			return &errData
		}
	}
	return nil
}

func (agent *Agent) enhanceKvError(err error, resp *memdQResponse, req *memdQRequest) error {
	enhErr := KeyValueError{
		InnerError: err,
	}

	if req != nil {
		enhErr.BucketName = agent.bucketName
		enhErr.ScopeName = req.ScopeName
		enhErr.CollectionName = req.CollectionName
		enhErr.CollectionID = req.CollectionID

		enhErr.RetryReasons = req.retryReasons
		enhErr.RetryAttempts = req.retryCount
	}

	if resp != nil {
		enhErr.StatusCode = resp.Status
		enhErr.Opaque = resp.Opaque

		errMapData := agent.getKvErrMapData(enhErr.StatusCode)
		if errMapData != nil {
			enhErr.ErrorName = errMapData.Name
			enhErr.ErrorDescription = errMapData.Description
		}

		if DatatypeFlag(resp.Datatype)&DatatypeFlagJSON != 0 {
			var enhancedData struct {
				Error struct {
					Context string `json:"context"`
					Ref     string `json:"ref"`
				} `json:"error"`
			}
			if parseErr := json.Unmarshal(resp.Value, &enhancedData); parseErr == nil {
				enhErr.Context = enhancedData.Error.Context
				enhErr.Ref = enhancedData.Error.Ref
			}
		}
	}

	return enhErr
}

func translateMemdError(err error, req *memdQRequest) error {
	switch err {
	case ErrMemdInvalidArgs:
		return errInvalidArgument
	case ErrMemdInternalError:
		return errInternalServerFailure
	case ErrMemdAccessError:
		return errAuthenticationFailure
	case ErrMemdAuthError:
		return errAuthenticationFailure
	case ErrMemdTmpFail:
		return errTemporaryFailure
	case ErrMemdBusy:
		return errTemporaryFailure
	case ErrMemdKeyExists:
		if req.Opcode == cmdReplace || (req.Opcode == cmdDelete && req.Cas != 0) {
			return errCasMismatch
		}
		return errDocumentExists
	case ErrMemdCollectionNotFound:
		return errCollectionNotFound
	case ErrMemdUnknownCommand:
		return errUnsupportedOperation
	case ErrMemdNotSupported:
		return errUnsupportedOperation

	case ErrMemdKeyNotFound:
		return errDocumentNotFound
	case ErrMemdLocked:
		// BUGFIX(brett19): This resolves a bug in the server processing of the LOCKED
		// operation where the server will respond with LOCKED rather than a CAS mismatch.
		if req.Opcode == cmdUnlockKey {
			return errDocumentNotFound
		}
		return errDocumentLocked
	case ErrMemdTooBig:
		return errValueTooLarge
	case ErrMemdSubDocNotJSON:
		return errValueNotJSON
	case ErrMemdDurabilityInvalidLevel:
		return errDurabilityLevelNotAvailable
	case ErrMemdDurabilityImpossible:
		return errDurabilityImpossible
	case ErrMemdSyncWriteAmbiguous:
		return errDurabilityAmbiguous
	case ErrMemdSyncWriteInProgess:
		return errDurableWriteInProgress
	case ErrMemdSyncWriteReCommitInProgress:
		return errDurableWriteReCommitInProgress
	case ErrMemdSubDocPathNotFound:
		return errPathNotFound
	case ErrMemdSubDocPathInvalid:
		return errPathInvalid
	case ErrMemdSubDocPathTooBig:
		return errPathTooBig
	case ErrMemdSubDocDocTooDeep:
		return errPathTooDeep
	case ErrMemdSubDocValueTooDeep:
		return errValueTooDeep
	case ErrMemdSubDocCantInsert:
		return errValueInvalid
	case ErrMemdSubDocNotJSON:
		return errDocumentNotJSON
	case ErrMemdSubDocBadRange:
		return errNumberTooBig
	case ErrMemdBadDelta:
		return errDeltaInvalid
	case ErrMemdSubDocBadDelta:
		return errDeltaInvalid
	case ErrMemdSubDocPathExists:
		return errPathExists
	case ErrXattrUnknownMacro:
		return errXattrUnknownMacro
	case ErrXattrInvalidFlagCombo:
		return errXattrInvalidFlagCombo
	case ErrXattrInvalidKeyCombo:
		return errXattrInvalidKeyCombo
	case ErrMemdSubDocXattrUnknownVAttr:
		return errXattrUnknownVirtualAttribute
	case ErrMemdSubDocXattrCannotModifyVAttr:
		return errXattrCannotModifyVirtualAttribute
	case ErrXattrInvalidOrder:
		return errXattrInvalidOrder
	}

	return err
}

func (agent *Agent) makeSubDocError(index int, code StatusCode, req *memdQRequest, resp *memdQResponse) error {
	err := getKvStatusCodeError(code)
	err = translateMemdError(err, req)
	err = agent.enhanceKvError(err, resp, req)
	return SubDocumentError{
		Index:      index,
		InnerError: err,
	}
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
		kvErrData := agent.getKvErrMapData(resp.Status)
		if kvErrData != nil {
			hasRetryAttr := false

			for _, attr := range kvErrData.Attributes {
				if attr == "auto-retry" || attr == "retry-now" || attr == "retry-later" {
					hasRetryAttr = true
					break
				}
			}

			if hasRetryAttr {
				if agent.waitAndRetryOperation(req, KVErrMapRetryReason) {
					return true, nil
				}
			}
		}
	}

	err = agent.enhanceKvError(err, resp, req)

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

	err := agent.dispatchDirectToAddress(req, address)
	if err != nil {
		shortCircuit, routeErr := agent.handleOpRoutingResp(nil, req, err)
		if shortCircuit {
			return req, nil
		}

		return nil, routeErr
	}

	return req, nil
}
