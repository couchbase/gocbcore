package gocbcore

import (
	"encoding/json"
	"time"
)

// Cas represents a unique revision of a document.  This can be used
// to perform optimistic locking.
type Cas uint64

// VbUuid represents a unique identifier for a particular vbucket history.
type VbUuid uint64

// SeqNo is a sequential mutation number indicating the order and precise
// position of a write that has occurred.
type SeqNo uint64

// MutationToken represents a particular mutation within the cluster.
type MutationToken struct {
	VbId   uint16
	VbUuid VbUuid
	SeqNo  SeqNo
}

// SingleServerStats represents the stats returned from a single server.
type SingleServerStats struct {
	Stats map[string]string
	Error error
}

// ObserveSeqNoStats represents the stats returned from an observe operation.
type ObserveSeqNoStats struct {
	DidFailover  bool
	VbId         uint16
	VbUuid       VbUuid
	PersistSeqNo SeqNo
	CurrentSeqNo SeqNo
	OldVbUuid    VbUuid
	LastSeqNo    SeqNo
}

// SubDocResult encapsulates the results from a single subdocument operation.
type SubDocResult struct {
	Err   error
	Value []byte
}

// PendingOp represents an outstanding operation within the client.
// This can be used to cancel an operation before it completes.
type PendingOp interface {
	Cancel() bool
}

type multiPendingOp struct {
	ops []PendingOp
}

func (mp *multiPendingOp) Cancel() bool {
	allCancelled := true
	for _, op := range mp.ops {
		if !op.Cancel() {
			allCancelled = false
		}
	}
	return allCancelled
}

func (agent *Agent) waitAndRetryOperation(req *memdQRequest, waitDura time.Duration) {
	if waitDura == 0 {
		agent.requeueDirect(req)
	} else {
		time.AfterFunc(waitDura, func() {
			agent.requeueDirect(req)
		})
	}
}

func (agent *Agent) waitAndRetryNmv(req *memdQRequest) {
	agent.waitAndRetryOperation(req, agent.nmvRetryDelay)
}

func (agent *Agent) handleOpNmv(resp *memdQResponse, req *memdQRequest) {
	// Grab just the hostname from the source address
	sourceHost, err := hostFromHostPort(resp.sourceAddr)
	if err != nil {
		logErrorf("NMV response source address was invalid, skipping config update")
		agent.waitAndRetryNmv(req)
		return
	}

	// Try to parse the value as a bucket configuration
	bk, err := parseConfig(resp.Value, sourceHost)
	if err == nil {
		agent.updateConfig(bk)
	}

	// Redirect it!  This may actually come back to this server, but I won't tell
	//   if you don't ;)
	agent.waitAndRetryNmv(req)
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

func (agent *Agent) makeMemdError(code StatusCode, errMapData *kvErrorMapError, ehData []byte) error {
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

func (agent *Agent) makeBasicMemdError(code StatusCode) error {
	if !agent.useKvErrorMaps {
		return agent.makeMemdError(code, nil, nil)
	}

	errMapData := agent.getKvErrMapData(code)
	return agent.makeMemdError(code, errMapData, nil)
}

func (agent *Agent) handleOpRoutingResp(resp *memdQResponse, req *memdQRequest) (bool, error) {
	var err error

	if resp.Magic == resMagic {
		// Temporary backwards compatibility handling...
		if resp.Status == StatusLocked {
			switch req.Opcode {
			case cmdSet:
				resp.Status = StatusKeyExists
			case cmdReplace:
				resp.Status = StatusKeyExists
			case cmdDelete:
				resp.Status = StatusKeyExists
			default:
				resp.Status = StatusTmpFail
			}
		}

		if resp.Status == StatusNotMyVBucket {
			agent.handleOpNmv(resp, req)
			return true, nil
		} else if resp.Status == StatusSuccess {
			return false, nil
		}

		kvErrData := agent.getKvErrMapData(resp.Status)
		if kvErrData != nil {
			for _, attr := range kvErrData.Attributes {
				if attr == "auto-retry" {
					retryWait := kvErrData.Retry.CalculateRetryDelay(req.retryCount)
					maxDura := time.Duration(kvErrData.Retry.MaxDuration) * time.Millisecond
					if time.Now().Sub(req.dispatchTime)+retryWait > maxDura {
						break
					}

					req.retryCount++
					agent.waitAndRetryOperation(req, retryWait)
					return true, nil
				}
			}
		}

		if DatatypeFlag(resp.Datatype)&DatatypeFlagJson != 0 {
			err = agent.makeMemdError(resp.Status, kvErrData, resp.Value)

			if !IsErrorStatus(err, StatusSuccess) &&
				!IsErrorStatus(err, StatusKeyNotFound) &&
				!IsErrorStatus(err, StatusKeyExists) {
				logDebugf("detailed error: %+v", err)
			}
		} else {
			err = agent.makeMemdError(resp.Status, kvErrData, nil)
		}
	}

	return false, err
}

func (agent *Agent) dispatchOp(req *memdQRequest) (PendingOp, error) {
	req.RoutingCallback = agent.handleOpRoutingResp
	req.dispatchTime = time.Now()

	err := agent.dispatchDirect(req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (agent *Agent) dispatchOpToAddress(req *memdQRequest, address string) (PendingOp, error) {
	req.RoutingCallback = agent.handleOpRoutingResp
	req.dispatchTime = time.Now()

	// We set the ReplicaIdx to a negative number to ensure it is not redispatched
	// and we check that it was 0 to begin with to ensure it wasn't miss-used.
	if req.ReplicaIdx != 0 {
		return nil, ErrInvalidReplica
	}
	req.ReplicaIdx = -999999999

	for {
		routingInfo := agent.routingInfo.Get()
		if routingInfo == nil {
			return nil, ErrShutdown
		}

		var foundPipeline *memdPipeline
		for _, pipeline := range routingInfo.clientMux.pipelines {
			if pipeline.Address() == address {
				foundPipeline = pipeline
				break
			}
		}

		if foundPipeline == nil {
			return nil, ErrInvalidServer
		}

		err := foundPipeline.SendRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err == errPipelineFull {
			return nil, ErrOverload
		} else if err != nil {
			return nil, err
		}

		break
	}

	return req, nil
}

// GetCallback is invoked with the results of `Get` operations.
type GetCallback func([]byte, uint32, Cas, error)

// UnlockCallback is invoked with the results of `Unlock` operations.
type UnlockCallback func(Cas, MutationToken, error)

// TouchCallback is invoked with the results of `Touch` operations.
type TouchCallback func(Cas, MutationToken, error)

// RemoveCallback is invoked with the results of `Remove` operations.
type RemoveCallback func(Cas, MutationToken, error)

// StoreCallback is invoked with the results of any basic storage operations.
type StoreCallback func(Cas, MutationToken, error)

// CounterCallback is invoked with the results of `Counter` operations.
type CounterCallback func(uint64, Cas, MutationToken, error)

// ObserveCallback is invoked with the results of `Observe` operations.
type ObserveCallback func(KeyState, Cas, error)

// ObserveSeqNoCallback is invoked with the results of `ObserveSeqNo` operations.
type ObserveSeqNoCallback func(SeqNo, SeqNo, error)

// ObserveSeqNoExCallback is invoked with the results of `ObserveSeqNoEx` operations.
type ObserveSeqNoExCallback func(*ObserveSeqNoStats, error)

// GetRandomCallback is invoked with the results of `GetRandom` operations.
type GetRandomCallback func([]byte, []byte, uint32, Cas, error)

// ServerStatsCallback is invoked with the results of `Stats` operations.
type ServerStatsCallback func(stats map[string]SingleServerStats)

// GetInCallback is invoked with the results of `GetIn` operations.
type GetInCallback func([]byte, Cas, error)

// ExistsInCallback is invoked with the results of `ExistsIn` operations.
type ExistsInCallback func(Cas, error)

// RemoveInCallback is invoked with the results of `RemoveIn` operations.
type RemoveInCallback func(Cas, MutationToken, error)

// StoreInCallback is invoked with the results of any sub-document storage operations.
type StoreInCallback func(Cas, MutationToken, error)

// CounterInCallback is invoked with the results of `CounterIn` operations.
type CounterInCallback func([]byte, Cas, MutationToken, error)

// LookupInCallback is invoked with the results of `LookupIn` operations.
type LookupInCallback func([]SubDocResult, Cas, error)

// MutateInCallback is invoked with the results of `MutateIn` operations.
type MutateInCallback func([]SubDocResult, Cas, MutationToken, error)

// GetMetaCallback is invoked with the results of `GetMeta` operations.
type GetMetaCallback func([]byte, uint32, Cas, uint32, SeqNo, uint8, uint32, error)
