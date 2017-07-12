package gocbcore

import (
	"net"
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

func (agent *Agent) waitAndRetryOperation(req *memdQRequest) {
	if agent.nmvRetryDelay == 0 {
		agent.requeueDirect(req)
	} else {
		time.AfterFunc(agent.nmvRetryDelay, func() {
			agent.requeueDirect(req)
		})
	}
}

func (agent *Agent) handleOpNmv(resp *memdQResponse, req *memdQRequest) {
	// Grab just the hostname from the source address
	sourceHost, _, err := net.SplitHostPort(resp.sourceAddr)
	if err != nil {
		logErrorf("NMV response source address was invalid, skipping config update")
		agent.waitAndRetryOperation(req)
		return
	}

	// Try to parse the value as a bucket configuration
	bk, err := parseConfig(resp.Value, sourceHost)
	if err == nil {
		agent.updateConfig(bk)
	}

	// Redirect it!  This may actually come back to this server, but I won't tell
	//   if you don't ;)
	agent.waitAndRetryOperation(req)
}

func (agent *Agent) handleOpRoutingResp(resp *memdQResponse, req *memdQRequest) bool {
	if resp.Magic == resMagic && resp.Status == StatusNotMyVBucket {
		agent.handleOpNmv(resp, req)
		return true
	}

	return false
}

func (agent *Agent) dispatchOp(req *memdQRequest) (PendingOp, error) {
	req.RoutingCallback = agent.handleOpRoutingResp

	err := agent.dispatchDirect(req)
	if err != nil {
		return nil, err
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
