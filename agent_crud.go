package gocbcore

import (
	"sync"
)

// GetOptions encapsulates the parameters for a GetEx operation.
type GetOptions struct {
	Key            []byte
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetResult encapsulates the result of a GetEx operation.
type GetResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetExCallback is invoked upon completion of a GetEx operation.
type GetExCallback func(*GetResult, error)

// GetEx retrieves a document.
func (agent *Agent) GetEx(opts GetOptions, cb GetExCallback) (PendingOp, error) {
	return agent.crudCmpt.Get(opts, cb)
}

// GetAndTouchOptions encapsulates the parameters for a GetAndTouchEx operation.
type GetAndTouchOptions struct {
	Key            []byte
	Expiry         uint32
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetAndTouchResult encapsulates the result of a GetAndTouchEx operation.
type GetAndTouchResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetAndTouchExCallback is invoked upon completion of a GetAndTouchEx operation.
type GetAndTouchExCallback func(*GetAndTouchResult, error)

// GetAndTouchEx retrieves a document and updates its expiry.
func (agent *Agent) GetAndTouchEx(opts GetAndTouchOptions, cb GetAndTouchExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetAndTouch(opts, cb)
}

// GetAndLockOptions encapsulates the parameters for a GetAndLockEx operation.
type GetAndLockOptions struct {
	Key            []byte
	LockTime       uint32
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetAndLockResult encapsulates the result of a GetAndLockEx operation.
type GetAndLockResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetAndLockExCallback is invoked upon completion of a GetAndLockEx operation.
type GetAndLockExCallback func(*GetAndLockResult, error)

// GetAndLockEx retrieves a document and locks it.
func (agent *Agent) GetAndLockEx(opts GetAndLockOptions, cb GetAndLockExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetAndLock(opts, cb)
}

// GetAnyReplicaOptions encapsulates the parameters for a GetAnyReplicaEx operation.
type GetAnyReplicaOptions struct {
	Key            []byte
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetOneReplicaOptions encapsulates the parameters for a GetOneReplicaEx operation.
type GetOneReplicaOptions struct {
	Key            []byte
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy
	ReplicaIdx     int

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetReplicaResult encapsulates the result of a GetReplica operation.
type GetReplicaResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetReplicaExCallback is invoked upon completion of a GetReplica operation.
type GetReplicaExCallback func(*GetReplicaResult, error)

// GetOneReplicaEx retrieves a document from a replica server.
func (agent *Agent) GetOneReplicaEx(opts GetOneReplicaOptions, cb GetReplicaExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetOneReplica(opts, cb)
}

// TouchOptions encapsulates the parameters for a TouchEx operation.
type TouchOptions struct {
	Key            []byte
	Expiry         uint32
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// TouchResult encapsulates the result of a TouchEx operation.
type TouchResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// TouchExCallback is invoked upon completion of a TouchEx operation.
type TouchExCallback func(*TouchResult, error)

// TouchEx updates the expiry for a document.
func (agent *Agent) TouchEx(opts TouchOptions, cb TouchExCallback) (PendingOp, error) {
	return agent.crudCmpt.Touch(opts, cb)
}

// UnlockOptions encapsulates the parameters for a UnlockEx operation.
type UnlockOptions struct {
	Key            []byte
	Cas            Cas
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// UnlockResult encapsulates the result of a UnlockEx operation.
type UnlockResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// UnlockExCallback is invoked upon completion of a UnlockEx operation.
type UnlockExCallback func(*UnlockResult, error)

// UnlockEx unlocks a locked document.
func (agent *Agent) UnlockEx(opts UnlockOptions, cb UnlockExCallback) (PendingOp, error) {
	return agent.crudCmpt.Unlock(opts, cb)
}

// DeleteOptions encapsulates the parameters for a DeleteEx operation.
type DeleteOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Cas                    Cas
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// DeleteResult encapsulates the result of a DeleteEx operation.
type DeleteResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// DeleteExCallback is invoked upon completion of a DeleteEx operation.
type DeleteExCallback func(*DeleteResult, error)

// DeleteEx removes a document.
func (agent *Agent) DeleteEx(opts DeleteOptions, cb DeleteExCallback) (PendingOp, error) {
	return agent.crudCmpt.Delete(opts, cb)
}

// StoreResult encapsulates the result of a AddEx, SetEx or ReplaceEx operation.
type StoreResult struct {
	Cas           Cas
	MutationToken MutationToken
}

type storeOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Cas                    Cas
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// StoreExCallback is invoked upon completion of a AddEx, SetEx or ReplaceEx operation.
type StoreExCallback func(*StoreResult, error)

// AddOptions encapsulates the parameters for a AddEx operation.
type AddOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// AddEx stores a document as long as it does not already exist.
func (agent *Agent) AddEx(opts AddOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.crudCmpt.Add(opts, cb)
}

// SetOptions encapsulates the parameters for a SetEx operation.
type SetOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// SetEx stores a document.
func (agent *Agent) SetEx(opts SetOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.crudCmpt.Set(opts, cb)
}

// ReplaceOptions encapsulates the parameters for a ReplaceEx operation.
type ReplaceOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Cas                    Cas
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// ReplaceEx replaces the value of a Couchbase document with another value.
func (agent *Agent) ReplaceEx(opts ReplaceOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.crudCmpt.Replace(opts, cb)
}

// AdjoinOptions encapsulates the parameters for a AppendEx or PrependEx operation.
type AdjoinOptions struct {
	Key                    []byte
	Value                  []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Cas                    Cas
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// AdjoinResult encapsulates the result of a AppendEx or PrependEx operation.
type AdjoinResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// AdjoinExCallback is invoked upon completion of a AppendEx or PrependEx operation.
type AdjoinExCallback func(*AdjoinResult, error)

// AppendEx appends some bytes to a document.
func (agent *Agent) AppendEx(opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	return agent.crudCmpt.Append(opts, cb)
}

// PrependEx prepends some bytes to a document.
func (agent *Agent) PrependEx(opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	return agent.crudCmpt.Prepend(opts, cb)
}

// CounterOptions encapsulates the parameters for a IncrementEx or DecrementEx operation.
type CounterOptions struct {
	Key                    []byte
	Delta                  uint64
	Initial                uint64
	Expiry                 uint32
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Cas                    Cas
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// CounterResult encapsulates the result of a IncrementEx or DecrementEx operation.
type CounterResult struct {
	Value         uint64
	Cas           Cas
	MutationToken MutationToken
}

// CounterExCallback is invoked upon completion of a IncrementEx or DecrementEx operation.
type CounterExCallback func(*CounterResult, error)

// IncrementEx increments the unsigned integer value in a document.
func (agent *Agent) IncrementEx(opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	return agent.crudCmpt.Increment(opts, cb)
}

// DecrementEx decrements the unsigned integer value in a document.
func (agent *Agent) DecrementEx(opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	return agent.crudCmpt.Decrement(opts, cb)
}

// GetRandomOptions encapsulates the parameters for a GetRandomEx operation.
type GetRandomOptions struct {
	RetryStrategy RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetRandomResult encapsulates the result of a GetRandomEx operation.
type GetRandomResult struct {
	Key      []byte
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetRandomExCallback is invoked upon completion of a GetRandomEx operation.
type GetRandomExCallback func(*GetRandomResult, error)

// GetRandomEx retrieves the key and value of a random document stored within Couchbase Server.
func (agent *Agent) GetRandomEx(opts GetRandomOptions, cb GetRandomExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetRandom(opts, cb)
}

// GetMetaOptions encapsulates the parameters for a GetMetaEx operation.
type GetMetaOptions struct {
	Key            []byte
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetMetaResult encapsulates the result of a GetMetaEx operation.
type GetMetaResult struct {
	Value    []byte
	Flags    uint32
	Cas      Cas
	Expiry   uint32
	SeqNo    SeqNo
	Datatype uint8
	Deleted  uint32
}

// GetMetaExCallback is invoked upon completion of a GetMetaEx operation.
type GetMetaExCallback func(*GetMetaResult, error)

// GetMetaEx retrieves a document along with some internal Couchbase meta-data.
func (agent *Agent) GetMetaEx(opts GetMetaOptions, cb GetMetaExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetMeta(opts, cb)
}

// SetMetaOptions encapsulates the parameters for a SetMetaEx operation.
type SetMetaOptions struct {
	Key            []byte
	Value          []byte
	Extra          []byte
	Datatype       uint8
	Options        uint32
	Flags          uint32
	Expiry         uint32
	Cas            Cas
	RevNo          uint64
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// SetMetaResult encapsulates the result of a SetMetaEx operation.
type SetMetaResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// SetMetaExCallback is invoked upon completion of a SetMetaEx operation.
type SetMetaExCallback func(*SetMetaResult, error)

// SetMetaEx stores a document along with setting some internal Couchbase meta-data.
func (agent *Agent) SetMetaEx(opts SetMetaOptions, cb SetMetaExCallback) (PendingOp, error) {
	return agent.crudCmpt.SetMeta(opts, cb)
}

// DeleteMetaOptions encapsulates the parameters for a DeleteMetaEx operation.
type DeleteMetaOptions struct {
	Key            []byte
	Value          []byte
	Extra          []byte
	Datatype       uint8
	Options        uint32
	Flags          uint32
	Expiry         uint32
	Cas            Cas
	RevNo          uint64
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// DeleteMetaResult encapsulates the result of a DeleteMetaEx operation.
type DeleteMetaResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// DeleteMetaExCallback is invoked upon completion of a DeleteMetaEx operation.
type DeleteMetaExCallback func(*DeleteMetaResult, error)

// DeleteMetaEx deletes a document along with setting some internal Couchbase meta-data.
func (agent *Agent) DeleteMetaEx(opts DeleteMetaOptions, cb DeleteMetaExCallback) (PendingOp, error) {
	return agent.crudCmpt.DeleteMeta(opts, cb)
}

// SingleServerStats represents the stats returned from a single server.
type SingleServerStats struct {
	Stats map[string]string
	Error error
}

// StatsTarget is used for providing a specific target to the StatsEx operation.
type StatsTarget interface {
}

// VBucketIDStatsTarget indicates that a specific vbucket should be targeted by the StatsEx operation.
type VBucketIDStatsTarget struct {
	VbID uint16
}

// StatsOptions encapsulates the parameters for a StatsEx operation.
type StatsOptions struct {
	Key string
	// Target indicates that something specific should be targeted by the operation. If left nil
	// then the stats command will be sent to all servers.
	Target        StatsTarget
	RetryStrategy RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// StatsResult encapsulates the result of a StatsEx operation.
type StatsResult struct {
	Servers map[string]SingleServerStats
}

// StatsExCallback is invoked upon completion of a StatsEx operation.
type StatsExCallback func(*StatsResult, error)

// StatsEx retrieves statistics information from the server.  Note that as this
// function is an aggregator across numerous servers, there are no guarantees
// about the consistency of the results.  Occasionally, some nodes may not be
// represented in the results, or there may be conflicting information between
// multiple nodes (a vbucket active on two separate nodes at once).
func (agent *Agent) StatsEx(opts StatsOptions, cb StatsExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("StatsEx", opts.TraceContext)

	muxer := agent.kvMux.GetState()
	if muxer == nil {
		tracer.Finish()
		return nil, errShutdown
	}

	stats := make(map[string]SingleServerStats)
	var statsLock sync.Mutex

	op := new(multiPendingOp)
	op.isIdempotent = true
	var expected uint32

	pipelines := make([]*memdPipeline, 0)

	switch target := opts.Target.(type) {
	case nil:
		expected = uint32(muxer.NumPipelines())

		for i := 0; i < muxer.NumPipelines(); i++ {
			pipelines = append(pipelines, muxer.GetPipeline(i))
		}
	case VBucketIDStatsTarget:
		expected = 1

		srvIdx, err := muxer.vbMap.NodeByVbucket(target.VbID, 0)
		if err != nil {
			return nil, err
		}

		pipelines = append(pipelines, muxer.GetPipeline(srvIdx))
	default:
		return nil, errInvalidArgument
	}

	opHandledLocked := func() {
		completed := op.IncrementCompletedOps()
		if expected-completed == 0 {
			tracer.Finish()
			cb(&StatsResult{
				Servers: stats,
			}, nil)
		}
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	for _, pipeline := range pipelines {
		serverAddress := pipeline.Address()

		handler := func(resp *memdQResponse, req *memdQRequest, err error) {
			statsLock.Lock()
			defer statsLock.Unlock()

			// Fetch the specific stats key for this server.  Creating a new entry
			// for the server if we did not previously have one.
			curStats, ok := stats[serverAddress]
			if !ok {
				stats[serverAddress] = SingleServerStats{
					Stats: make(map[string]string),
				}
				curStats = stats[serverAddress]
			}

			if err != nil {
				// Store the first (and hopefully only) error into the Error field of this
				// server's stats entry.
				if curStats.Error == nil {
					curStats.Error = err
				} else {
					logDebugf("Got additional error for stats: %s: %v", serverAddress, err)
				}

				// When an error occurs, we need to cancel our persistent op.  However, because
				// a previous error may already have cancelled this and then raced, we should
				// ensure only a single completion is counted.
				if req.internalCancel(err) {
					opHandledLocked()
				}

				return
			}

			// Check if the key length is zero.  This indicates that we have reached
			// the ending of the stats listing by this server.
			if len(resp.Key) == 0 {
				// As this is a persistent request, we must manually cancel it to remove
				// it from the pending ops list.  To ensure we do not race multiple cancels,
				// we only handle it as completed the one time cancellation succeeds.
				if req.internalCancel(err) {
					opHandledLocked()
				}

				return
			}

			// Add the stat for this server to the list of stats.
			curStats.Stats[string(resp.Key)] = string(resp.Value)
		}

		req := &memdQRequest{
			memdPacket: memdPacket{
				Magic:    reqMagic,
				Opcode:   cmdStat,
				Datatype: 0,
				Cas:      0,
				Key:      []byte(opts.Key),
				Value:    nil,
			},
			Persistent:       true,
			Callback:         handler,
			RootTraceContext: tracer.RootContext(),
			RetryStrategy:    opts.RetryStrategy,
		}

		curOp, err := agent.kvMux.DispatchDirectToAddress(req, serverAddress)
		if err != nil {
			statsLock.Lock()
			stats[serverAddress] = SingleServerStats{
				Error: err,
			}
			opHandledLocked()
			statsLock.Unlock()

			continue
		}

		op.ops = append(op.ops, curOp)
	}

	return op, nil
}
