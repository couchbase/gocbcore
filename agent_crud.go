package gocbcore

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

// StoreExCallback is invoked upon completion of a AddEx, SetEx or ReplaceEx operation.
type StoreExCallback func(*StoreResult, error)

// AddEx stores a document as long as it does not already exist.
func (agent *Agent) AddEx(opts AddOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.crudCmpt.Add(opts, cb)
}

// SetEx stores a document.
func (agent *Agent) SetEx(opts SetOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.crudCmpt.Set(opts, cb)
}

// ReplaceEx replaces the value of a Couchbase document with another value.
func (agent *Agent) ReplaceEx(opts ReplaceOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.crudCmpt.Replace(opts, cb)
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

// StatsEx retrieves statistics information from the server.  Note that as this
// function is an aggregator across numerous servers, there are no guarantees
// about the consistency of the results.  Occasionally, some nodes may not be
// represented in the results, or there may be conflicting information between
// multiple nodes (a vbucket active on two separate nodes at once).
func (agent *Agent) StatsEx(opts StatsOptions, cb StatsExCallback) (PendingOp, error) {
	return agent.statsCmpt.Stats(opts, cb)
}
