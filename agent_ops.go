package gocbcore

// GetExCallback is invoked upon completion of a GetEx operation.
type GetExCallback func(*GetResult, error)

// GetEx retrieves a document.
func (agent *Agent) GetEx(opts GetOptions, cb GetExCallback) (PendingOp, error) {
	return agent.crudCmpt.Get(opts, cb)
}

// GetAndTouchExCallback is invoked upon completion of a GetAndTouchEx operation.
type GetAndTouchExCallback func(*GetAndTouchResult, error)

// GetAndTouchEx retrieves a document and updates its expiry.
func (agent *Agent) GetAndTouchEx(opts GetAndTouchOptions, cb GetAndTouchExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetAndTouch(opts, cb)
}

// GetAndLockExCallback is invoked upon completion of a GetAndLockEx operation.
type GetAndLockExCallback func(*GetAndLockResult, error)

// GetAndLockEx retrieves a document and locks it.
func (agent *Agent) GetAndLockEx(opts GetAndLockOptions, cb GetAndLockExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetAndLock(opts, cb)
}

// GetReplicaExCallback is invoked upon completion of a GetReplica operation.
type GetReplicaExCallback func(*GetReplicaResult, error)

// GetOneReplicaEx retrieves a document from a replica server.
func (agent *Agent) GetOneReplicaEx(opts GetOneReplicaOptions, cb GetReplicaExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetOneReplica(opts, cb)
}

// TouchExCallback is invoked upon completion of a TouchEx operation.
type TouchExCallback func(*TouchResult, error)

// TouchEx updates the expiry for a document.
func (agent *Agent) TouchEx(opts TouchOptions, cb TouchExCallback) (PendingOp, error) {
	return agent.crudCmpt.Touch(opts, cb)
}

// UnlockExCallback is invoked upon completion of a UnlockEx operation.
type UnlockExCallback func(*UnlockResult, error)

// UnlockEx unlocks a locked document.
func (agent *Agent) UnlockEx(opts UnlockOptions, cb UnlockExCallback) (PendingOp, error) {
	return agent.crudCmpt.Unlock(opts, cb)
}

// DeleteExCallback is invoked upon completion of a DeleteEx operation.
type DeleteExCallback func(*DeleteResult, error)

// DeleteEx removes a document.
func (agent *Agent) DeleteEx(opts DeleteOptions, cb DeleteExCallback) (PendingOp, error) {
	return agent.crudCmpt.Delete(opts, cb)
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

// GetRandomExCallback is invoked upon completion of a GetRandomEx operation.
type GetRandomExCallback func(*GetRandomResult, error)

// GetRandomEx retrieves the key and value of a random document stored within Couchbase Server.
func (agent *Agent) GetRandomEx(opts GetRandomOptions, cb GetRandomExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetRandom(opts, cb)
}

// GetMetaExCallback is invoked upon completion of a GetMetaEx operation.
type GetMetaExCallback func(*GetMetaResult, error)

// GetMetaEx retrieves a document along with some internal Couchbase meta-data.
func (agent *Agent) GetMetaEx(opts GetMetaOptions, cb GetMetaExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetMeta(opts, cb)
}

// SetMetaExCallback is invoked upon completion of a SetMetaEx operation.
type SetMetaExCallback func(*SetMetaResult, error)

// SetMetaEx stores a document along with setting some internal Couchbase meta-data.
func (agent *Agent) SetMetaEx(opts SetMetaOptions, cb SetMetaExCallback) (PendingOp, error) {
	return agent.crudCmpt.SetMeta(opts, cb)
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

// ObserveExCallback is invoked upon completion of a ObserveEx operation.
type ObserveExCallback func(*ObserveResult, error)

// ObserveEx retrieves the current CAS and persistence state for a document.
func (agent *Agent) ObserveEx(opts ObserveOptions, cb ObserveExCallback) (PendingOp, error) {
	return agent.crudCmpt.Observe(opts, cb)
}

// ObserveVbExCallback is invoked upon completion of a ObserveVbEx operation.
type ObserveVbExCallback func(*ObserveVbResult, error)

// ObserveVbEx retrieves the persistence state sequence numbers for a particular VBucket
// and includes additional details not included by the basic version.
func (agent *Agent) ObserveVbEx(opts ObserveVbOptions, cb ObserveVbExCallback) (PendingOp, error) {
	return agent.crudCmpt.ObserveVb(opts, cb)
}

// SubDocOp defines a per-operation structure to be passed to MutateIn
// or LookupIn for performing many sub-document operations.
type SubDocOp struct {
	Op    SubDocOpType
	Flags SubdocFlag
	Path  string
	Value []byte
}

// LookupInExCallback is invoked upon completion of a LookupInEx operation.
type LookupInExCallback func(*LookupInResult, error)

type subdocOpList struct {
	ops     []SubDocOp
	indexes []int
}

func (sol *subdocOpList) Prepend(op SubDocOp, index int) {
	sol.ops = append([]SubDocOp{op}, sol.ops...)
	sol.indexes = append([]int{index}, sol.indexes...)
}

func (sol *subdocOpList) Append(op SubDocOp, index int) {
	sol.ops = append(sol.ops, op)
	sol.indexes = append(sol.indexes, index)
}

// LookupInEx performs a multiple-lookup sub-document operation on a document.
func (agent *Agent) LookupInEx(opts LookupInOptions, cb LookupInExCallback) (PendingOp, error) {
	return agent.crudCmpt.LookupIn(opts, cb)
}

// MutateInExCallback is invoked upon completion of a MutateInEx operation.
type MutateInExCallback func(*MutateInResult, error)

// MutateInEx performs a multiple-mutation sub-document operation on a document.
func (agent *Agent) MutateInEx(opts MutateInOptions, cb MutateInExCallback) (PendingOp, error) {
	return agent.crudCmpt.MutateIn(opts, cb)
}

// N1QLQuery executes a N1QL query
func (agent *Agent) N1QLQuery(opts N1QLQueryOptions) (*N1QLRowReader, error) {
	return agent.n1qlCmpt.N1QLQuery(opts)
}

// PreparedN1QLQuery executes a prepared N1QL query
func (agent *Agent) PreparedN1QLQuery(opts N1QLQueryOptions) (*N1QLRowReader, error) {
	return agent.n1qlCmpt.PreparedN1QLQuery(opts)
}

// AnalyticsQuery executes an analytics query
func (agent *Agent) AnalyticsQuery(opts AnalyticsQueryOptions) (*AnalyticsRowReader, error) {
	return agent.analyticsCmpt.AnalyticsQuery(opts)
}

// SearchQuery executes a Search query
func (agent *Agent) SearchQuery(opts SearchQueryOptions) (*SearchRowReader, error) {
	return agent.searchCmpt.SearchQuery(opts)
}

// ViewQuery executes a view query
func (agent *Agent) ViewQuery(opts ViewQueryOptions) (*ViewQueryRowReader, error) {
	return agent.viewCmpt.ViewQuery(opts)
}

// DoHTTPRequest will perform an HTTP request against one of the HTTP
// services which are available within the SDK.
func (agent *Agent) DoHTTPRequest(req *HTTPRequest) (*HTTPResponse, error) {
	return agent.httpComponent.DoHTTPRequest(req)
}

// ManifestCallback is invoked upon completion of a GetCollectionManifest operation.
type ManifestCallback func(manifest []byte, err error)

// GetCollectionManifest fetches the current server manifest. This function will not update the client's collection
// id cache.
func (agent *Agent) GetCollectionManifest(opts GetCollectionManifestOptions, cb ManifestCallback) (PendingOp, error) {
	return agent.cidMgr.GetCollectionManifest(opts, cb)
}

// CollectionIDCallback is invoked upon completion of a GetCollectionID operation.
type CollectionIDCallback func(manifestID uint64, collectionID uint32, err error)

// GetCollectionID fetches the collection id and manifest id that the collection belongs to, given a scope name
// and collection name. This function will also prime the client's collection id cache.
func (agent *Agent) GetCollectionID(scopeName string, collectionName string, opts GetCollectionIDOptions, cb CollectionIDCallback) (PendingOp, error) {
	return agent.cidMgr.GetCollectionID(scopeName, collectionName, opts, cb)
}

// PingKvExCallback is invoked upon completion of a PingKvEx operation.
type PingKvExCallback func(*PingKvResult, error)

// PingKvEx pings all of the servers we are connected to and returns
// a report regarding the pings that were performed.
func (agent *Agent) PingKvEx(opts PingKvOptions, cb PingKvExCallback) (PendingOp, error) {
	return agent.diagnosticsCmpt.PingKvEx(opts, cb)
}

// Diagnostics returns diagnostics information about the client.
// Mainly containing a list of open connections and their current
// states.
func (agent *Agent) Diagnostics() (*DiagnosticInfo, error) {
	return agent.diagnosticsCmpt.Diagnostics()
}
