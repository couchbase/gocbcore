package gocbcore

// SubDocResult encapsulates the results from a single sub-document operation.
type SubDocResult struct {
	Err   error
	Value []byte
}

// GetInResult encapsulates the result of a GetInEx operation.
type GetInResult struct {
	Value []byte
	Cas   Cas
}

// GetInExCallback is invoked upon completion of a GetInEx operation.
type GetInExCallback func(*GetInResult, error)

// GetInEx retrieves the value at a particular path within a JSON document.
func (agent *Agent) GetInEx(opts GetInOptions, cb GetInExCallback) (PendingOp, error) {
	return agent.crudCmpt.GetIn(opts, cb)
}

// ExistsInResult encapsulates the result of a ExistsInEx operation.
type ExistsInResult struct {
	Cas Cas
}

// ExistsInExCallback is invoked upon completion of a ExistsInEx operation.
type ExistsInExCallback func(*ExistsInResult, error)

// ExistsInEx returns whether a particular path exists within a document.
func (agent *Agent) ExistsInEx(opts ExistsInOptions, cb ExistsInExCallback) (PendingOp, error) {
	return agent.crudCmpt.ExistsIn(opts, cb)
}

// StoreInResult encapsulates the result of a SetInEx, AddInEx, ReplaceInEx,
// PushFrontInEx, PushBackInEx, ArrayInsertInEx or AddUniqueInEx operation.
type StoreInResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// StoreInExCallback is invoked upon completion of a SetInEx, AddInEx,
// ReplaceInEx, PushFrontInEx, PushBackInEx, ArrayInsertInEx or
// AddUniqueInEx operation.
type StoreInExCallback func(*StoreInResult, error)

// SetInEx sets the value at a path within a document.
func (agent *Agent) SetInEx(opts StoreInOptions, cb StoreInExCallback) (PendingOp, error) {
	return agent.crudCmpt.SetIn(opts, cb)
}

// AddInEx adds a value at the path within a document.  This method
// works like SetIn, but only only succeeds if the path does not
// currently exist.
func (agent *Agent) AddInEx(opts StoreInOptions, cb StoreInExCallback) (PendingOp, error) {
	return agent.crudCmpt.AddIn(opts, cb)
}

// ReplaceInEx replaces the value at the path within a document.
// This method works like SetIn, but only only succeeds
// if the path currently exists.
func (agent *Agent) ReplaceInEx(opts StoreInOptions, cb StoreInExCallback) (PendingOp, error) {
	return agent.crudCmpt.ReplaceIn(opts, cb)
}

// PushFrontInEx pushes an entry to the front of an array at a path within a document.
func (agent *Agent) PushFrontInEx(opts StoreInOptions, cb StoreInExCallback) (PendingOp, error) {
	return agent.crudCmpt.PushFrontIn(opts, cb)
}

// PushBackInEx pushes an entry to the back of an array at a path within a document.
func (agent *Agent) PushBackInEx(opts StoreInOptions, cb StoreInExCallback) (PendingOp, error) {
	return agent.crudCmpt.PushBackIn(opts, cb)
}

// ArrayInsertInEx inserts an entry to an array at a path within the document.
func (agent *Agent) ArrayInsertInEx(opts StoreInOptions, cb StoreInExCallback) (PendingOp, error) {
	return agent.crudCmpt.ArrayInsertIn(opts, cb)
}

// AddUniqueInEx adds an entry to an array at a path but only if the value doesn't already exist in the array.
func (agent *Agent) AddUniqueInEx(opts StoreInOptions, cb StoreInExCallback) (PendingOp, error) {
	return agent.crudCmpt.AddUniqueIn(opts, cb)
}

// CounterInResult encapsulates the result of a CounterInEx operation.
type CounterInResult struct {
	Value         []byte
	Cas           Cas
	MutationToken MutationToken
}

// CounterInExCallback is invoked upon completion of a CounterInEx operation.
type CounterInExCallback func(*CounterInResult, error)

// CounterInEx performs an arithmetic add or subtract on a value at a path in the document.
func (agent *Agent) CounterInEx(opts CounterInOptions, cb CounterInExCallback) (PendingOp, error) {
	return agent.crudCmpt.CounterIn(opts, cb)
}

// DeleteInResult encapsulates the result of a DeleteInEx operation.
type DeleteInResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// DeleteInExCallback is invoked upon completion of a DeleteInEx operation.
type DeleteInExCallback func(*DeleteInResult, error)

// DeleteInEx removes the value at a path within the document.
func (agent *Agent) DeleteInEx(opts DeleteInOptions, cb DeleteInExCallback) (PendingOp, error) {
	return agent.crudCmpt.DeleteIn(opts, cb)
}

// SubDocOp defines a per-operation structure to be passed to MutateIn
// or LookupIn for performing many sub-document operations.
type SubDocOp struct {
	Op    SubDocOpType
	Flags SubdocFlag
	Path  string
	Value []byte
}

// LookupInResult encapsulates the result of a LookupInEx operation.
type LookupInResult struct {
	Cas Cas
	Ops []SubDocResult
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

// MutateInResult encapsulates the result of a MutateInEx operation.
type MutateInResult struct {
	Cas           Cas
	MutationToken MutationToken
	Ops           []SubDocResult
}

// MutateInExCallback is invoked upon completion of a MutateInEx operation.
type MutateInExCallback func(*MutateInResult, error)

// MutateInEx performs a multiple-mutation sub-document operation on a document.
func (agent *Agent) MutateInEx(opts MutateInOptions, cb MutateInExCallback) (PendingOp, error) {
	return agent.crudCmpt.MutateIn(opts, cb)
}
