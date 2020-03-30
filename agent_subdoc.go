package gocbcore

// SubDocResult encapsulates the results from a single sub-document operation.
type SubDocResult struct {
	Err   error
	Value []byte
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
