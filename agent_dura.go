package gocbcore

// ObserveResult encapsulates the result of a ObserveEx operation.
type ObserveResult struct {
	KeyState KeyState
	Cas      Cas
}

// ObserveExCallback is invoked upon completion of a ObserveEx operation.
type ObserveExCallback func(*ObserveResult, error)

// ObserveEx retrieves the current CAS and persistence state for a document.
func (agent *Agent) ObserveEx(opts ObserveOptions, cb ObserveExCallback) (PendingOp, error) {
	return agent.crudCmpt.Observe(opts, cb)
}

// ObserveVbResult encapsulates the result of a ObserveVbEx operation.
type ObserveVbResult struct {
	DidFailover  bool
	VbID         uint16
	VbUUID       VbUUID
	PersistSeqNo SeqNo
	CurrentSeqNo SeqNo
	OldVbUUID    VbUUID
	LastSeqNo    SeqNo
}

// ObserveVbExCallback is invoked upon completion of a ObserveVbEx operation.
type ObserveVbExCallback func(*ObserveVbResult, error)

// ObserveVbEx retrieves the persistence state sequence numbers for a particular VBucket
// and includes additional details not included by the basic version.
func (agent *Agent) ObserveVbEx(opts ObserveVbOptions, cb ObserveVbExCallback) (PendingOp, error) {
	return agent.crudCmpt.ObserveVb(opts, cb)
}
