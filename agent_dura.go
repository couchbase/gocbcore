package gocbcore

import (
	"encoding/binary"
)

// ObserveOptions encapsulates the parameters for a ObserveEx operation.
type ObserveOptions struct {
	Key            []byte
	ReplicaIdx     int
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// ObserveResult encapsulates the result of a ObserveEx operation.
type ObserveResult struct {
	KeyState KeyState
	Cas      Cas
}

// ObserveExCallback is invoked upon completion of a ObserveEx operation.
type ObserveExCallback func(*ObserveResult, error)

// ObserveEx retrieves the current CAS and persistence state for a document.
func (agent *Agent) ObserveEx(opts ObserveOptions, cb ObserveExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("ObserveEx", opts.TraceContext)

	if agent.bucketType() != bktTypeCouchbase {
		tracer.Finish()
		return nil, errFeatureNotAvailable
	}

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Value) < 4 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}
		keyLen := int(binary.BigEndian.Uint16(resp.Value[2:]))

		if len(resp.Value) != 2+2+keyLen+1+8 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}
		keyState := KeyState(resp.Value[2+2+keyLen])
		cas := binary.BigEndian.Uint64(resp.Value[2+2+keyLen+1:])

		tracer.Finish()
		cb(&ObserveResult{
			KeyState: keyState,
			Cas:      Cas(cas),
		}, nil)
	}

	vbID := agent.KeyToVbucket(opts.Key)
	keyLen := len(opts.Key)

	valueBuf := make([]byte, 2+2+keyLen)
	binary.BigEndian.PutUint16(valueBuf[0:], vbID)
	binary.BigEndian.PutUint16(valueBuf[2:], uint16(keyLen))
	copy(valueBuf[4:], opts.Key)

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdObserve,
			Datatype:     0,
			Cas:          0,
			Extras:       nil,
			Key:          nil,
			Value:        valueBuf,
			Vbucket:      vbID,
			CollectionID: opts.CollectionID,
		},
		ReplicaIdx:       opts.ReplicaIdx,
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// ObserveVbOptions encapsulates the parameters for a ObserveVbEx operation.
type ObserveVbOptions struct {
	VbID          uint16
	VbUUID        VbUUID
	ReplicaIdx    int
	RetryStrategy RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
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
	tracer := agent.createOpTrace("ObserveVbEx", nil)

	if agent.bucketType() != bktTypeCouchbase {
		tracer.Finish()
		return nil, errFeatureNotAvailable
	}

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Value) < 1 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}

		formatType := resp.Value[0]
		if formatType == 0 {
			// Normal
			if len(resp.Value) < 27 {
				tracer.Finish()
				cb(nil, errProtocol)
				return
			}

			vbID := binary.BigEndian.Uint16(resp.Value[1:])
			vbUUID := binary.BigEndian.Uint64(resp.Value[3:])
			persistSeqNo := binary.BigEndian.Uint64(resp.Value[11:])
			currentSeqNo := binary.BigEndian.Uint64(resp.Value[19:])

			tracer.Finish()
			cb(&ObserveVbResult{
				DidFailover:  false,
				VbID:         vbID,
				VbUUID:       VbUUID(vbUUID),
				PersistSeqNo: SeqNo(persistSeqNo),
				CurrentSeqNo: SeqNo(currentSeqNo),
			}, nil)
			return
		} else if formatType == 1 {
			// Hard Failover
			if len(resp.Value) < 43 {
				cb(nil, errProtocol)
				return
			}

			vbID := binary.BigEndian.Uint16(resp.Value[1:])
			vbUUID := binary.BigEndian.Uint64(resp.Value[3:])
			persistSeqNo := binary.BigEndian.Uint64(resp.Value[11:])
			currentSeqNo := binary.BigEndian.Uint64(resp.Value[19:])
			oldVbUUID := binary.BigEndian.Uint64(resp.Value[27:])
			lastSeqNo := binary.BigEndian.Uint64(resp.Value[35:])

			tracer.Finish()
			cb(&ObserveVbResult{
				DidFailover:  true,
				VbID:         vbID,
				VbUUID:       VbUUID(vbUUID),
				PersistSeqNo: SeqNo(persistSeqNo),
				CurrentSeqNo: SeqNo(currentSeqNo),
				OldVbUUID:    VbUUID(oldVbUUID),
				LastSeqNo:    SeqNo(lastSeqNo),
			}, nil)
			return
		} else {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}
	}

	valueBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBuf[0:], uint64(opts.VbUUID))

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdObserveSeqNo,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    valueBuf,
			Vbucket:  opts.VbID,
		},
		ReplicaIdx:       opts.ReplicaIdx,
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		RetryStrategy:    opts.RetryStrategy,
	}
	return agent.dispatchOp(req)
}
