package gocbcore

import (
	"encoding/binary"
	"time"
)

func (crud *crudComponent) Observe(opts ObserveOptions, cb ObserveCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("Observe", opts.TraceContext)

	if crud.cidMgr.BucketType() != bktTypeCouchbase {
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

	vbID := crud.cidMgr.KeyToVbucket(opts.Key)
	keyLen := len(opts.Key)

	valueBuf := make([]byte, 2+2+keyLen)
	binary.BigEndian.PutUint16(valueBuf[0:], vbID)
	binary.BigEndian.PutUint16(valueBuf[2:], uint16(keyLen))
	copy(valueBuf[4:], opts.Key)

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
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

	if !opts.Deadline.IsZero() {
		req.Timer = time.AfterFunc(opts.Deadline.Sub(time.Now()), func() {
			req.cancelWithCallback(errUnambiguousTimeout)
		})
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) ObserveVb(opts ObserveVbOptions, cb ObserveVbCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("ObserveVb", nil)

	if crud.cidMgr.BucketType() != bktTypeCouchbase {
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
		opts.RetryStrategy = crud.defaultRetryStrategy
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

	if !opts.Deadline.IsZero() {
		req.Timer = time.AfterFunc(opts.Deadline.Sub(time.Now()), func() {
			req.cancelWithCallback(errUnambiguousTimeout)
		})
	}

	return crud.cidMgr.Dispatch(req)
}
