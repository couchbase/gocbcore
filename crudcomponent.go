package gocbcore

import "encoding/binary"

type crudComponent struct {
	cidMgr               *collectionIDManager
	defaultRetryStrategy RetryStrategy
	tracer               *tracerComponent
	errMapManager        *errMapManager
}

func newCRUDComponent(cidMgr *collectionIDManager, defaultRetryStrategy RetryStrategy, tracerCmpt *tracerComponent,
	errMapManager *errMapManager) *crudComponent {
	return &crudComponent{
		cidMgr:               cidMgr,
		defaultRetryStrategy: defaultRetryStrategy,
		tracer:               tracerCmpt,
		errMapManager:        errMapManager,
	}
}

func (crud *crudComponent) Get(opts GetOptions, cb GetExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}

		res := GetResult{}
		res.Value = resp.Value
		res.Flags = binary.BigEndian.Uint32(resp.Extras[0:])
		res.Cas = Cas(resp.Cas)
		res.Datatype = resp.Datatype

		// tracer.Finish()
		cb(&res, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGet,
			Datatype:     0,
			Cas:          0,
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) GetAndTouch(opts GetAndTouchOptions, cb GetAndTouchExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetAndTouchEx", opts.TraceContext)

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		tracer.Finish()
		cb(&GetAndTouchResult{
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Expiry)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGAT,
			Datatype:     0,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) GetAndLock(opts GetAndLockOptions, cb GetAndLockExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetAndLockEx", opts.TraceContext)

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		tracer.Finish()
		cb(&GetAndLockResult{
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.LockTime)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGetLocked,
			Datatype:     0,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) GetOneReplica(opts GetOneReplicaOptions, cb GetReplicaExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetOneReplicaEx", opts.TraceContext)

	if opts.ReplicaIdx <= 0 {
		tracer.Finish()
		return nil, errInvalidReplica
	}

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			cb(nil, errProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		cb(&GetReplicaResult{
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGetReplica,
			Datatype:     0,
			Cas:          0,
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		ReplicaIdx:       opts.ReplicaIdx,
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) Touch(opts TouchOptions, cb TouchExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("TouchEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&TouchResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Expiry)

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       cmdTouch,
			Datatype:     0,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) Unlock(opts UnlockOptions, cb UnlockExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("UnlockEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&UnlockResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdUnlockKey,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) Delete(opts DeleteOptions, cb DeleteExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("DeleteEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&DeleteResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if crud.cidMgr.HasDurabilityLevelStatus(durabilityLevelStatusUnsupported) {
			return nil, errFeatureNotAvailable
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       cmdDelete,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) store(opName string, opcode commandCode, opts storeOptions, cb StoreExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace(opName, opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&StoreResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if crud.cidMgr.HasDurabilityLevelStatus(durabilityLevelStatusUnsupported) {
			return nil, errFeatureNotAvailable
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Flags)
	binary.BigEndian.PutUint32(extraBuf[4:], opts.Expiry)
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       opcode,
			Datatype:     opts.Datatype,
			Cas:          uint64(opts.Cas),
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        opts.Value,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) Set(opts SetOptions, cb StoreExCallback) (PendingOp, error) {
	return crud.store("SetEx", cmdSet, storeOptions{
		Key:                    opts.Key,
		CollectionName:         opts.CollectionName,
		ScopeName:              opts.ScopeName,
		RetryStrategy:          opts.RetryStrategy,
		Value:                  opts.Value,
		Flags:                  opts.Flags,
		Datatype:               opts.Datatype,
		Cas:                    0,
		Expiry:                 opts.Expiry,
		TraceContext:           opts.TraceContext,
		DurabilityLevel:        opts.DurabilityLevel,
		DurabilityLevelTimeout: opts.DurabilityLevelTimeout,
		CollectionID:           opts.CollectionID,
	}, cb)
}

func (crud *crudComponent) Add(opts AddOptions, cb StoreExCallback) (PendingOp, error) {
	return crud.store("AddEx", cmdAdd, storeOptions{
		Key:                    opts.Key,
		CollectionName:         opts.CollectionName,
		ScopeName:              opts.ScopeName,
		RetryStrategy:          opts.RetryStrategy,
		Value:                  opts.Value,
		Flags:                  opts.Flags,
		Datatype:               opts.Datatype,
		Cas:                    0,
		Expiry:                 opts.Expiry,
		TraceContext:           opts.TraceContext,
		DurabilityLevel:        opts.DurabilityLevel,
		DurabilityLevelTimeout: opts.DurabilityLevelTimeout,
		CollectionID:           opts.CollectionID,
	}, cb)
}

func (crud *crudComponent) Replace(opts ReplaceOptions, cb StoreExCallback) (PendingOp, error) {
	return crud.store("ReplaceEx", cmdReplace, storeOptions{
		Key:                    opts.Key,
		CollectionName:         opts.CollectionName,
		ScopeName:              opts.ScopeName,
		RetryStrategy:          opts.RetryStrategy,
		Value:                  opts.Value,
		Flags:                  opts.Flags,
		Datatype:               opts.Datatype,
		Cas:                    opts.Cas,
		Expiry:                 opts.Expiry,
		TraceContext:           opts.TraceContext,
		DurabilityLevel:        opts.DurabilityLevel,
		DurabilityLevelTimeout: opts.DurabilityLevelTimeout,
		CollectionID:           opts.CollectionID,
	}, cb)
}

func (crud *crudComponent) adjoin(opName string, opcode commandCode, opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace(opName, opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&AdjoinResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if crud.cidMgr.HasDurabilityLevelStatus(durabilityLevelStatusUnsupported) {
			return nil, errFeatureNotAvailable
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       opcode,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       nil,
			Key:          opts.Key,
			Value:        opts.Value,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) Append(opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	return crud.adjoin("AppendEx", cmdAppend, opts, cb)
}

func (crud *crudComponent) Prepend(opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	return crud.adjoin("PrependEx", cmdPrepend, opts, cb)
}

func (crud *crudComponent) counter(opName string, opcode commandCode, opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace(opName, opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Value) != 8 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}
		intVal := binary.BigEndian.Uint64(resp.Value)

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&CounterResult{
			Value:         intVal,
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	// You cannot have an expiry when you do not want to create the document.
	if opts.Initial == uint64(0xFFFFFFFFFFFFFFFF) && opts.Expiry != 0 {
		return nil, errInvalidArgument
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if crud.cidMgr.HasDurabilityLevelStatus(durabilityLevelStatusUnsupported) {
			return nil, errFeatureNotAvailable
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	extraBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extraBuf[0:], opts.Delta)
	if opts.Initial != uint64(0xFFFFFFFFFFFFFFFF) {
		binary.BigEndian.PutUint64(extraBuf[8:], opts.Initial)
		binary.BigEndian.PutUint32(extraBuf[16:], opts.Expiry)
	} else {
		binary.BigEndian.PutUint64(extraBuf[8:], 0x0000000000000000)
		binary.BigEndian.PutUint32(extraBuf[16:], 0xFFFFFFFF)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       opcode,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) Increment(opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	return crud.counter("IncrementEx", cmdIncrement, opts, cb)
}

func (crud *crudComponent) Decrement(opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	return crud.counter("DecrementEx", cmdDecrement, opts, cb)
}

func (crud *crudComponent) GetRandom(opts GetRandomOptions, cb GetRandomExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetRandomEx", opts.TraceContext)

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		tracer.Finish()
		cb(&GetRandomResult{
			Key:      resp.Key,
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetRandom,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) GetMeta(opts GetMetaOptions, cb GetMetaExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetMetaEx", nil)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 21 {
			tracer.Finish()
			cb(nil, errProtocol)
			return
		}

		deleted := binary.BigEndian.Uint32(resp.Extras[0:])
		flags := binary.BigEndian.Uint32(resp.Extras[4:])
		expTime := binary.BigEndian.Uint32(resp.Extras[8:])
		seqNo := SeqNo(binary.BigEndian.Uint64(resp.Extras[12:]))
		dataType := resp.Extras[20]

		tracer.Finish()
		cb(&GetMetaResult{
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Expiry:   expTime,
			SeqNo:    seqNo,
			Datatype: dataType,
			Deleted:  deleted,
		}, nil)
	}

	extraBuf := make([]byte, 1)
	extraBuf[0] = 2

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGetMeta,
			Datatype:     0,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) SetMeta(opts SetMetaOptions, cb SetMetaExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetMetaEx", nil)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&SetMetaResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	extraBuf := make([]byte, 30+len(opts.Extra))
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Flags)
	binary.BigEndian.PutUint32(extraBuf[4:], opts.Expiry)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(opts.RevNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(opts.Cas))
	binary.BigEndian.PutUint32(extraBuf[24:], opts.Options)
	binary.BigEndian.PutUint16(extraBuf[28:], uint16(len(opts.Extra)))
	copy(extraBuf[30:], opts.Extra)

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdSetMeta,
			Datatype:     opts.Datatype,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        opts.Value,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}

func (crud *crudComponent) DeleteMeta(opts DeleteMetaOptions, cb DeleteMetaExCallback) (PendingOp, error) {
	tracer := crud.tracer.CreateOpTrace("GetMetaEx", nil)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&DeleteMetaResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	extraBuf := make([]byte, 30+len(opts.Extra))
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Flags)
	binary.BigEndian.PutUint32(extraBuf[4:], opts.Expiry)
	binary.BigEndian.PutUint64(extraBuf[8:], opts.RevNo)
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(opts.Cas))
	binary.BigEndian.PutUint32(extraBuf[24:], opts.Options)
	binary.BigEndian.PutUint16(extraBuf[28:], uint16(len(opts.Extra)))
	copy(extraBuf[30:], opts.Extra)

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdDelMeta,
			Datatype:     opts.Datatype,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        opts.Value,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return crud.cidMgr.Dispatch(req)
}
