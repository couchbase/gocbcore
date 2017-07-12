package gocbcore

import (
	"encoding/binary"
)

// GetIn retrieves the value at a particular path within a JSON document.
func (agent *Agent) GetIn(key []byte, path string, flags SubdocFlag, cb GetInCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, 0, err)
			return
		}

		cb(resp.Value, Cas(resp.Cas), nil)
	}

	pathBytes := []byte(path)

	extraBuf := make([]byte, 3)
	binary.BigEndian.PutUint16(extraBuf[0:], uint16(len(pathBytes)))
	extraBuf[2] = uint8(flags)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdSubDocGet,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    pathBytes,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// ExistsIn returns whether a particular path exists within a document.
func (agent *Agent) ExistsIn(key []byte, path string, flags SubdocFlag, cb ExistsInCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(0, err)
			return
		}

		cb(Cas(resp.Cas), nil)
	}

	pathBytes := []byte(path)

	extraBuf := make([]byte, 3)
	binary.BigEndian.PutUint16(extraBuf[0:], uint16(len(pathBytes)))
	extraBuf[2] = uint8(flags)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdSubDocExists,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    pathBytes,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

func (agent *Agent) storeIn(opcode commandCode, key []byte, path string, value []byte, flags SubdocFlag, cas Cas, expiry uint32, cb StoreInCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbId = req.Vbucket
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	pathBytes := []byte(path)

	valueBuf := make([]byte, len(pathBytes)+len(value))
	copy(valueBuf[0:], pathBytes)
	copy(valueBuf[len(pathBytes):], value)

	var extraBuf []byte
	if expiry != 0 {
		extraBuf = make([]byte, 7)
	} else {
		extraBuf = make([]byte, 3)
	}
	binary.BigEndian.PutUint16(extraBuf[0:], uint16(len(pathBytes)))
	extraBuf[2] = uint8(flags)
	if len(extraBuf) >= 7 {
		binary.BigEndian.PutUint32(extraBuf[3:], expiry)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    valueBuf,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// SetIn sets the value at a path within a document.
func (agent *Agent) SetIn(key []byte, path string, value []byte, flags SubdocFlag, cas Cas, expiry uint32, cb StoreInCallback) (PendingOp, error) {
	return agent.storeIn(cmdSubDocDictSet, key, path, value, flags, cas, expiry, cb)
}

// AddIn adds a value at the path within a document.  This method
// works like SetIn, but only only succeeds if the path does not
// currently exist.
func (agent *Agent) AddIn(key []byte, path string, value []byte, flags SubdocFlag, cas Cas, expiry uint32, cb StoreInCallback) (PendingOp, error) {
	return agent.storeIn(cmdSubDocDictAdd, key, path, value, flags, cas, expiry, cb)
}

// ReplaceIn replaces the value at the path within a document.
// This method works like SetIn, but only only succeeds
// if the path currently exists.
func (agent *Agent) ReplaceIn(key []byte, path string, value []byte, cas Cas, expiry uint32, flags SubdocFlag, cb StoreInCallback) (PendingOp, error) {
	return agent.storeIn(cmdSubDocReplace, key, path, value, flags, cas, expiry, cb)
}

// PushFrontIn pushes an entry to the front of an array at a path within a document.
func (agent *Agent) PushFrontIn(key []byte, path string, value []byte, flags SubdocFlag, cas Cas, expiry uint32, cb StoreInCallback) (PendingOp, error) {
	return agent.storeIn(cmdSubDocArrayPushFirst, key, path, value, flags, cas, expiry, cb)
}

// PushBackIn pushes an entry to the back of an array at a path within a document.
func (agent *Agent) PushBackIn(key []byte, path string, value []byte, flags SubdocFlag, cas Cas, expiry uint32, cb StoreInCallback) (PendingOp, error) {
	return agent.storeIn(cmdSubDocArrayPushLast, key, path, value, flags, cas, expiry, cb)
}

// ArrayInsertIn inserts an entry to an array at a path within the document.
func (agent *Agent) ArrayInsertIn(key []byte, path string, value []byte, cas Cas, expiry uint32, flags SubdocFlag, cb StoreInCallback) (PendingOp, error) {
	return agent.storeIn(cmdSubDocArrayInsert, key, path, value, flags, cas, expiry, cb)
}

// AddUniqueIn adds an entry to an array at a path but only if the value doesn't already exist in the array.
func (agent *Agent) AddUniqueIn(key []byte, path string, value []byte, flags SubdocFlag, cas Cas, expiry uint32, cb StoreInCallback) (PendingOp, error) {
	return agent.storeIn(cmdSubDocArrayAddUnique, key, path, value, flags, cas, expiry, cb)
}

// CounterIn performs an arithmetic add or subtract on a value at a path in the document.
func (agent *Agent) CounterIn(key []byte, path string, value []byte, cas Cas, expiry uint32, flags SubdocFlag, cb CounterInCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			cb(nil, 0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbId = req.Vbucket
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(resp.Value, Cas(resp.Cas), mutToken, nil)
	}

	pathBytes := []byte(path)

	valueBuf := make([]byte, len(pathBytes)+len(value))
	copy(valueBuf[0:], pathBytes)
	copy(valueBuf[len(pathBytes):], value)

	var extraBuf []byte
	if expiry != 0 {
		extraBuf = make([]byte, 7)
	} else {
		extraBuf = make([]byte, 3)
	}
	binary.BigEndian.PutUint16(extraBuf[0:], uint16(len(pathBytes)))
	extraBuf[2] = uint8(flags)
	if len(extraBuf) >= 7 {
		binary.BigEndian.PutUint32(extraBuf[3:], expiry)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdSubDocCounter,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    valueBuf,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// RemoveIn removes the value at a path within the document.
func (agent *Agent) RemoveIn(key []byte, path string, cas Cas, expiry uint32, flags SubdocFlag, cb RemoveInCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbId = req.Vbucket
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	pathBytes := []byte(path)

	var extraBuf []byte
	if expiry != 0 {
		extraBuf = make([]byte, 7)
	} else {
		extraBuf = make([]byte, 3)
	}
	binary.BigEndian.PutUint16(extraBuf[0:], uint16(len(pathBytes)))
	extraBuf[2] = uint8(flags)
	if len(extraBuf) >= 7 {
		binary.BigEndian.PutUint32(extraBuf[3:], expiry)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdSubDocDelete,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    pathBytes,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// SubDocOp defines a per-operation structure to be passed to MutateIn
// or LookupIn for performing many sub-document operations.
type SubDocOp struct {
	Op    SubDocOpType
	Flags SubdocFlag
	Path  string
	Value []byte
}

// SubDocLookup performs a multiple-lookup sub-document operation on a document.
func (agent *Agent) SubDocLookup(key []byte, ops []SubDocOp, flags SubdocDocFlag, cb LookupInCallback) (PendingOp, error) {
	results := make([]SubDocResult, len(ops))

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil &&
			!IsErrorStatus(err, StatusSubDocMultiPathFailureDeleted) &&
			!IsErrorStatus(err, StatusSubDocSuccessDeleted) &&
			!IsErrorStatus(err, StatusSubDocBadMulti) {
			cb(nil, 0, err)
			return
		}

		respIter := 0
		for i := range results {
			if respIter+6 > len(resp.Value) {
				cb(nil, 0, ErrProtocol)
				return
			}

			resError := StatusCode(binary.BigEndian.Uint16(resp.Value[respIter+0:]))
			resValueLen := int(binary.BigEndian.Uint32(resp.Value[respIter+2:]))

			if respIter+6+resValueLen > len(resp.Value) {
				cb(nil, 0, ErrProtocol)
				return
			}

			results[i].Err = agent.makeBasicMemdError(resError)
			results[i].Value = resp.Value[respIter+6 : respIter+6+resValueLen]
			respIter += 6 + resValueLen
		}

		cb(results, Cas(resp.Cas), err)
	}

	pathBytesList := make([][]byte, len(ops))
	pathBytesTotal := 0
	for i, op := range ops {
		pathBytes := []byte(op.Path)
		pathBytesList[i] = pathBytes
		pathBytesTotal += len(pathBytes)
	}

	valueBuf := make([]byte, len(ops)*4+pathBytesTotal)

	valueIter := 0
	for i, op := range ops {
		if op.Op != SubDocOpGet && op.Op != SubDocOpExists &&
			op.Op != SubDocOpGetDoc && op.Op != SubDocOpGetCount {
			return nil, ErrInvalidArgs
		}
		if op.Value != nil {
			return nil, ErrInvalidArgs
		}

		pathBytes := pathBytesList[i]
		pathBytesLen := len(pathBytes)

		valueBuf[valueIter+0] = uint8(op.Op)
		valueBuf[valueIter+1] = uint8(op.Flags)
		binary.BigEndian.PutUint16(valueBuf[valueIter+2:], uint16(pathBytesLen))
		copy(valueBuf[valueIter+4:], pathBytes)
		valueIter += 4 + pathBytesLen
	}

	var extraBuf []byte
	if flags != 0 {
		extraBuf = append(extraBuf, uint8(flags))
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdSubDocMultiLookup,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    valueBuf,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// SubDocMutate performs a multiple-mutation sub-document operation on a document.
func (agent *Agent) SubDocMutate(key []byte, ops []SubDocOp, flags SubdocDocFlag, cas Cas, expiry uint32, cb MutateInCallback) (PendingOp, error) {
	results := make([]SubDocResult, len(ops))

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil &&
			!IsErrorStatus(err, StatusSubDocSuccessDeleted) &&
			!IsErrorStatus(err, StatusSubDocBadMulti) {
			cb(nil, 0, MutationToken{}, err)
			return
		}

		if IsErrorStatus(err, StatusSubDocBadMulti) {
			if len(resp.Value) != 3 {
				cb(nil, 0, MutationToken{}, ErrProtocol)
				return
			}

			opIndex := int(resp.Value[0])
			resError := StatusCode(binary.BigEndian.Uint16(resp.Value[1:]))

			err := SubDocMutateError{
				Err:     agent.makeBasicMemdError(resError),
				OpIndex: opIndex,
			}
			cb(nil, 0, MutationToken{}, err)
			return
		}

		for readPos := uint32(0); readPos < uint32(len(resp.Value)); {
			opIndex := int(resp.Value[readPos+0])
			opStatus := StatusCode(binary.BigEndian.Uint16(resp.Value[readPos+1:]))
			results[opIndex].Err = agent.makeBasicMemdError(opStatus)
			readPos += 3

			if opStatus == StatusSuccess {
				valLength := binary.BigEndian.Uint32(resp.Value[readPos:])
				results[opIndex].Value = resp.Value[readPos+4 : readPos+4+valLength]
				readPos += 4 + valLength
			}
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbId = req.Vbucket
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(results, Cas(resp.Cas), mutToken, nil)
	}

	pathBytesList := make([][]byte, len(ops))
	pathBytesTotal := 0
	valueBytesTotal := 0
	for i, op := range ops {
		pathBytes := []byte(op.Path)
		pathBytesList[i] = pathBytes
		pathBytesTotal += len(pathBytes)
		valueBytesTotal += len(op.Value)
	}

	valueBuf := make([]byte, len(ops)*8+pathBytesTotal+valueBytesTotal)

	valueIter := 0
	for i, op := range ops {
		if op.Op != SubDocOpDictAdd && op.Op != SubDocOpDictSet &&
			op.Op != SubDocOpDelete && op.Op != SubDocOpReplace &&
			op.Op != SubDocOpArrayPushLast && op.Op != SubDocOpArrayPushFirst &&
			op.Op != SubDocOpArrayInsert && op.Op != SubDocOpArrayAddUnique &&
			op.Op != SubDocOpCounter && op.Op != SubDocOpSetDoc &&
			op.Op != SubDocOpAddDoc && op.Op != SubDocOpDeleteDoc {
			return nil, ErrInvalidArgs
		}

		pathBytes := pathBytesList[i]
		pathBytesLen := len(pathBytes)
		valueBytesLen := len(op.Value)

		valueBuf[valueIter+0] = uint8(op.Op)
		valueBuf[valueIter+1] = uint8(op.Flags)
		binary.BigEndian.PutUint16(valueBuf[valueIter+2:], uint16(pathBytesLen))
		binary.BigEndian.PutUint32(valueBuf[valueIter+4:], uint32(valueBytesLen))
		copy(valueBuf[valueIter+8:], pathBytes)
		copy(valueBuf[valueIter+8+pathBytesLen:], op.Value)
		valueIter += 8 + pathBytesLen + valueBytesLen
	}

	var extraBuf []byte
	if expiry != 0 {
		tmpBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(tmpBuf[0:], expiry)
		extraBuf = append(extraBuf, tmpBuf...)
	}
	if flags != 0 {
		extraBuf = append(extraBuf, uint8(flags))
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdSubDocMultiMutation,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    valueBuf,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}
