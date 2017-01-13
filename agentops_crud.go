package gocbcore

import (
	"encoding/binary"
	"sync/atomic"
)

// Retrieves a document.
func (agent *Agent) Get(key []byte, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Retrieves a document and updates its expiry.
func (agent *Agent) GetAndTouch(key []byte, expiry uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], expiry)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdGAT,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Retrieves a document and locks it.
func (agent *Agent) GetAndLock(key []byte, lockTime uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], lockTime)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdGetLocked,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

func (agent *Agent) getOneReplica(key []byte, replicaIdx int, cb GetCallback) (PendingOp, error) {
	if replicaIdx <= 0 {
		panic("Replica number must be greater than 0")
	}

	handler := func(resp *memdPacket, req *memdPacket, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdGetReplica,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback:   handler,
		ReplicaIdx: replicaIdx,
	}
	return agent.dispatchOp(req)
}

func (agent *Agent) getAnyReplica(key []byte, cb GetCallback) (PendingOp, error) {
	opRes := &multiPendingOp{}

	var cbCalled uint32
	handler := func(value []byte, flags uint32, cas Cas, err error) {
		if atomic.CompareAndSwapUint32(&cbCalled, 0, 1) {
			// Cancel all other commands if possible.
			opRes.Cancel()
			// Dispatch Callback
			cb(value, flags, cas, err)
		}
	}

	// Dispatch a getReplica for each replica server
	numReplicas := agent.NumReplicas()
	for repIdx := 1; repIdx <= numReplicas; repIdx++ {
		op, err := agent.getOneReplica(key, repIdx, handler)
		if err == nil {
			opRes.ops = append(opRes.ops, op)
		}
	}

	// If we have no pending ops, no requests were successful
	if len(opRes.ops) == 0 {
		return nil, ErrNoReplicas
	}

	return opRes, nil
}

// Retrieves a document from a replica server.
func (agent *Agent) GetReplica(key []byte, replicaIdx int, cb GetCallback) (PendingOp, error) {
	if replicaIdx > 0 {
		return agent.getOneReplica(key, replicaIdx, cb)
	} else if replicaIdx == 0 {
		return agent.getAnyReplica(key, cb)
	} else {
		panic("Replica number must not be less than 0.")
	}
}

// Touches a document, updating its expiry.
func (agent *Agent) Touch(key []byte, cas Cas, expiry uint32, cb TouchCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbId = agent.KeyToVbucket(key)
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], expiry)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdTouch,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Unlocks a locked document.
func (agent *Agent) Unlock(key []byte, cas Cas, cb UnlockCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
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

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdUnlockKey,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Removes a document.
func (agent *Agent) Remove(key []byte, cas Cas, cb RemoveCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
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

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdDelete,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

func (agent *Agent) store(opcode CommandCode, key, value []byte, flags uint32, cas Cas, expiry uint32, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
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

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    value,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Stores a document as long as it does not already exist.
func (agent *Agent) Add(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return agent.store(CmdAdd, key, value, flags, 0, expiry, cb)
}

// Stores a document.
func (agent *Agent) Set(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return agent.store(CmdSet, key, value, flags, 0, expiry, cb)
}

// Replaces the value of a Couchbase document with another value.
func (agent *Agent) Replace(key, value []byte, flags uint32, cas Cas, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return agent.store(CmdReplace, key, value, flags, cas, expiry, cb)
}

// Performs an adjoin operation.
func (agent *Agent) adjoin(opcode CommandCode, key, value []byte, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
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

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      key,
			Value:    value,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Appends some bytes to a document.
func (agent *Agent) Append(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return agent.adjoin(CmdAppend, key, value, cb)
}

// Prepends some bytes to a document.
func (agent *Agent) Prepend(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return agent.adjoin(CmdPrepend, key, value, cb)
}

// Performs a counter operation.
func (agent *Agent) counter(opcode CommandCode, key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, req *memdPacket, err error) {
		if err != nil {
			cb(0, 0, MutationToken{}, err)
			return
		}

		if len(resp.Value) != 8 {
			cb(0, 0, MutationToken{}, ErrProtocol)
			return
		}
		intVal := binary.BigEndian.Uint64(resp.Value)

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbId = req.Vbucket
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(intVal, Cas(resp.Cas), mutToken, nil)
	}

	// You cannot have an expiry when you do not want to create the document.
	if initial == uint64(0xFFFFFFFFFFFFFFFF) && expiry != 0 {
		return nil, ErrInvalidArgs
	}

	extraBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extraBuf[0:], delta)
	if initial != uint64(0xFFFFFFFFFFFFFFFF) {
		binary.BigEndian.PutUint64(extraBuf[8:], initial)
		binary.BigEndian.PutUint32(extraBuf[16:], expiry)
	} else {
		binary.BigEndian.PutUint64(extraBuf[8:], 0x0000000000000000)
		binary.BigEndian.PutUint32(extraBuf[16:], 0xFFFFFFFF)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Increments the unsigned integer value in a document.
func (agent *Agent) Increment(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return agent.counter(CmdIncrement, key, delta, initial, expiry, cb)
}

// Decrements the unsigned integer value in a document.
func (agent *Agent) Decrement(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return agent.counter(CmdDecrement, key, delta, initial, expiry, cb)
}

// *VOLATILE*
// Returns the key and value of a random document stored within Couchbase Server.
func (agent *Agent) GetRandom(cb GetRandomCallback) (PendingOp, error) {
	handler := func(resp *memdPacket, _ *memdPacket, err error) {
		if err != nil {
			cb(nil, nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Key, resp.Value, flags, Cas(resp.Cas), nil)
	}
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    ReqMagic,
			Opcode:   CmdGetRandom,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

func (agent *Agent) Stats(key string, callback ServerStatsCallback) (PendingOp, error) {
	config := agent.routingInfo.get()
	allOk := true
	// Iterate over each of the configs

	op := new(struct {
		multiPendingOp
		remaining int32
	})
	op.remaining = int32(len(config.servers))

	stats := make(map[string]SingleServerStats)

	defer func() {
		if !allOk {
			op.Cancel()
		}
	}()

	for index, server := range config.servers {
		var req *memdQRequest
		serverName := server.address

		handler := func(resp *memdPacket, _ *memdPacket, err error) {
			// No stat key!
			curStats, ok := stats[serverName]

			if !ok {
				stats[serverName] = SingleServerStats{
					Stats: make(map[string]string),
				}
				curStats = stats[serverName]
			}
			if err != nil {
				if curStats.Error == nil {
					curStats.Error = err
				} else {
					logDebugf("Got additional error for stats: %s: %v", serverName, err)
				}
			}

			if len(resp.Key) == 0 {
				// No more request for server!
				req.Cancel()

				remaining := atomic.AddInt32(&op.remaining, -1)
				if remaining == 0 {
					callback(stats)
				}
			} else {
				curStats.Stats[string(resp.Key)] = string(resp.Value)
			}
		}

		// Send the request
		req = &memdQRequest{
			memdPacket: memdPacket{
				Magic:    ReqMagic,
				Opcode:   CmdStat,
				Datatype: 0,
				Cas:      0,
				Key:      []byte(key),
				Value:    nil,
			},
			Persistent: true,
			ReplicaIdx: (-1) + (-index),
			Callback:   handler,
		}

		curOp, err := agent.dispatchOp(req)
		if err != nil {
			return nil, err
		}
		op.ops = append(op.ops, curOp)
	}
	allOk = true
	return op, nil
}
