package gocbcore

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// Get retrieves a document.
func (agent *Agent) Get(key []byte, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGet,
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

// GetAndTouch retrieves a document and updates its expiry.
func (agent *Agent) GetAndTouch(key []byte, expiry uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
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
			Magic:    reqMagic,
			Opcode:   cmdGAT,
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

// GetAndLock retrieves a document and locks it.
func (agent *Agent) GetAndLock(key []byte, lockTime uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
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
			Magic:    reqMagic,
			Opcode:   cmdGetLocked,
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
		return nil, ErrInvalidReplica
	}

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetReplica,
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

	// We use a lock here to guard from concurrent modification by
	//  operation completion cancellation and op dispatch / insertion.
	var lock sync.Mutex

	// 0/1 depending on whether a result was received.
	var cbCalled uint32
	handler := func(value []byte, flags uint32, cas Cas, err error) {
		lock.Lock()

		if cbCalled == 1 {
			// Do nothing if we already got an answer
			lock.Unlock()
			return
		}

		// Mark the callback as having been invoked
		cbCalled = 1

		// Cancel any remaining operation
		opRes.Cancel()

		lock.Unlock()

		// Dispatch Callback
		cb(value, flags, cas, err)
	}

	// Dispatch a getReplica for each replica server
	numReplicas := agent.NumReplicas()
	for repIdx := 1; repIdx <= numReplicas; repIdx++ {
		op, err := agent.getOneReplica(key, repIdx, handler)
		if err == nil {
			lock.Lock()
			if cbCalled == 1 {
				op.Cancel()
				lock.Unlock()
				break
			}

			opRes.ops = append(opRes.ops, op)
			lock.Unlock()
		}
	}

	// If we have no pending ops, no requests were successful
	if len(opRes.ops) == 0 {
		return nil, ErrNoReplicas
	}

	return opRes, nil
}

// GetReplica retrieves a document from a replica server.
func (agent *Agent) GetReplica(key []byte, replicaIdx int, cb GetCallback) (PendingOp, error) {
	if replicaIdx > 0 {
		return agent.getOneReplica(key, replicaIdx, cb)
	} else if replicaIdx == 0 {
		return agent.getAnyReplica(key, cb)
	} else {
		return nil, ErrInvalidReplica
	}
}

// Touch updates the expiry for a document.
func (agent *Agent) Touch(key []byte, cas Cas, expiry uint32, cb TouchCallback) (PendingOp, error) {
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

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], expiry)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdTouch,
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

// Unlock unlocks a locked document.
func (agent *Agent) Unlock(key []byte, cas Cas, cb UnlockCallback) (PendingOp, error) {
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

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdUnlockKey,
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

// Remove removes a document.
func (agent *Agent) Remove(key []byte, cas Cas, cb RemoveCallback) (PendingOp, error) {
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

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDelete,
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

func (agent *Agent) store(opcode commandCode, key, value []byte, flags uint32, cas Cas, expiry uint32, cb StoreCallback) (PendingOp, error) {
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

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
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

// Add stores a document as long as it does not already exist.
func (agent *Agent) Add(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return agent.store(cmdAdd, key, value, flags, 0, expiry, cb)
}

// Set stores a document.
func (agent *Agent) Set(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return agent.store(cmdSet, key, value, flags, 0, expiry, cb)
}

// Replace replaces the value of a Couchbase document with another value.
func (agent *Agent) Replace(key, value []byte, flags uint32, cas Cas, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return agent.store(cmdReplace, key, value, flags, cas, expiry, cb)
}

func (agent *Agent) adjoin(opcode commandCode, key, value []byte, cb StoreCallback) (PendingOp, error) {
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

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
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

// Append appends some bytes to a document.
func (agent *Agent) Append(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return agent.adjoin(cmdAppend, key, value, cb)
}

// Prepend prepends some bytes to a document.
func (agent *Agent) Prepend(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return agent.adjoin(cmdPrepend, key, value, cb)
}

func (agent *Agent) counter(opcode commandCode, key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
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
			Magic:    reqMagic,
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

// Increment increments the unsigned integer value in a document.
func (agent *Agent) Increment(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return agent.counter(cmdIncrement, key, delta, initial, expiry, cb)
}

// Decrement decrements the unsigned integer value in a document.
func (agent *Agent) Decrement(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return agent.counter(cmdDecrement, key, delta, initial, expiry, cb)
}

// GetRandom retrieves the key and value of a random document stored within Couchbase Server.
func (agent *Agent) GetRandom(cb GetRandomCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Key, resp.Value, flags, Cas(resp.Cas), nil)
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
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// Stats retrieves statistics information from the server.
func (agent *Agent) Stats(key string, callback ServerStatsCallback) (PendingOp, error) {
	config := agent.routingInfo.Get()
	allOk := true
	// Iterate over each of the configs

	// TODO(brett19): Stop using routingInfo internals to dispatch these

	op := new(struct {
		multiPendingOp
		remaining int32
	})
	op.remaining = int32(config.clientMux.NumPipelines())

	stats := make(map[string]SingleServerStats)
	var statsLock sync.Mutex

	defer func() {
		if !allOk {
			op.Cancel()
		}
	}()

	for index := 0; index < config.clientMux.NumPipelines(); index++ {
		server := config.clientMux.GetPipeline(index)

		var req *memdQRequest
		serverName := server.Address()

		handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
			statsLock.Lock()
			defer statsLock.Unlock()

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
				Magic:    reqMagic,
				Opcode:   cmdStat,
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
