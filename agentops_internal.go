package gocbcore

import (
	"encoding/binary"
)

// GetMeta retrieves a document along with some internal Couchbase meta-data.
func (agent *Agent) GetMeta(key []byte, cb GetMetaCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			cb(nil, 0, 0, 0, 0, 0, 0, err)
			return
		}

		if len(resp.Extras) != 21 {
			cb(nil, 0, 0, 0, 0, 0, 0, ErrProtocol)
			return
		}

		deleted := binary.BigEndian.Uint32(resp.Extras[0:])
		flags := binary.BigEndian.Uint32(resp.Extras[4:])
		expTime := binary.BigEndian.Uint32(resp.Extras[8:])
		seqNo := SeqNo(binary.BigEndian.Uint64(resp.Extras[12:]))
		dataType := resp.Extras[20]

		cb(resp.Value, flags, Cas(resp.Cas), expTime, seqNo, dataType, deleted, nil)
	}

	extraBuf := make([]byte, 1)
	extraBuf[0] = 2

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetMeta,
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

// SetMeta stores a document along with setting some internal Couchbase meta-data.
func (agent *Agent) SetMeta(key, value, extra []byte, datatype uint8, options, flags, expiry uint32, cas, revseqno uint64, cb StoreCallback) (PendingOp, error) {
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

	extraBuf := make([]byte, 30+len(extra))
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	binary.BigEndian.PutUint64(extraBuf[8:], revseqno)
	binary.BigEndian.PutUint64(extraBuf[16:], cas)
	binary.BigEndian.PutUint32(extraBuf[24:], options)
	binary.BigEndian.PutUint16(extraBuf[28:], uint16(len(extra)))
	copy(extraBuf[30:], extra)
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdSetMeta,
			Datatype: datatype,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    value,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// DeleteMeta deletes a document along with setting some internal Couchbase meta-data.
func (agent *Agent) DeleteMeta(key, value, extra []byte, datatype uint8, options, flags, expiry uint32, cas, revseqno uint64, cb RemoveCallback) (PendingOp, error) {
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

	extraBuf := make([]byte, 30+len(extra))
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	binary.BigEndian.PutUint64(extraBuf[8:], revseqno)
	binary.BigEndian.PutUint64(extraBuf[16:], cas)
	binary.BigEndian.PutUint32(extraBuf[24:], options)
	binary.BigEndian.PutUint16(extraBuf[28:], uint16(len(extra)))
	copy(extraBuf[30:], extra)
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDelMeta,
			Datatype: datatype,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    value,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}
