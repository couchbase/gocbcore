package gocbcore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/couchbase/gocbcore/v8/memd"
)

type dcpComponent struct {
	kvMux *kvMux
}

func newDcpComponent(kvMux *kvMux) *dcpComponent {
	return &dcpComponent{
		kvMux: kvMux,
	}
}

func (dcp *dcpComponent) OpenStream(vbID uint16, flags memd.DcpStreamAddFlag, vbUUID VbUUID, startSeqNo,
	endSeqNo, snapStartSeqNo, snapEndSeqNo SeqNo, evtHandler StreamObserver, filter *StreamFilter, cb OpenStreamCallback) (PendingOp, error) {
	var req *memdQRequest
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if resp != nil && resp.Magic == memd.CmdMagicRes {
			// This is the response to the open stream request.
			if err != nil {
				req.internalCancel(err)

				// All client errors are handled by the StreamObserver
				cb(nil, err)
				return
			}

			numEntries := len(resp.Value) / 16
			entries := make([]FailoverEntry, numEntries)
			for i := 0; i < numEntries; i++ {
				entries[i] = FailoverEntry{
					VbUUID: VbUUID(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
					SeqNo:  SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
				}
			}

			cb(entries, nil)
			return
		}

		if err != nil {
			req.internalCancel(err)
			streamID := noStreamID
			if filter != nil {
				streamID = filter.StreamID
			}
			evtHandler.End(vbID, streamID, err)
			return
		}

		if resp == nil {
			logWarnf("DCP event occurred with no error and no response")
			return
		}

		// This is one of the stream events
		switch resp.Command {
		case memd.CmdDcpSnapshotMarker:
			vbID := resp.Vbucket
			newStartSeqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			newEndSeqNo := binary.BigEndian.Uint64(resp.Extras[8:])
			snapshotType := binary.BigEndian.Uint32(resp.Extras[16:])
			var streamID uint16
			if resp.StreamIDFrame != nil {
				streamID = resp.StreamIDFrame.StreamID
			}
			evtHandler.SnapshotMarker(newStartSeqNo, newEndSeqNo, vbID, streamID, SnapshotState(snapshotType))
		case memd.CmdDcpMutation:
			vbID := resp.Vbucket
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			flags := binary.BigEndian.Uint32(resp.Extras[16:])
			expiry := binary.BigEndian.Uint32(resp.Extras[20:])
			lockTime := binary.BigEndian.Uint32(resp.Extras[24:])
			var streamID uint16
			if resp.StreamIDFrame != nil {
				streamID = resp.StreamIDFrame.StreamID
			}
			evtHandler.Mutation(seqNo, revNo, flags, expiry, lockTime, resp.Cas, resp.Datatype, vbID, resp.CollectionID, streamID, resp.Key, resp.Value)
		case memd.CmdDcpDeletion:
			vbID := resp.Vbucket
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			var streamID uint16
			if resp.StreamIDFrame != nil {
				streamID = resp.StreamIDFrame.StreamID
			}
			evtHandler.Deletion(seqNo, revNo, resp.Cas, resp.Datatype, vbID, resp.CollectionID, streamID, resp.Key, resp.Value)
		case memd.CmdDcpExpiration:
			vbID := resp.Vbucket
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			var streamID uint16
			if resp.StreamIDFrame != nil {
				streamID = resp.StreamIDFrame.StreamID
			}
			evtHandler.Expiration(seqNo, revNo, resp.Cas, vbID, resp.CollectionID, streamID, resp.Key)
		case memd.CmdDcpEvent:
			vbID := resp.Vbucket
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			eventCode := memd.StreamEventCode(binary.BigEndian.Uint32(resp.Extras[8:]))
			version := resp.Extras[12]
			var streamID uint16
			if resp.StreamIDFrame != nil {
				streamID = resp.StreamIDFrame.StreamID
			}

			switch eventCode {
			case memd.StreamEventCollectionCreate:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				collectionID := binary.BigEndian.Uint32(resp.Value[12:])
				var ttl uint32
				if version == 1 {
					ttl = binary.BigEndian.Uint32(resp.Value[16:])
				}
				evtHandler.CreateCollection(seqNo, version, vbID, manifestUID, scopeID, collectionID, ttl, streamID, resp.Key)
			case memd.StreamEventCollectionDelete:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				collectionID := binary.BigEndian.Uint32(resp.Value[12:])
				evtHandler.DeleteCollection(seqNo, version, vbID, manifestUID, scopeID, collectionID, streamID)
			case memd.StreamEventCollectionFlush:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				collectionID := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.FlushCollection(seqNo, version, vbID, manifestUID, collectionID)
			case memd.StreamEventScopeCreate:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.CreateScope(seqNo, version, vbID, manifestUID, scopeID, streamID, resp.Key)
			case memd.StreamEventScopeDelete:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.DeleteScope(seqNo, version, vbID, manifestUID, scopeID, streamID)
			case memd.StreamEventCollectionChanged:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				collectionID := binary.BigEndian.Uint32(resp.Value[8:])
				ttl := binary.BigEndian.Uint32(resp.Value[12:])
				evtHandler.ModifyCollection(seqNo, version, vbID, manifestUID, collectionID, ttl, streamID)
			}
		case memd.CmdDcpStreamEnd:
			vbID := resp.Vbucket
			code := memd.StreamEndStatus(binary.BigEndian.Uint32(resp.Extras[0:]))
			var streamID uint16
			if resp.StreamIDFrame != nil {
				streamID = resp.StreamIDFrame.StreamID
			}
			evtHandler.End(vbID, streamID, getStreamEndStatusError(code))
			req.internalCancel(err)
		case memd.CmdDcpSeqNoAdvanced:
			vbID := resp.Vbucket
			snapshotType := binary.BigEndian.Uint32(resp.Extras[0:])
			var streamID uint16
			if resp.StreamIDFrame != nil {
				streamID = resp.StreamIDFrame.StreamID
			}
			evtHandler.OSOSnapshot(vbID, streamID, snapshotType)
		}
	}

	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(flags))
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(startSeqNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(endSeqNo))
	binary.BigEndian.PutUint64(extraBuf[24:], uint64(vbUUID))
	binary.BigEndian.PutUint64(extraBuf[32:], uint64(snapStartSeqNo))
	binary.BigEndian.PutUint64(extraBuf[40:], uint64(snapEndSeqNo))

	var val []byte
	val = nil
	if filter != nil {
		convertedFilter := streamFilter{}
		for _, cid := range filter.Collections {
			convertedFilter.Collections = append(convertedFilter.Collections, fmt.Sprintf("%x", cid))
		}
		if filter.Scope != noScopeID {
			convertedFilter.Scope = fmt.Sprintf("%x", filter.Scope)
		}
		if filter.ManifestUID != noManifestUID {
			convertedFilter.ManifestUID = fmt.Sprintf("%x", filter.ManifestUID)
		}
		if filter.StreamID != noStreamID {
			convertedFilter.StreamID = filter.StreamID
		}
		var err error
		val, err = json.Marshal(convertedFilter)
		if err != nil {
			return nil, err
		}
	}

	req = &memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdDcpStreamReq,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    val,
			Vbucket:  vbID,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return dcp.kvMux.DispatchDirect(req)
}

func (dcp *dcpComponent) CloseStreamWithID(vbID uint16, streamID uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(_ *memdQResponse, _ *memdQRequest, err error) {
		cb(err)
	}

	streamFrame := &memd.StreamIDFrame{
		StreamID: streamID,
	}

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:         memd.CmdMagicReq,
			Command:       memd.CmdDcpCloseStream,
			Datatype:      0,
			Cas:           0,
			Extras:        nil,
			Key:           nil,
			Value:         nil,
			Vbucket:       vbID,
			StreamIDFrame: streamFrame,
		},
		Callback:      handler,
		ReplicaIdx:    0,
		Persistent:    false,
		RetryStrategy: newFailFastRetryStrategy(),
	}

	return dcp.kvMux.DispatchDirect(req)
}

func (dcp *dcpComponent) CloseStream(vbID uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(_ *memdQResponse, _ *memdQRequest, err error) {
		cb(err)
	}

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdDcpCloseStream,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbID,
		},
		Callback:      handler,
		ReplicaIdx:    0,
		Persistent:    false,
		RetryStrategy: newFailFastRetryStrategy(),
	}
	return dcp.kvMux.DispatchDirect(req)
}

func (dcp *dcpComponent) GetFailoverLog(vbID uint16, cb GetFailoverLogCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		numEntries := len(resp.Value) / 16
		entries := make([]FailoverEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = FailoverEntry{
				VbUUID: VbUUID(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
				SeqNo:  SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
			}
		}
		cb(entries, nil)
	}

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdDcpGetFailoverLog,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbID,
		},
		Callback:      handler,
		ReplicaIdx:    0,
		Persistent:    false,
		RetryStrategy: newFailFastRetryStrategy(),
	}
	return dcp.kvMux.DispatchDirect(req)
}

func (dcp *dcpComponent) GetVbucketSeqnosWithCollectionID(serverIdx int, state memd.VbucketState, collectionID uint32, cb GetVBucketSeqnosCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		var vbs []VbSeqNoEntry

		numVbs := len(resp.Value) / 10
		for i := 0; i < numVbs; i++ {
			vbs = append(vbs, VbSeqNoEntry{
				VbID:  binary.BigEndian.Uint16(resp.Value[i*10:]),
				SeqNo: SeqNo(binary.BigEndian.Uint64(resp.Value[i*10+2:])),
			})
		}

		cb(vbs, nil)
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(state))
	binary.BigEndian.PutUint32(extraBuf[4:], collectionID)

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGetAllVBSeqnos,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    nil,
			Vbucket:  0,
		},
		Callback:      handler,
		ReplicaIdx:    -serverIdx,
		Persistent:    false,
		RetryStrategy: newFailFastRetryStrategy(),
	}

	return dcp.kvMux.DispatchDirect(req)
}

func (dcp *dcpComponent) GetVbucketSeqnos(serverIdx int, state memd.VbucketState, cb GetVBucketSeqnosCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		var vbs []VbSeqNoEntry

		numVbs := len(resp.Value) / 10
		for i := 0; i < numVbs; i++ {
			vbs = append(vbs, VbSeqNoEntry{
				VbID:  binary.BigEndian.Uint16(resp.Value[i*10:]),
				SeqNo: SeqNo(binary.BigEndian.Uint64(resp.Value[i*10+2:])),
			})
		}

		cb(vbs, nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(state))

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGetAllVBSeqnos,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    nil,
			Vbucket:  0,
		},
		Callback:      handler,
		ReplicaIdx:    -serverIdx,
		Persistent:    false,
		RetryStrategy: newFailFastRetryStrategy(),
	}

	return dcp.kvMux.DispatchDirect(req)
}
