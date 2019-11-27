package gocbcore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	noManifestUID = uint64(0xFFFFFFFFFFFFFFFF)
	noScopeID     = uint32(0xFFFFFFFF)
	noStreamID    = uint16(0xFFFF)
)

// SnapshotState represents the state of a particular cluster snapshot.
type SnapshotState uint32

// HasInMemory returns whether this snapshot is available in memory.
func (s SnapshotState) HasInMemory() bool {
	return uint32(s)&1 != 0
}

// HasOnDisk returns whether this snapshot is available on disk.
func (s SnapshotState) HasOnDisk() bool {
	return uint32(s)&2 != 0
}

// FailoverEntry represents a single entry in the server fail-over log.
type FailoverEntry struct {
	VbUUID VbUUID
	SeqNo  SeqNo
}

// StreamObserver provides an interface to receive events from a running DCP stream.
type StreamObserver interface {
	SnapshotMarker(startSeqNo, endSeqNo uint64, vbID uint16, streamID uint16, snapshotType SnapshotState)
	Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbID uint16, collectionID uint32, streamID uint16, key, value []byte)
	Deletion(seqNo, revNo, cas uint64, datatype uint8, vbID uint16, collectionID uint32, streamID uint16, key, value []byte)
	Expiration(seqNo, revNo, cas uint64, vbID uint16, collectionID uint32, streamID uint16, key []byte)
	End(vbID uint16, streamID uint16, err error)
	CreateCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, ttl uint32, streamID uint16, key []byte)
	DeleteCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, streamID uint16)
	FlushCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32)
	CreateScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16, key []byte)
	DeleteScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16)
	ModifyCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32, ttl uint32, streamID uint16)
}

// NewStreamFilter returns a new StreamFilter.
func NewStreamFilter() *StreamFilter {
	return &StreamFilter{
		ManifestUID: noManifestUID,
		Scope:       noScopeID,
		StreamID:    noStreamID,
	}
}

// StreamFilter provides options for filtering a DCP stream.
type StreamFilter struct {
	ManifestUID uint64
	Collections []uint32
	Scope       uint32
	StreamID    uint16
}

type streamFilter struct {
	ManifestUID string   `json:"uid,omitempty"`
	Collections []string `json:"collections,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	StreamID    uint16   `json:"sid,omitempty"`
}

// OpenStreamCallback is invoked with the results of `OpenStream` operations.
type OpenStreamCallback func([]FailoverEntry, error)

// CloseStreamCallback is invoked with the results of `CloseStream` operations.
type CloseStreamCallback func(error)

// GetFailoverLogCallback is invoked with the results of `GetFailoverLog` operations.
type GetFailoverLogCallback func([]FailoverEntry, error)

// VbSeqNoEntry represents a single GetVbucketSeqnos sequence number entry.
type VbSeqNoEntry struct {
	VbID  uint16
	SeqNo SeqNo
}

// GetVBucketSeqnosCallback is invoked with the results of `GetVBucketSeqnos` operations.
type GetVBucketSeqnosCallback func([]VbSeqNoEntry, error)

// OpenStream opens a DCP stream for a particular VBucket and, optionally, filter.
func (agent *Agent) OpenStream(vbID uint16, flags DcpStreamAddFlag, vbUUID VbUUID, startSeqNo,
	endSeqNo, snapStartSeqNo, snapEndSeqNo SeqNo, evtHandler StreamObserver, filter *StreamFilter, cb OpenStreamCallback) (PendingOp, error) {
	var req *memdQRequest
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if resp != nil && resp.Magic == resMagic {
			// This is the response to the open stream request.
			if err != nil {
				req.Cancel()

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
			req.Cancel()
			streamID := noStreamID
			if filter != nil {
				streamID = filter.StreamID
			}
			evtHandler.End(vbID, streamID, err)
			return
		}

		// This is one of the stream events
		switch resp.Opcode {
		case cmdDcpSnapshotMarker:
			vbID := uint16(resp.Vbucket)
			newStartSeqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			newEndSeqNo := binary.BigEndian.Uint64(resp.Extras[8:])
			snapshotType := binary.BigEndian.Uint32(resp.Extras[16:])
			var streamID uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamID {
				streamID = resp.FrameExtras.StreamID
			}
			evtHandler.SnapshotMarker(newStartSeqNo, newEndSeqNo, vbID, streamID, SnapshotState(snapshotType))
		case cmdDcpMutation:
			vbID := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			flags := binary.BigEndian.Uint32(resp.Extras[16:])
			expiry := binary.BigEndian.Uint32(resp.Extras[20:])
			lockTime := binary.BigEndian.Uint32(resp.Extras[24:])
			var streamID uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamID {
				streamID = resp.FrameExtras.StreamID
			}
			evtHandler.Mutation(seqNo, revNo, flags, expiry, lockTime, resp.Cas, resp.Datatype, vbID, resp.CollectionID, streamID, resp.Key, resp.Value)
		case cmdDcpDeletion:
			vbID := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			var streamID uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamID {
				streamID = resp.FrameExtras.StreamID
			}
			evtHandler.Deletion(seqNo, revNo, resp.Cas, resp.Datatype, vbID, resp.CollectionID, streamID, resp.Key, resp.Value)
		case cmdDcpExpiration:
			vbID := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			var streamID uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamID {
				streamID = resp.FrameExtras.StreamID
			}
			evtHandler.Expiration(seqNo, revNo, resp.Cas, vbID, resp.CollectionID, streamID, resp.Key)
		case cmdDcpEvent:
			vbID := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			eventCode := StreamEventCode(binary.BigEndian.Uint32(resp.Extras[8:]))
			version := resp.Extras[12]
			var streamID uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamID {
				streamID = resp.FrameExtras.StreamID
			}

			switch eventCode {
			case StreamEventCollectionCreate:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				collectionID := binary.BigEndian.Uint32(resp.Value[12:])
				var ttl uint32
				if version == 1 {
					ttl = binary.BigEndian.Uint32(resp.Value[16:])
				}
				evtHandler.CreateCollection(seqNo, version, vbID, manifestUID, scopeID, collectionID, ttl, streamID, resp.Key)
			case StreamEventCollectionDelete:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				collectionID := binary.BigEndian.Uint32(resp.Value[12:])
				evtHandler.DeleteCollection(seqNo, version, vbID, manifestUID, scopeID, collectionID, streamID)
			case StreamEventCollectionFlush:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				collectionID := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.FlushCollection(seqNo, version, vbID, manifestUID, collectionID)
			case StreamEventScopeCreate:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.CreateScope(seqNo, version, vbID, manifestUID, scopeID, streamID, resp.Key)
			case StreamEventScopeDelete:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				scopeID := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.DeleteScope(seqNo, version, vbID, manifestUID, scopeID, streamID)
			case StreamEventCollectionChanged:
				manifestUID := binary.BigEndian.Uint64(resp.Value[0:])
				collectionID := binary.BigEndian.Uint32(resp.Value[8:])
				ttl := binary.BigEndian.Uint32(resp.Value[12:])
				evtHandler.ModifyCollection(seqNo, version, vbID, manifestUID, collectionID, ttl, streamID)
			}
		case cmdDcpStreamEnd:
			vbID := uint16(resp.Vbucket)
			code := streamEndStatus(binary.BigEndian.Uint32(resp.Extras[0:]))
			var streamID uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamID {
				streamID = resp.FrameExtras.StreamID
			}
			evtHandler.End(vbID, streamID, getStreamEndError(code))
			req.Cancel()
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
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpStreamReq,
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
	return agent.dispatchOp(req)
}

// CloseStreamWithID shuts down an open stream for the specified VBucket for the specified stream.
func (agent *Agent) CloseStreamWithID(vbID uint16, streamID uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(_ *memdQResponse, _ *memdQRequest, err error) {
		cb(err)
	}

	frameExtras := &memdFrameExtras{
		HasStreamID: true,
		StreamID:    streamID,
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:       altReqMagic,
			Opcode:      cmdDcpCloseStream,
			Datatype:    0,
			Cas:         0,
			Extras:      nil,
			Key:         nil,
			Value:       nil,
			Vbucket:     vbID,
			FrameExtras: frameExtras,
		},
		Callback:      handler,
		ReplicaIdx:    0,
		Persistent:    false,
		RetryStrategy: NewFailFastRetryStrategy(),
	}
	return agent.dispatchOp(req)
}

// CloseStream shuts down an open stream for the specified VBucket.
func (agent *Agent) CloseStream(vbID uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(_ *memdQResponse, _ *memdQRequest, err error) {
		cb(err)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpCloseStream,
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
		RetryStrategy: NewFailFastRetryStrategy(),
	}
	return agent.dispatchOp(req)
}

// GetFailoverLog retrieves the fail-over log for a particular VBucket.  This is used
// to resume an interrupted stream after a node fail-over has occurred.
func (agent *Agent) GetFailoverLog(vbID uint16, cb GetFailoverLogCallback) (PendingOp, error) {
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
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpGetFailoverLog,
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
		RetryStrategy: NewFailFastRetryStrategy(),
	}
	return agent.dispatchOp(req)
}

// GetVbucketSeqnosWithCollectionID returns the last checkpoint for a particular VBucket for a particular collection. This is useful
// for starting a DCP stream from wherever the server currently is.
func (agent *Agent) GetVbucketSeqnosWithCollectionID(serverIdx int, state VbucketState, collectionID uint32, cb GetVBucketSeqnosCallback) (PendingOp, error) {
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
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetAllVBSeqnos,
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
		RetryStrategy: NewFailFastRetryStrategy(),
	}

	return agent.dispatchOp(req)
}

// GetVbucketSeqnos returns the last checkpoint for a particular VBucket.  This is useful
// for starting a DCP stream from wherever the server currently is.
func (agent *Agent) GetVbucketSeqnos(serverIdx int, state VbucketState, cb GetVBucketSeqnosCallback) (PendingOp, error) {
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
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetAllVBSeqnos,
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
		RetryStrategy: NewFailFastRetryStrategy(),
	}

	return agent.dispatchOp(req)
}
