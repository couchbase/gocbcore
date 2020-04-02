package gocbcore

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
	OSOSnapshot(vbID uint16, streamID uint16, snapshotType uint32)
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
