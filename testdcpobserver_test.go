package gocbcore

import (
	"sync"
)

type Mutation struct {
	Expiry       uint32
	Locktime     uint32
	Cas          uint64
	Value        []byte
	CollectionID uint32
	StreamID     uint16
}

type Deletion struct {
	IsExpiration bool
	DeleteTime   uint32
	Cas          uint64
	CollectionID uint32
	StreamID     uint16
}

type TestStreamObserver struct {
	lock        sync.Mutex
	mutations   map[string]Mutation
	deletions   map[string]Deletion
	expirations map[string]Deletion
	endWg       sync.WaitGroup
}

func (so *TestStreamObserver) SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, streamId uint16,
	snapshotType SnapshotState) {
}

func (so *TestStreamObserver) Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	collectionId uint32, streamId uint16, key, value []byte) {
	mutation := Mutation{
		Expiry:       expiry,
		Locktime:     lockTime,
		Cas:          cas,
		Value:        value,
		CollectionID: collectionId,
		StreamID:     streamId,
	}

	so.lock.Lock()
	so.mutations[string(key)] = mutation
	so.lock.Unlock()
}

func (so *TestStreamObserver) Deletion(seqNo, revNo uint64, deleteTime uint32, cas uint64, datatype uint8, vbId uint16, collectionId uint32, streamId uint16,
	key, value []byte) {
	mutation := Deletion{
		IsExpiration: false,
		DeleteTime:   deleteTime,
		Cas:          cas,
		CollectionID: collectionId,
		StreamID:     streamId,
	}

	so.lock.Lock()
	so.deletions[string(key)] = mutation
	so.lock.Unlock()
}

func (so *TestStreamObserver) Expiration(seqNo, revNo uint64, deleteTime uint32, cas uint64, vbId uint16, collectionId uint32, streamId uint16, key []byte) {
	mutation := Deletion{
		IsExpiration: true,
		DeleteTime:   deleteTime,
		Cas:          cas,
		CollectionID: collectionId,
		StreamID:     streamId,
	}

	so.lock.Lock()
	so.deletions[string(key)] = mutation
	so.lock.Unlock()
}

func (so *TestStreamObserver) End(vbId uint16, streamId uint16, err error) {
	so.endWg.Done()
}

func (so *TestStreamObserver) CreateCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	collectionId uint32, ttl uint32, streamId uint16, key []byte) {
}

func (so *TestStreamObserver) DeleteCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	collectionId uint32, streamId uint16) {
}

func (so *TestStreamObserver) FlushCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64,
	collectionId uint32) {
}

func (so *TestStreamObserver) CreateScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	streamId uint16, key []byte) {
}

func (so *TestStreamObserver) DeleteScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	streamId uint16) {
}

func (so *TestStreamObserver) ModifyCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64,
	collectionId uint32, ttl uint32, streamId uint16) {
}

func (so *TestStreamObserver) OSOSnapshot(vbId uint16, snapshotType uint32, streamID uint16) {
}

func (so *TestStreamObserver) SeqNoAdvanced(vbId uint16, bySeqNo uint64, streamID uint16) {
}
