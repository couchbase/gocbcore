package gocbcore

import (
	"strconv"
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

type SnapshotMarker struct {
	lastSnapStart uint64
	lastSnapEnd   uint64
}

type DCPEventCounter struct {
	mutations          map[string]Mutation
	deletions          map[string]Deletion
	expirations        map[string]Deletion
	scopes             map[string]int
	collections        map[string]int
	scopesDeleted      map[string]int
	collectionsDeleted map[string]int
}


type TestStreamObserver struct {
	lock      sync.Mutex
	lastSeqno map[uint16]uint64
	snapshots map[uint16]SnapshotMarker
	counter   *DCPEventCounter
	endWg     sync.WaitGroup
}

func (so *TestStreamObserver) newCounter() {
	so.counter = &DCPEventCounter{
		mutations:          make(map[string]Mutation),
		deletions:          make(map[string]Deletion),
		expirations:        make(map[string]Deletion),
		scopes:             make(map[string]int),
		collections:        make(map[string]int),
		scopesDeleted:      make(map[string]int),
		collectionsDeleted: make(map[string]int),
	}
}

func (so *TestStreamObserver) SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, streamId uint16,
	snapshotType SnapshotState) {
	so.lock.Lock()
	so.snapshots[vbId] = SnapshotMarker{startSeqNo, endSeqNo}
	if so.lastSeqno[vbId] < startSeqNo || so.lastSeqno[vbId] > endSeqNo {
		so.lastSeqno[vbId] = startSeqNo
	}
	so.lock.Unlock()
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
	so.counter.mutations[string(key)] = mutation
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
	so.counter.deletions[string(key)] = mutation
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
	so.counter.deletions[string(key)] = mutation
	so.lock.Unlock()
}

func (so *TestStreamObserver) End(vbId uint16, streamId uint16, err error) {
	so.endWg.Done()
}

func (so *TestStreamObserver) CreateCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	collectionId uint32, ttl uint32, streamId uint16, key []byte) {
	so.lock.Lock()
	so.counter.collections[strconv.Itoa(int(scopeId))+"."+string(key)]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) DeleteCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	collectionId uint32, streamId uint16) {
	so.lock.Lock()
	so.counter.collectionsDeleted[strconv.Itoa(int(scopeId))+"."+strconv.Itoa(int(collectionId))]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) FlushCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64,
	collectionId uint32) {
}

func (so *TestStreamObserver) CreateScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	streamId uint16, key []byte) {
	so.lock.Lock()
	so.counter.scopes[string(key)]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) DeleteScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	streamId uint16) {
	so.lock.Lock()
	so.counter.scopesDeleted[strconv.Itoa(int(scopeId))]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) ModifyCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64,
	collectionId uint32, ttl uint32, streamId uint16) {
}

func (so *TestStreamObserver) OSOSnapshot(vbId uint16, snapshotType uint32, streamID uint16) {
}

func (so *TestStreamObserver) SeqNoAdvanced(vbId uint16, bySeqNo uint64, streamID uint16) {
}
