package gocbcore

import (
	"strconv"
	"sync"
)

type DCPEventCounter struct {
	mutations          map[string]DcpMutation
	deletions          map[string]DcpDeletion
	expirations        map[string]DcpExpiration
	scopes             map[string]int
	collections        map[string]int
	scopesDeleted      map[string]int
	collectionsDeleted map[string]int
}

type TestStreamObserver struct {
	lock      sync.Mutex
	lastSeqno map[uint16]uint64
	snapshots map[uint16]DcpSnapshotMarker
	counter   *DCPEventCounter
	endWg     sync.WaitGroup
}

func (so *TestStreamObserver) newCounter() {
	so.counter = &DCPEventCounter{
		mutations:          make(map[string]DcpMutation),
		deletions:          make(map[string]DcpDeletion),
		expirations:        make(map[string]DcpExpiration),
		scopes:             make(map[string]int),
		collections:        make(map[string]int),
		scopesDeleted:      make(map[string]int),
		collectionsDeleted: make(map[string]int),
	}
}

func (so *TestStreamObserver) SnapshotMarker(snapshotMarker DcpSnapshotMarker) {
	so.lock.Lock()
	so.snapshots[snapshotMarker.VbID] = snapshotMarker
	if so.lastSeqno[snapshotMarker.VbID] < snapshotMarker.StartSeqNo || so.lastSeqno[snapshotMarker.VbID] > snapshotMarker.EndSeqNo {
		so.lastSeqno[snapshotMarker.VbID] = snapshotMarker.StartSeqNo
	}
	so.lock.Unlock()
}

func (so *TestStreamObserver) Mutation(mutation DcpMutation) {
	so.lock.Lock()
	so.counter.mutations[string(mutation.Key)] = mutation
	so.lock.Unlock()
}

func (so *TestStreamObserver) Deletion(deletion DcpDeletion) {
	so.lock.Lock()
	so.counter.deletions[string(deletion.Key)] = deletion
	so.lock.Unlock()
}

func (so *TestStreamObserver) Expiration(expiration DcpExpiration) {
	so.lock.Lock()
	so.counter.expirations[string(expiration.Key)] = expiration
	so.lock.Unlock()
}

func (so *TestStreamObserver) End(end DcpStreamEnd, err error) {
	so.endWg.Done()
}

func (so *TestStreamObserver) CreateCollection(creation DcpCollectionCreation) {
	so.lock.Lock()
	so.counter.collections[strconv.Itoa(int(creation.ScopeID))+"."+string(creation.Key)]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) DeleteCollection(deletion DcpCollectionDeletion) {
	so.lock.Lock()
	so.counter.collectionsDeleted[strconv.Itoa(int(deletion.ScopeID))+"."+strconv.Itoa(int(deletion.CollectionID))]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) FlushCollection(flush DcpCollectionFlush) {
}

func (so *TestStreamObserver) CreateScope(creation DcpScopeCreation) {
	so.lock.Lock()
	so.counter.scopes[string(creation.Key)]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) DeleteScope(deletion DcpScopeDeletion) {
	so.lock.Lock()
	so.counter.scopesDeleted[strconv.Itoa(int(deletion.ScopeID))]++
	so.lock.Unlock()
}

func (so *TestStreamObserver) ModifyCollection(modification DcpCollectionModification) {
}

func (so *TestStreamObserver) OSOSnapshot(snapshot DcpOSOSnapshot) {
}

func (so *TestStreamObserver) SeqNoAdvanced(seqNoAdvanced DcpSeqNoAdvanced) {
}
