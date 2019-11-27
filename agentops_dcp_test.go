package gocbcore

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"golang.org/x/sync/errgroup"
)

type testDCPMutation struct {
	key   []byte
	value []byte
	cas   uint64
}

type testObserver struct {
	mutationsRcvd       int32
	deletionsRcvd       int32
	expirationsRcvd     int32
	mutations           map[uint16]map[uint32]map[string]testDCPMutation
	deletions           map[uint16]map[uint32]map[string]testDCPMutation
	expirations         map[uint16]map[uint32]map[string]testDCPMutation
	createdCollections  map[uint16]testDCPMutation
	deletedCollections  map[uint16]testDCPMutation
	modifiedCollections map[uint16]testDCPMutation
	createdScopes       map[uint16]testDCPMutation
	mlock               sync.Mutex
	dlock               sync.Mutex
	elock               sync.Mutex
}

func (so *testObserver) SnapshotMarker(startSeqNo, endSeqNo uint64, vbID uint16, streamID uint16,
	snapshotType SnapshotState) {
}

func (so *testObserver) Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbID uint16,
	CollectionID uint32, streamID uint16, key, value []byte) {
	so.mlock.Lock()
	if _, ok := so.mutations[streamID]; !ok {
		so.mutations[streamID] = make(map[uint32]map[string]testDCPMutation)
	}
	if _, ok := so.mutations[streamID][CollectionID]; !ok {
		so.mutations[streamID][CollectionID] = make(map[string]testDCPMutation)
	}
	so.mutations[streamID][CollectionID][string(key)] = testDCPMutation{
		key:   key,
		value: value,
		cas:   cas,
	}
	atomic.AddInt32(&so.mutationsRcvd, 1)
	so.mlock.Unlock()
}

func (so *testObserver) Deletion(seqNo, revNo, cas uint64, datatype uint8, vbID uint16, CollectionID uint32, streamID uint16,
	key, value []byte) {
	so.dlock.Lock()
	if _, ok := so.deletions[streamID]; !ok {
		so.deletions[streamID] = make(map[uint32]map[string]testDCPMutation)
	}
	if _, ok := so.deletions[streamID][CollectionID]; !ok {
		so.deletions[streamID][CollectionID] = make(map[string]testDCPMutation)
	}
	so.deletions[streamID][CollectionID][string(key)] = testDCPMutation{
		key:   key,
		value: value,
		cas:   cas,
	}
	atomic.AddInt32(&so.deletionsRcvd, 1)
	so.dlock.Unlock()
}

func (so *testObserver) Expiration(seqNo, revNo, cas uint64, vbID uint16, CollectionID uint32, streamID uint16, key []byte) {
	so.elock.Lock()
	if _, ok := so.expirations[streamID]; !ok {
		so.expirations[streamID] = make(map[uint32]map[string]testDCPMutation)
	}
	if _, ok := so.expirations[streamID][CollectionID]; !ok {
		so.expirations[streamID][CollectionID] = make(map[string]testDCPMutation)
	}
	so.expirations[streamID][CollectionID][string(key)] = testDCPMutation{
		key: key,
		cas: cas,
	}
	atomic.AddInt32(&so.expirationsRcvd, 1)
	so.elock.Unlock()
}

func (so *testObserver) End(vbID uint16, streamID uint16, err error) {
}

func (so *testObserver) CreateCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32,
	collectionID uint32, ttl uint32, streamID uint16, key []byte) {
}

func (so *testObserver) DeleteCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32,
	streamID uint16) {
}

func (so *testObserver) FlushCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32) {

}

func (so *testObserver) CreateScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32,
	streamID uint16, key []byte) {
}

func (so *testObserver) DeleteScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32,
	streamID uint16) {
}

func (so *testObserver) ModifyCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64,
	collectionID uint32, ttl uint32, streamID uint16) {
}

func TestDCP(t *testing.T) {
	if globalDCPAgent == nil {
		t.Skip("Skipping test as DCP not supported")
	}

	if testing.Short() {
		t.Fatalf("Skipping test as using short mode")
	}

	testDCP(t)
}
func testDCP(t *testing.T) {
	observer := &testObserver{
		mutations:   make(map[uint16]map[uint32]map[string]testDCPMutation),
		deletions:   make(map[uint16]map[uint32]map[string]testDCPMutation),
		expirations: make(map[uint16]map[uint32]map[string]testDCPMutation),
	}
	numMutations := 25
	numDeletions := 10
	numExpirations := 5

	var deletions map[string]uint32
	var mutations map[string]uint32
	var expirations map[string]uint32

	var collectionName string
	var filter *StreamFilter
	var streamID uint16
	var cid uint32

	if globalDCPOpAgent.useCollections {
		collectionName = "collection1"
		manifest, err := testCreateCollection(collectionName, "_default", globalDCPOpAgent.bucketName, globalDCPOpAgent.Agent)
		if err != nil {
			t.Fatalf("Failed to create collection, %v", err)
		}

		for _, col := range manifest.Scopes[0].Collections {
			if col.Name == "collection1" {
				cid = col.UID
			}
		}
		if cid == 0 {
			t.Fatalf("Failed to find collection1 in manifest")
		}

		streamID = 1
		filter = NewStreamFilter()
		filter.StreamID = streamID
		filter.Collections = []uint32{cid}
	}

	openStreams(t, observer, filter)

	// do our best to tidy up otherwise we'll hit nil pointers when we try to close the agents
	defer func() {
		for i := 0; i < globalDCPAgent.numVbuckets; i++ {
			waitCh := make(chan error)
			if globalDCPOpAgent.useCollections {
				globalDCPAgent.CloseStreamWithID(uint16(i), streamID, func(e error) {
					waitCh <- e
				})
			} else {
				globalDCPAgent.CloseStream(uint16(i), func(e error) {
					waitCh <- e
				})
			}

			err := <-waitCh
			if err != nil {
				t.Errorf("CloseStream failed, %v", err)
			}
		}
	}()

	mutations, expirations = doMutations(t, numMutations, numExpirations, "_default")

	// if we're using collections then we create 2 sets of docs so that can we can test the filter
	if globalDCPOpAgent.useCollections {
		mutations, expirations = doMutations(t, numMutations, numExpirations, collectionName)
	}

	err := waitForChanges(&observer.mutationsRcvd, numMutations)
	if err != nil {
		t.Fatalf("Failed waiting for mutations, %v", err)
	}

	if globalAgent.SupportsFeature(TestDCPExpiryFeature) {
		// wait for expirations to take place
		time.Sleep(2 * time.Second)

		triggerExpirations(t, expirations, "_default")

		if globalDCPOpAgent.useCollections {
			triggerExpirations(t, expirations, collectionName)
		}

		err = waitForChanges(&observer.expirationsRcvd, numExpirations)
		if err != nil {
			t.Fatalf("Failed waiting for expirations, %v", err)
		}
	}

	deletions = doDeletions(t, numDeletions, mutations, "_default")

	if globalDCPOpAgent.useCollections {
		deletions = doDeletions(t, numDeletions, mutations, collectionName)
	}

	err = waitForChanges(&observer.deletionsRcvd, numDeletions)
	if err != nil {
		t.Fatalf("Failed waiting for deletions, %v", err)
	}

	observerMutations := observer.mutations[streamID][cid]
	for k, v := range mutations {
		observed, ok := observerMutations[k]
		if !ok {
			t.Errorf("Observer missing mutation entry for %s", k)
			continue
		}

		if cbCrc(observed.value) != v {
			t.Errorf("%s mutation had incorrect value, expected checksum %d but was %d", k, v, cbCrc(observed.value))
		}

		if observed.cas == 0 {
			t.Errorf("%s mutation had cas value of 0", k)
		}
	}

	observerDeletions := observer.deletions[streamID][cid]
	for k := range deletions {
		observed, ok := observerDeletions[k]
		if !ok {
			t.Errorf("Observer missing deletion entry for %s", k)
			continue
		}

		if observed.cas == 0 {
			t.Errorf("%s deletion had cas value of 0", k)
		}
	}

	if globalAgent.SupportsFeature(TestDCPExpiryFeature) {
		observerExpirations := observer.expirations[streamID][cid]
		for k := range expirations {
			observed, ok := observerExpirations[k]
			if !ok {
				t.Errorf("Observer missing expiration entry for %s", k)
				continue
			}

			if observed.cas == 0 {
				t.Errorf("%s expiration had cas value of 0", k)
			}
		}
	}
}

func waitForChanges(metric *int32, numChanges int) error {
	timer := time.After(10 * time.Second)
	for {
		select {
		case <-timer:
			return errors.New("timeout waiting for changes")
		default:
			if atomic.LoadInt32(metric) == int32(numChanges) {
				return nil
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func openStreams(t *testing.T, observer *testObserver, filter *StreamFilter) {
	var g errgroup.Group
	var ops []PendingOp
	lock := sync.Mutex{}
	for i := 0; i < globalDCPAgent.numVbuckets; i++ {
		vbucket := i // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			waitCh := make(chan error)
			op, err := globalDCPAgent.OpenStream(uint16(vbucket), DcpStreamAddFlagActiveOnly, 0, 0, math.MaxInt64, 0, 0,
				observer, filter, func(entries []FailoverEntry, e error) {
					waitCh <- e
				})
			if err != nil {
				return err
			}
			lock.Lock()
			ops = append(ops, op)
			lock.Unlock()

			return <-waitCh
		})
	}

	if err := g.Wait(); err != nil {
		for _, op := range ops {
			op.Cancel()
		}
		t.Fatalf("Failed running OpenStreams, %v", err)
	}
}

func doMutations(t *testing.T, numMutations, numExpirations int,
	collectionName string) (map[string]uint32, map[string]uint32) {
	var lock sync.Mutex
	var g errgroup.Group
	mutations := make(map[string]uint32)
	expirations := make(map[string]uint32)

	for i := 0; i < numMutations; i++ {
		num := i
		g.Go(func() error {
			waitCh := make(chan error)
			key := []byte(strconv.Itoa(num))

			randomBytes := make([]byte, rand.Intn(256))
			for j := 0; j < len(randomBytes); j++ {
				randomBytes[j] = byte(j)
			}

			var expiry uint32
			lock.Lock()
			if globalAgent.SupportsFeature(TestDCPExpiryFeature) && numMutations-num <= numExpirations {
				expiry = 1
				expirations[string(key)] = cbCrc(randomBytes)
			}
			mutations[string(key)] = cbCrc(randomBytes)
			lock.Unlock()

			_, err := globalDCPOpAgent.SetEx(SetOptions{
				Key:            key,
				Value:          randomBytes,
				Expiry:         expiry,
				CollectionName: collectionName,
				ScopeName:      "_default",
			}, func(result *StoreResult, e error) {
				waitCh <- e
			})
			if err != nil {
				return err
			}

			return <-waitCh
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Failed running mutations, %v", err)
	}

	return mutations, expirations
}

func doDeletions(t *testing.T, numDeletions int, mutations map[string]uint32,
	collectionName string) map[string]uint32 {
	var lock sync.Mutex
	var g errgroup.Group
	deletions := make(map[string]uint32)

	for i := 0; i < numDeletions; i++ {
		num := i
		g.Go(func() error {
			waitCh := make(chan error)
			key := []byte(strconv.Itoa(num))

			lock.Lock()
			deletions[string(key)] = 0
			delete(mutations, string(key))
			lock.Unlock()

			_, err := globalDCPOpAgent.DeleteEx(DeleteOptions{
				Key:            key,
				CollectionName: collectionName,
				ScopeName:      "_default",
			}, func(result *DeleteResult, e error) {
				waitCh <- e
			})
			if err != nil {
				return err
			}

			return <-waitCh
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Failed running mutations, %v", err)
	}

	return deletions
}

func triggerExpirations(t *testing.T, expirations map[string]uint32, collectionName string) {
	var g errgroup.Group
	// we need to kick the server to expire these docs
	for k := range expirations {
		key := k
		g.Go(func() error {
			waitCh := make(chan error)
			_, err := globalDCPOpAgent.GetEx(GetOptions{
				Key:            []byte(key),
				CollectionName: collectionName,
				ScopeName:      "_default",
			}, func(result *GetResult, e error) {
				if !IsErrorStatus(e, StatusKeyNotFound) {
					waitCh <- e
				}
				waitCh <- nil
			})
			if err != nil {
				return err
			}

			return <-waitCh
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Failed triggering expirations, %v", err)
	}
}
