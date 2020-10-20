package gocbcore

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
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
	mutationsRcvd   int32
	deletionsRcvd   int32
	expirationsRcvd int32
	mutations       map[string]testDCPMutation
	deletions       map[string]testDCPMutation
	expirations     map[string]testDCPMutation
	mlock           sync.Mutex
	dlock           sync.Mutex
	elock           sync.Mutex
}

func (so *testObserver) SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, snapshotType SnapshotState) {
}

func (so *testObserver) Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	key, value []byte) {
	so.mlock.Lock()
	so.mutations[string(key)] = testDCPMutation{
		key:   key,
		value: value,
		cas:   cas,
	}
	so.mutationsRcvd++
	so.mlock.Unlock()
}

func (so *testObserver) Deletion(seqNo, revNo, cas uint64, datatype uint8, vbId uint16, key, value []byte) {
	so.dlock.Lock()
	so.deletions[string(key)] = testDCPMutation{
		key:   key,
		value: value,
		cas:   cas,
	}
	so.deletionsRcvd++
	so.dlock.Unlock()
}

func (so *testObserver) Expiration(seqNo, revNo, cas uint64, vbId uint16, key []byte) {
	so.elock.Lock()
	so.expirations[string(key)] = testDCPMutation{
		key: key,
		cas: cas,
	}
	so.expirationsRcvd++
	so.elock.Unlock()
}

func (so *testObserver) End(vbId uint16, err error) {
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
		mutations:   make(map[string]testDCPMutation),
		deletions:   make(map[string]testDCPMutation),
		expirations: make(map[string]testDCPMutation),
	}
	numMutations := 25
	numDeletions := 10
	numExpirations := 5

	var deletions map[string]uint32
	var mutations map[string]uint32
	var expirations map[string]uint32

	openStreams(t, observer)

	// do our best to tidy up otherwise we'll hit nil pointers when we try to close the agents
	defer func() {
		for i := 0; i < globalDCPAgent.numVbuckets; i++ {
			waitCh := make(chan error)
			globalDCPAgent.CloseStream(uint16(i), func(e error) {
				waitCh <- e
			})

			err := <-waitCh
			if err != nil {
				t.Errorf("CloseStream failed, %v", err)
			}
		}
	}()

	mutations, expirations = doMutations(t, numMutations, numExpirations)

	err := waitForChanges(&observer.mutationsRcvd, numMutations)
	if err != nil {
		t.Fatalf("Failed waiting for mutations, %v", err)
	}

	if globalAgent.SupportsFeature(TestDCPExpiryFeature) {
		// wait for expirations to take place
		time.Sleep(2 * time.Second)

		triggerExpirations(t, expirations)

		err = waitForChanges(&observer.expirationsRcvd, numExpirations)
		if err != nil {
			t.Fatalf("Failed waiting for expirations, %v", err)
		}
	}

	deletions = doDeletions(t, numDeletions, mutations)

	err = waitForChanges(&observer.deletionsRcvd, numDeletions)
	if err != nil {
		t.Fatalf("Failed waiting for deletions, %v", err)
	}

	for k, v := range mutations {
		observed, ok := observer.mutations[k]
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

	for k := range deletions {
		observed, ok := observer.deletions[k]
		if !ok {
			t.Errorf("Observer missing deletion entry for %s", k)
			continue
		}

		if observed.cas == 0 {
			t.Errorf("%s deletion had cas value of 0", k)
		}
	}

	if globalAgent.SupportsFeature(TestDCPExpiryFeature) {
		for k := range expirations {
			observed, ok := observer.expirations[k]
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
			if *metric == int32(numChanges) {
				return nil
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func openStreams(t *testing.T, observer *testObserver) {
	var g errgroup.Group
	var ops []PendingOp
	for i := 0; i < globalDCPAgent.numVbuckets; i++ {
		vbucket := i // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			waitCh := make(chan error)
			op, err := globalDCPAgent.OpenStream(uint16(vbucket), DcpStreamAddFlagActiveOnly, 0, 0, math.MaxInt64, 0, 0,
				observer, func(entries []FailoverEntry, e error) {
					waitCh <- e
				})
			if err != nil {
				return err
			}
			ops = append(ops, op)

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

func doMutations(t *testing.T, numMutations, numExpirations int) (map[string]uint32, map[string]uint32) {
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
				Key:    key,
				Value:  randomBytes,
				Expiry: expiry,
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

func doDeletions(t *testing.T, numDeletions int, mutations map[string]uint32) map[string]uint32 {
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
				Key: key,
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

func triggerExpirations(t *testing.T, expirations map[string]uint32) {
	var g errgroup.Group
	// we need to kick the server to expire these docs
	for k := range expirations {
		key := k
		g.Go(func() error {
			waitCh := make(chan error)
			_, err := globalDCPOpAgent.GetEx(GetOptions{
				Key: []byte(key),
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
