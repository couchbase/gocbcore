package gocbcore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"strconv"
	"testing"
	"time"
)

/* BUG(brett19): GOCBC-691: Disabled due to known issue with CID error retries
func TestCidRetries(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureCollections)

	agent, s := testGetAgentAndHarness(t)

	bucketName := s.BucketName
	scopeName := s.ScopeName
	collectionName := "testCidRetries"

	_, err := testCreateCollection(collectionName, scopeName, bucketName, agent)
	if err != nil {
		t.Logf("Failed to create collection: %v", err)
	}

	// prime the cid map cache
	s.PushOp(agent.GetCollectionID(scopeName, collectionName, GetCollectionIDOptions{},
		func(manifestID uint64, collectionID uint32, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Get CID operation failed: %v", err)
				}
			})
		}),
	)
	s.Wait(0)

	// delete the collection
	_, err = testDeleteCollection(collectionName, scopeName, bucketName, agent, true)
	if err != nil {
		t.Fatalf("Failed to delete collection: %v", err)
	}

	// recreate
	_, err = testCreateCollection(collectionName, scopeName, bucketName, agent)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Set should succeed as we detect cid unknown, fetch the cid and then retry again. This should happen
	// even if we don't set a retry strategy.
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("test"),
		Value:          []byte("{}"),
		CollectionName: collectionName,
		ScopeName:      scopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	// Get
	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("test"),
		CollectionName: collectionName,
		ScopeName:      scopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}
*/

func TestBasicOps(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	// Set
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("test"),
		Value:          []byte("{}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	// Get
	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("test"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}

func TestCasMismatch(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	// Set
	var cas Cas
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testCasMismatch"),
		Value:          []byte("{}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			cas = res.Cas
		})
	}))
	s.Wait(0)

	// Replace to change cas on the server
	s.PushOp(agent.ReplaceEx(ReplaceOptions{
		Key:            []byte("testCasMismatch"),
		Value:          []byte("{\"key\":\"value\"}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Replace operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	// Replace which should fail with a cas mismatch
	s.PushOp(agent.ReplaceEx(ReplaceOptions{
		Key:            []byte("testCasMismatch"),
		Value:          []byte("{\"key\":\"value2\"}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
		Cas:            cas,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err == nil {
				s.Fatalf("Set operation succeeded but should have failed")
			}

			if !errors.Is(err, ErrCasMismatch) {
				t.Fatalf("Expected CasMismatch error but was %v", err)
			}
		})
	}))
	s.Wait(0)
}

func TestGetReplica(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureReplicas)
	agent, s := testGetAgentAndHarness(t)

	// Set
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testReplica"),
		Value:          []byte("{}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	retries := 0
	keyExists := false
	for {
		s.PushOp(agent.GetOneReplicaEx(GetOneReplicaOptions{
			Key:            []byte("testReplica"),
			ReplicaIdx:     1,
			CollectionName: s.CollectionName,
			ScopeName:      s.ScopeName,
		}, func(res *GetReplicaResult, err error) {
			s.Wrap(func() {
				keyNotFound := errors.Is(err, ErrDocumentNotFound)
				if err == nil {
					keyExists = true
				} else if err != nil && !keyNotFound {
					s.Fatalf("GetReplica specific returned error that was not document not found: %v", err)
				}
				if !keyNotFound && res.Cas == Cas(0) {
					s.Fatalf("Invalid cas received")
				}
			})
		}))
		s.Wait(0)
		if keyExists {
			break
		}
		retries++
		if retries >= 5 {
			t.Fatalf("GetReplica could not locate key")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestBasicReplace(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	oldCas := Cas(0)
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testx"),
		Value:          []byte("{}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		oldCas = res.Cas
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.ReplaceEx(ReplaceOptions{
		Key:            []byte("testx"),
		Value:          []byte("[]"),
		Cas:            oldCas,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Replace operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}

func TestBasicRemove(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testy"),
		Value:          []byte("{}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.DeleteEx(DeleteOptions{
		Key:            []byte("testy"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *DeleteResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Remove operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func TestBasicInsert(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.DeleteEx(DeleteOptions{
		Key:            []byte("testz"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *DeleteResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.AddEx(AddOptions{
		Key:            []byte("testz"),
		Value:          []byte("[]"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Add operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}

func TestBasicCounters(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	// Counters
	s.PushOp(agent.DeleteEx(DeleteOptions{
		Key:            []byte("testCounters"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *DeleteResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.IncrementEx(CounterOptions{
		Key:            []byte("testCounters"),
		Delta:          5,
		Initial:        11,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *CounterResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if res.Value != 11 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.IncrementEx(CounterOptions{
		Key:            []byte("testCounters"),
		Delta:          5,
		Initial:        22,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *CounterResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if res.Value != 16 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.DecrementEx(CounterOptions{
		Key:            []byte("testCounters"),
		Delta:          3,
		Initial:        65,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *CounterResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if res.Value != 13 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	}))
	s.Wait(0)
}

func TestBasicAdjoins(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureAdjoin)

	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte("there"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.AppendEx(AdjoinOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte(" Frank!"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *AdjoinResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Append operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.PrependEx(AdjoinOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte("Hello "),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *AdjoinResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Prepend operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testAdjoins"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}

			if string(res.Value) != "Hello there Frank!" {
				s.Fatalf("Adjoin operations did not behave")
			}
		})
	}))
	s.Wait(0)
}

func TestExpiry(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testExpiry"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.TimeTravel(2000 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testExpiry"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
		RetryStrategy:  NewBestEffortRetryStrategy(nil),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentNotFound) {
				s.Fatalf("Get should have returned document not found")
			}
		})
	}))
	s.Wait(0)
}

func TestTouch(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testTouch"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.TouchEx(TouchOptions{
		Key:            []byte("testTouch"),
		Expiry:         3,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *TouchResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testTouch"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	}))
	s.Wait(0)

	s.TimeTravel(2500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testTouch"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentNotFound) {
				s.Fatalf("Get should have returned document not found")
			}
		})
	}))
	s.Wait(0)
}

func TestGetAndTouch(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testGetAndTouch"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetAndTouchEx(GetAndTouchOptions{
		Key:            []byte("testGetAndTouch"),
		Expiry:         3,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetAndTouchResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testGetAndTouch"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	}))
	s.Wait(0)

	s.TimeTravel(2500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testGetAndTouch"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentNotFound) {
				s.Fatalf("Get should have returned document not found")
			}
		})
	}))
	s.Wait(0)
}

// This test will lock the document for 1 second, it will then perform set requests for up to 2 seconds,
// the operation should succeed within the 2 seconds.
func TestRetrySet(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testRetrySet"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetAndLockEx(GetAndLockOptions{
		Key:            []byte("testRetrySet"),
		LockTime:       1,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetAndLockResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("GetAndLock operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testRetrySet"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
		RetryStrategy:  NewBestEffortRetryStrategy(nil),
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func TestObserve(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureReplicas)

	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.ObserveEx(ObserveOptions{
		Key:            []byte("testObserve"),
		ReplicaIdx:     1,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *ObserveResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Observe operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func TestObserveSeqNo(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureReplicas)

	agent, s := testGetAgentAndHarness(t)

	origMt := MutationToken{}
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Initial set operation failed: %v", err)
			}

			mt := res.MutationToken
			if mt.VbUUID == 0 && mt.SeqNo == 0 {
				s.Skipf("ObserveSeqNo not supported by server")
			}

			origMt = mt
		})
	}))
	s.Wait(0)

	origCurSeqNo := SeqNo(0)
	vbID := agent.KeyToVbucket([]byte("testObserve"))
	s.PushOp(agent.ObserveVbEx(ObserveVbOptions{
		VbID:       vbID,
		VbUUID:     origMt.VbUUID,
		ReplicaIdx: 1,
	}, func(res *ObserveVbResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("ObserveSeqNo operation failed: %v", err)
			}

			origCurSeqNo = res.CurrentSeqNo
		})
	}))
	s.Wait(0)

	newMt := MutationToken{}
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Second set operation failed: %v", err)
			}

			newMt = res.MutationToken
		})
	}))
	s.Wait(0)

	vbID = agent.KeyToVbucket([]byte("testObserve"))
	s.PushOp(agent.ObserveVbEx(ObserveVbOptions{
		VbID:       vbID,
		VbUUID:     newMt.VbUUID,
		ReplicaIdx: 1,
	}, func(res *ObserveVbResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("ObserveSeqNo operation failed: %v", err)
			}
			if res.CurrentSeqNo < origCurSeqNo {
				s.Fatalf("SeqNo does not appear to be working")
			}
		})
	}))
	s.Wait(0)
}

func TestRandomGet(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	distkeys := s.MakeDistKeys(agent)
	for _, k := range distkeys {
		s.PushOp(agent.SetEx(SetOptions{
			Key:            []byte(k),
			Value:          []byte("Hello World!"),
			CollectionName: s.CollectionName,
			ScopeName:      s.ScopeName,
		}, func(res *StoreResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Couldn't store some items: %v", err)
				}
			})
		}))
		s.Wait(0)
	}

	s.PushOp(agent.GetRandomEx(GetRandomOptions{}, func(res *GetRandomResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if len(res.Key) == 0 {
				s.Fatalf("Invalid key returned")
			}
			if len(res.Value) == 0 {
				s.Fatalf("No value returned")
			}
		})
	}))
	s.Wait(0)
}

func TestSubdocXattrs(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testXattr"),
		Value:          []byte("{\"x\":\"xattrs\"}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	mutateOps := []SubDocOp{
		{
			Op:    SubDocOpDictSet,
			Flags: SubdocFlagXattrPath | SubdocFlagMkDirP,
			Path:  "xatest.test",
			Value: []byte("\"test value\""),
		},
		/*{
			Op: SubDocOpDictSet,
			Flags: SubdocFlagXattrPath | SubdocFlagExpandMacros | SubdocFlagMkDirP,
			Path: "xatest.rev",
			Value: []byte("\"${Mutation.CAS}\""),
		},*/
		{
			Op:    SubDocOpDictSet,
			Flags: SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}
	s.PushOp(agent.MutateInEx(MutateInOptions{
		Key:            []byte("testXattr"),
		Ops:            mutateOps,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Mutate operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	lookupOps := []SubDocOp{
		{
			Op:    SubDocOpGet,
			Flags: SubdocFlagXattrPath,
			Path:  "xatest",
		},
		{
			Op:    SubDocOpGet,
			Flags: SubdocFlagNone,
			Path:  "x",
		},
	}
	s.PushOp(agent.LookupInEx(LookupInOptions{
		Key:            []byte("testXattr"),
		Ops:            lookupOps,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *LookupInResult, err error) {
		s.Wrap(func() {
			if len(res.Ops) != 2 {
				s.Fatalf("Lookup operation wrong count")
			}
			if res.Ops[0].Err != nil {
				s.Fatalf("Lookup operation 1 failed: %v", res.Ops[0].Err)
			}
			if res.Ops[1].Err != nil {
				s.Fatalf("Lookup operation 2 failed: %v", res.Ops[1].Err)
			}

			/*
				xatest := fmt.Sprintf(`{"test":"test value","rev":"0x%016x"}`, cas)
				if !bytes.Equal(res[0].Value, []byte(xatest)) {
					s.Fatalf("Unexpected xatest value %s (doc) != %s (header)", res[0].Value, xatest)
				}
			*/
			if !bytes.Equal(res.Ops[0].Value, []byte(`{"test":"test value"}`)) {
				s.Fatalf("Unexpected xatest value %s", res.Ops[0].Value)
			}
			if !bytes.Equal(res.Ops[1].Value, []byte(`"x value"`)) {
				s.Fatalf("Unexpected document value %s", res.Ops[1].Value)
			}
		})
	}))
	s.Wait(0)
}

func TestSubdocXattrsReorder(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testXattrReorder"),
		Value:          []byte("{\"x\":\"xattrs\", \"y\":\"yattrs\"}"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	// This should reorder the ops before sending to the server.
	mutateOps := []SubDocOp{
		{
			Op:    SubDocOpDictSet,
			Flags: SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
		{
			Op:    SubDocOpDictSet,
			Flags: SubdocFlagXattrPath | SubdocFlagMkDirP,
			Path:  "xatest.test",
			Value: []byte("\"test value\""),
		},
		{
			Op:    SubDocOpDictSet,
			Flags: SubdocFlagXattrPath | SubdocFlagMkDirP,
			Path:  "xatest.ytest",
			Value: []byte("\"test value2\""),
		},
	}
	s.PushOp(agent.MutateInEx(MutateInOptions{
		Key:            []byte("testXattrReorder"),
		Ops:            mutateOps,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Mutate operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if len(res.Ops) != 3 {
				s.Fatalf("MutateIn operation wrong count was %d", len(res.Ops))
			}
			if res.Ops[0].Err != nil {
				s.Fatalf("MutateIn operation 1 failed: %v", res.Ops[0].Err)
			}
			if res.Ops[1].Err != nil {
				s.Fatalf("MutateIn operation 2 failed: %v", res.Ops[1].Err)
			}
			if res.Ops[2].Err != nil {
				s.Fatalf("MutateIn operation 3 failed: %v", res.Ops[2].Err)
			}
		})
	}))
	s.Wait(0)

	lookupOps := []SubDocOp{
		{
			Op:    SubDocOpGet,
			Flags: SubdocFlagXattrPath,
			Path:  "xatest.test",
		},
		{
			Op:    SubDocOpGet,
			Flags: SubdocFlagNone,
			Path:  "x",
		},
		{
			Op:    SubDocOpGet,
			Flags: SubdocFlagXattrPath,
			Path:  "xatest.ytest",
		},
	}
	s.PushOp(agent.LookupInEx(LookupInOptions{
		Key:            []byte("testXattrReorder"),
		Ops:            lookupOps,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *LookupInResult, err error) {
		s.Wrap(func() {
			if len(res.Ops) != 3 {
				s.Fatalf("Lookup operation wrong count: %d", len(res.Ops))
			}
			if res.Ops[0].Err != nil {
				s.Fatalf("Lookup operation 1 failed: %v", res.Ops[0].Err)
			}
			if res.Ops[1].Err != nil {
				s.Fatalf("Lookup operation 2 failed: %v", res.Ops[1].Err)
			}
			if res.Ops[2].Err != nil {
				s.Fatalf("Lookup operation 3 failed: %v", res.Ops[2].Err)
			}

			if !bytes.Equal(res.Ops[0].Value, []byte(`"test value"`)) {
				s.Fatalf("Unexpected xatest.test value %s", res.Ops[0].Value)
			}
			if !bytes.Equal(res.Ops[1].Value, []byte(`"x value"`)) {
				s.Fatalf("Unexpected document value %s", res.Ops[1].Value)
			}
			if !bytes.Equal(res.Ops[2].Value, []byte(`"test value2"`)) {
				s.Fatalf("Unexpected xatest.ytest value %s", res.Ops[2].Value)
			}
		})
	}))
	s.Wait(0)
}

func TestStats(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	numServers := agent.NumServers()

	s.PushOp(agent.StatsEx(StatsOptions{
		Key: "",
	}, func(res *StatsResult, err error) {
		s.Wrap(func() {
			if len(res.Servers) != numServers {
				s.Fatalf("Didn't Get all stats!")
			}
			for srv, curStats := range res.Servers {
				if curStats.Error != nil {
					s.Fatalf("Got error %v in stats for %s", curStats.Error, srv)
				}

				if curStats.Stats == nil || len(curStats.Stats) == 0 {
					s.Fatalf("Got no stats in stats for %s", srv)
				}
			}
		})
	}))
	s.Wait(0)
}

func TestGetHttpEps(t *testing.T) {
	agent, _ := testGetAgentAndHarness(t)

	// Relies on a 3.0.0+ server
	n1qlEpList := agent.N1qlEps()
	if len(n1qlEpList) == 0 {
		t.Fatalf("Failed to retrieve N1QL endpoint list")
	}

	mgmtEpList := agent.MgmtEps()
	if len(mgmtEpList) == 0 {
		t.Fatalf("Failed to retrieve N1QL endpoint list")
	}

	capiEpList := agent.CapiEps()
	if len(capiEpList) == 0 {
		t.Fatalf("Failed to retrieve N1QL endpoint list")
	}
}

func TestMemcachedBucket(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureMemd)

	s := testGetHarness(t)
	agent := s.MemdAgent()

	s.PushOp(agent.SetEx(SetOptions{
		Key:   []byte("key"),
		Value: []byte("value"),
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetEx(GetOptions{
		Key: []byte("key"),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Couldn't Get back key: %v", err)
			}
			if string(res.Value) != "value" {
				s.Fatalf("Got back wrong value!")
			}
		})
	}))
	s.Wait(0)

	// Try to perform Observe: should fail since this isn't supported on Memcached buckets
	_, err := agent.ObserveEx(ObserveOptions{
		Key: []byte("key"),
	}, func(res *ObserveResult, err error) {
		s.Wrap(func() {
			s.Fatalf("Scheduling should fail on memcached buckets!")
		})
	})

	if !errors.Is(err, ErrFeatureNotAvailable) {
		t.Fatalf("Expected observe error for memcached bucket!")
	}
}

func TestFlagsRoundTrip(t *testing.T) {
	// Ensure flags are round-tripped with the server correctly.
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("flagskey"),
		Value:          []byte(""),
		Flags:          0x99889988,
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("flagskey"),
		CollectionName: s.CollectionName,
		ScopeName:      s.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Couldn't Get back key: %v", err)
			}
			if res.Flags != 0x99889988 {
				s.Fatalf("flags failed to round-trip")
			}
		})
	}))
	s.Wait(0)
}

func TestMetaOps(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureGetMeta)

	agent, s := testGetAgentAndHarness(t)

	var currentCas Cas

	// Set

	s.PushOp(agent.SetEx(SetOptions{
		Key:   []byte("test"),
		Value: []byte("{}"),
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed")
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}

			currentCas = res.Cas
		})
	}))
	s.Wait(0)

	// GetMeta
	s.PushOp(agent.GetMetaEx(GetMetaOptions{
		Key: []byte("test"),
	}, func(res *GetMetaResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("GetMeta operation failed")
			}
			if res.Expiry != 0 {
				s.Fatalf("Invalid expiry received")
			}
			if res.Deleted != 0 {
				s.Fatalf("Invalid deleted flag received")
			}
			if res.Cas != currentCas {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}

func TestPing(t *testing.T) {
	agent, s := testGetAgentAndHarness(t)

	s.PushOp(agent.PingKvEx(PingKvOptions{}, func(res *PingKvResult, err error) {
		s.Wrap(func() {
			if len(res.Services) == 0 {
				s.Fatalf("Ping report contained no results")
			}
		})
	}))
	s.Wait(5)
}

func TestDiagnostics(t *testing.T) {
	agent, _ := testGetAgentAndHarness(t)

	report, err := agent.Diagnostics()
	if err != nil {
		t.Fatalf("Failed to fetch diagnostics: %s", err)
	}

	if len(report.MemdConns) == 0 {
		t.Fatalf("Diagnostics report contained no results")
	}

	for _, conn := range report.MemdConns {
		if conn.RemoteAddr == "" {
			t.Fatalf("Diagnostic report contained invalid entry")
		}
	}
}

type testAlternateAddressesRouteConfigMgr struct {
	cfg *routeConfig
}

func (taa *testAlternateAddressesRouteConfigMgr) OnNewRouteConfig(cfg *routeConfig) {
	taa.cfg = cfg
}

func TestAlternateAddressesEmptyStringConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{}, func() {
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk, "192.168.132.234:32799")

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func TestAlternateAddressesAutoConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType: "auto",
	}, func() {

	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk, "192.168.132.234:32799")

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func TestAlternateAddressesAutoInternalConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType: "auto",
	}, func() {
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk, "172.17.0.4:11210")

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		t.Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func TestAlternateAddressesDefaultConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType: "default",
	}, func() {

	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk, "192.168.132.234:32799")

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		t.Fatalf("Expected agent networkType to be default, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func TestAlternateAddressesExternalConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType: "external",
	}, func() {

	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk, "192.168.132.234:32799")

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func TestAlternateAddressesExternalConfigNoPorts(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses_without_ports.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType: "external",
	}, func() {
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk, "192.168.132.234:32799")

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func TestAlternateAddressesInvalidConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	var invalid bool
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType: "invalid",
	}, func() {
		invalid = true
	})
	cfgManager.OnNewConfig(cfgBk, "192.168.132.234:32799")

	networkType := cfgManager.NetworkType()
	if networkType != "invalid" {
		t.Fatalf("Expected agent networkType to be invalid, was %s", networkType)
	}

	if !invalid {
		t.Fatalf("Expected route config to be invalid, was valid")
	}
}

// These functions are likely temporary.

type testManifestWithError struct {
	Manifest Manifest
	Err      error
}

func testCreateCollection(name, scopeName, bucketName string, agent *Agent) (*Manifest, error) {
	if scopeName == "" {
		scopeName = "_default"
	}
	if name == "" {
		name = "_default"
	}

	data := url.Values{}
	data.Set("name", name)

	req := &HTTPRequest{
		Service: MgmtService,
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s/", bucketName, scopeName),
		Method:  "POST",
		Body:    []byte(data.Encode()),
		Headers: make(map[string]string),
		Timeout: 10 * time.Second,
	}

	req.Headers["Content-Type"] = "application/x-www-form-urlencoded"

	resp, err := agent.DoHTTPRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("could not create collection, status code: %d", resp.StatusCode)
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close response body")
		}
		return nil, fmt.Errorf("could not create collection, %s", string(data))
	}

	respBody := struct {
		UID string `json:"uid"`
	}{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&respBody)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	uid, err := strconv.ParseInt(respBody.UID, 16, 64)
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(20 * time.Second)
	waitCh := make(chan testManifestWithError)
	go waitForManifest(agent, uint64(uid), waitCh)

	for {
		select {
		case <-timer.C:
			return nil, errors.New("wait time for collection to become available expired")
		case manifest := <-waitCh:
			if manifest.Err != nil {
				return nil, manifest.Err
			}

			return &manifest.Manifest, nil
		}
	}
}

func testDeleteCollection(name, scopeName, bucketName string, agent *Agent, waitForDeletion bool) (*Manifest, error) {
	if scopeName == "" {
		scopeName = "_default"
	}
	if name == "" {
		name = "_default"
	}

	data := url.Values{}
	data.Set("name", name)

	req := &HTTPRequest{
		Service: MgmtService,
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s/%s", bucketName, scopeName, name),
		Method:  "DELETE",
		Headers: make(map[string]string),
		Timeout: 10 * time.Second,
	}

	resp, err := agent.DoHTTPRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("could not delete collection, status code: %d", resp.StatusCode)
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close response body")
		}
		return nil, fmt.Errorf("could not delete collection, %s", string(data))
	}

	respBody := struct {
		UID string `json:"uid"`
	}{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&respBody)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	uid, err := strconv.ParseInt(respBody.UID, 16, 64)
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(20 * time.Second)
	waitCh := make(chan testManifestWithError)
	go waitForManifest(agent, uint64(uid), waitCh)

	for {
		select {
		case <-timer.C:
			return nil, errors.New("wait time for collection to become deleted expired")
		case manifest := <-waitCh:
			if manifest.Err != nil {
				return nil, manifest.Err
			}

			return &manifest.Manifest, nil
		}
	}

}

func waitForManifest(agent *Agent, manifestID uint64, manifestCh chan testManifestWithError) {
	var manifest Manifest
	for manifest.UID != manifestID {
		setCh := make(chan struct{})
		agent.GetCollectionManifest(GetCollectionManifestOptions{}, func(bytes []byte, err error) {
			if err != nil {
				log.Println(err.Error())
				close(setCh)
				manifestCh <- testManifestWithError{Err: err}
				return
			}

			err = json.Unmarshal(bytes, &manifest)
			if err != nil {
				log.Println(err.Error())
				close(setCh)
				manifestCh <- testManifestWithError{Err: err}
				return
			}

			if manifest.UID == manifestID {
				close(setCh)
				manifestCh <- testManifestWithError{Manifest: manifest}
				return
			}
			setCh <- struct{}{}
		})
		<-setCh
		time.Sleep(500 * time.Millisecond)
	}
}
