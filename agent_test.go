package gocbcore

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbaselabs/gojcbmock"
)

// Gets a set of keys evenly distributed across all server nodes.
// the result is an array of strings, each index representing an index
// of a server
func (agent *Agent) makeDistKeys() (keys []string) {
	// Get the routing information
	cfg := agent.routingInfo.Get()
	keys = make([]string, cfg.clientMux.NumPipelines())
	remaining := len(keys)

	for i := 0; remaining > 0; i++ {
		keyTmp := fmt.Sprintf("DistKey_%d", i)
		// Map the vBucket and server
		vbID := cfg.vbMap.VbucketByKey([]byte(keyTmp))
		srvIx, err := cfg.vbMap.NodeByVbucket(vbID, 0)
		if err != nil || srvIx < 0 || srvIx >= len(keys) || keys[srvIx] != "" {
			continue
		}
		keys[srvIx] = keyTmp
		remaining--
	}
	return
}

const (
	defaultServerVersion = "5.1.0"
)

var globalAgent *testNode
var globalMemdAgent *testNode
var globalDCPAgent *testNode
var globalDCPOpAgent *testNode

func getAgent() *testNode {
	return globalAgent
}

func getAgentnSignaler(t *testing.T) (*testNode, *Signaler) {
	agent := getAgent()
	return agent, agent.getSignaler(t)
}

func TestCidRetries(t *testing.T) {
	agent, s := getAgentnSignaler(t)
	if !agent.SupportsFeature(TestCollectionFeature) || !agent.HasCollectionsSupport() {
		t.Skip("Collections are not supported or not enabled")
	}

	collectionName := "mytestcollectionname"
	scopeName := agent.ScopeName()
	if scopeName == "" {
		scopeName = "_default"
	}

	_, err := testCreateCollection(collectionName, scopeName, agent.bucketName, agent.Agent)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
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
	_, err = testDeleteCollection(collectionName, scopeName, agent.bucketName, agent.Agent, true)
	if err != nil {
		t.Fatalf("Failed to delete collection: %v", err)
	}

	// recreate
	_, err = testCreateCollection(collectionName, scopeName, agent.bucketName, agent.Agent)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Set should succeed as we detect cid unknown, fetch the cid and then retry again. This should happen
	// even if we don't set a retry strategy.
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("test"),
		Value:          []byte("{}"),
		CollectionName: collectionName,
		ScopeName:      agent.ScopeName(),
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
		ScopeName:      agent.ScopeName(),
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

func TestBasicOps(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	// Set
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("test"),
		Value:          []byte("{}"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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

func TestGetReplica(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	// Set
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testReplica"),
		Value:          []byte("{}"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
			CollectionName: agent.CollectionName(),
			ScopeName:      agent.ScopeName(),
		}, func(res *GetReplicaResult, err error) {
			s.Wrap(func() {
				keyNotFound := IsErrorStatus(err, StatusKeyNotFound)
				if err == nil {
					keyExists = true
				} else if err != nil && !keyNotFound {
					s.Fatalf("GetReplica specific returned error that was not key not found: %v", err)
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

func TestGetAnyReplica(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	// Set
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testReplica"),
		Value:          []byte("{}"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		// GetReplica Any
		s.PushOp(agent.GetAnyReplicaEx(GetAnyReplicaOptions{
			Key:            []byte("testReplica"),
			CollectionName: agent.CollectionName(),
			ScopeName:      agent.ScopeName(),
		}, func(res *GetReplicaResult, err error) {
			s.Wrap(func() {
				keyNotFound := IsErrorStatus(err, StatusKeyNotFound)
				if err == nil {
					keyExists = true
				} else if err != nil && !keyNotFound {
					s.Fatalf("GetReplica specific returned error that was not key not found: %v", err)
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
	agent, s := getAgentnSignaler(t)

	oldCas := Cas(0)
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testx"),
		Value:          []byte("{}"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *StoreResult, err error) {
		oldCas = res.Cas
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.ReplaceEx(ReplaceOptions{
		Key:            []byte("testx"),
		Value:          []byte("[]"),
		Cas:            oldCas,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testy"),
		Value:          []byte("{}"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.DeleteEx(DeleteOptions{
		Key:            []byte("testy"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.DeleteEx(DeleteOptions{
		Key:            []byte("testz"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *DeleteResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.AddEx(AddOptions{
		Key:            []byte("testz"),
		Value:          []byte("[]"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	// Counters
	s.PushOp(agent.DeleteEx(DeleteOptions{
		Key:            []byte("testCounters"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *DeleteResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.IncrementEx(CounterOptions{
		Key:            []byte("testCounters"),
		Delta:          5,
		Initial:        11,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	if !agent.SupportsFeature(TestAdjoinFeature) {
		t.Skip("Test does not work against server version due to serverside bug")
	}

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte("there"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.AppendEx(AdjoinOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte(" Frank!"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testExpiry"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(2000 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testExpiry"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
		RetryStrategy:  NewBestEffortRetryStrategy(nil),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !IsErrorStatus(err, StatusKeyNotFound) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	}))
	s.Wait(0)
}

func TestTouch(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testTouch"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *TouchResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testTouch"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(2500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testTouch"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !IsErrorStatus(err, StatusKeyNotFound) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	}))
	s.Wait(0)
}

func TestGetAndTouch(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testGetAndTouch"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *GetAndTouchResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testGetAndTouch"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(2500 * time.Millisecond)

	s.PushOp(agent.GetEx(GetOptions{
		Key:            []byte("testGetAndTouch"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !IsErrorStatus(err, StatusKeyNotFound) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	}))
	s.Wait(0)
}

// This test will lock the document for 1 second, it will then perform set requests for up to 2 seconds,
// the operation should succeed within the 2 seconds.
func TestRetrySet(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testRetrySet"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.ObserveEx(ObserveOptions{
		Key:            []byte("testObserve"),
		ReplicaIdx:     1,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	origMt := MutationToken{}
	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	distkeys := agent.makeDistKeys()
	for _, k := range distkeys {
		s.PushOp(agent.SetEx(SetOptions{
			Key:            []byte(k),
			Value:          []byte("Hello World!"),
			CollectionName: agent.CollectionName(),
			ScopeName:      agent.ScopeName(),
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
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("testXattr"),
		Value:          []byte("{\"x\":\"xattrs\"}"),
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		// TODO: Turn on Macro Expansion part of the xattr test
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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

func TestStats(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	numServers := agent.routingInfo.Get().clientMux.NumPipelines()

	s.PushOp(agent.StatsEx(StatsOptions{
		Key: "",
	}, func(res *StatsResult, err error) {
		s.Wrap(func() {
			if len(res.Servers) != numServers {
				s.Fatalf("Didn't get all stats!")
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
	agent := getAgent()

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
	if !globalAgent.SupportsFeature(TestMemdFeature) {
		t.Skip("Skipping test because memcached buckets not supported")
	}

	// Ensure we can do upserts..
	agent := globalMemdAgent
	s := agent.getSignaler(t)

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
				s.Fatalf("Couldn't get back key: %v", err)
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

	if err != ErrNotSupported {
		t.Fatalf("Expected observe error for memcached bucket!")
	}
}

func TestFlagsRoundTrip(t *testing.T) {
	// Ensure flags are round-tripped with the server correctly.
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.SetEx(SetOptions{
		Key:            []byte("flagskey"),
		Value:          []byte(""),
		Flags:          0x99889988,
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
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
		CollectionName: agent.CollectionName(),
		ScopeName:      agent.ScopeName(),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Couldn't get back key: %v", err)
			}
			if res.Flags != 0x99889988 {
				s.Fatalf("flags failed to round-trip")
			}
		})
	}))
	s.Wait(0)
}

func TestMetaOps(t *testing.T) {
	// Currently disabled as CouchbaseMock does not support it
	/*
		agent, s := getAgentnSignaler(t)

		var currentCas Cas

		// Set
		s.PushOp(agent.Set([]byte("test"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Set operation failed")
				}
				if cas == Cas(0) {
					s.Fatalf("Invalid cas received")
				}

				currentCas = cas
			})
		})
		s.Wait(0)

		// GetMeta
		s.PushOp(agent.GetMeta([]byte("test"), func(value []byte, flags uint32, cas Cas, expiry uint32, seqNo SeqNo, dataType uint8, deleted uint32, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("GetMeta operation failed")
				}
				if expiry != 0 {
					s.Fatalf("Invalid expiry received")
				}
				if deleted != 0 {
					s.Fatalf("Invalid deleted flag received")
				}
				if cas != currentCas {
					s.Fatalf("Invalid cas received")
				}
			})
		})
		s.Wait(0)
	*/
}

func TestPing(t *testing.T) {
	agent, s := getAgentnSignaler(t)

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
	agent, _ := getAgentnSignaler(t)

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

func TestAlternateAddressesEmptyStringConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	agent := Agent{}
	cfg := agent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if agent.networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", agent.networkType)
	}

	for i, server := range cfg.kvServerList {
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

	agent := Agent{}
	agent.networkType = "auto"
	cfg := agent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if agent.networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", agent.networkType)
	}

	for i, server := range cfg.kvServerList {
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

	agent := Agent{}
	agent.networkType = "auto"
	cfg := agent.buildFirstRouteConfig(cfgBk, "172.17.0.4:11210")

	if globalAgent.networkType != "default" {
		t.Fatalf("Expected agent networkType to be external, was %s", agent.networkType)
	}

	for i, server := range cfg.kvServerList {
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

	agent := Agent{}
	agent.networkType = "default"
	cfg := agent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if agent.networkType != "default" {
		t.Fatalf("Expected agent networkType to be default, was %s", agent.networkType)
	}

	for i, server := range cfg.kvServerList {
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

	agent := Agent{}
	agent.networkType = "external"
	cfg := agent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if agent.networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", agent.networkType)
	}

	for i, server := range cfg.kvServerList {
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

	agent := Agent{}
	agent.networkType = "external"
	cfg := agent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if agent.networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", agent.networkType)
	}

	for i, server := range cfg.kvServerList {
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

	agent := Agent{}
	agent.networkType = "invalid"
	cfg := agent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if agent.networkType != "invalid" {
		t.Fatalf("Expected agent networkType to be invalid, was %s", agent.networkType)
	}

	if cfg.IsValid() {
		t.Fatalf("Expected route config to be invalid, was valid")
	}
	if len(cfg.kvServerList) != 0 {
		t.Fatalf("Expected kvServerList to be empty, had %d items", len(cfg.kvServerList))
	}
}

type testLogger struct {
	Parent   Logger
	LogCount []uint64
}

func (logger *testLogger) Log(level LogLevel, offset int, format string, v ...interface{}) error {
	if level >= 0 && level < LogMaxVerbosity {
		atomic.AddUint64(&logger.LogCount[level], 1)
	}

	return logger.Parent.Log(level, offset+1, format, v...)
}

func createTestLogger() *testLogger {
	return &testLogger{
		Parent:   VerboseStdioLogger(),
		LogCount: make([]uint64, LogMaxVerbosity),
	}
}

func TestMain(m *testing.M) {
	initialGoroutineCount := runtime.NumGoroutine()

	memdservers := flag.String("memdservers", "", "Comma separated list of connection strings to connect to for real memd servers")
	httpservers := flag.String("httpservers", "", "Comma separated list of connection strings to connect to for real http servers")
	bucketName := flag.String("bucket", "default", "The bucket to use to test against")
	memdBucketName := flag.String("memd-bucket", "memd", "The memd bucket to use to test against")
	dcpBucketName := flag.String("dcp-bucket", "", "The dcp bucket to use to test against")
	user := flag.String("user", "", "The username to use to authenticate when using a real server")
	password := flag.String("pass", "", "The password to use to authenticate when using a real server")
	version := flag.String("version", "", "The server version being tested against (major.minor.patch.build_edition)")
	collectionName := flag.String("collection-name", "", "The collection name to use to test with collections")
	collectionsDcp := flag.Bool("dcp-collections", false, "Whether or not to use collections for dcp tests")
	disableLogger := flag.Bool("disable-logger", false, "Whether or not to disable the logger")
	flag.Parse()

	var logger *testLogger
	if !*disableLogger {
		// Set up our special logger which logs the log level count
		logger = createTestLogger()
		SetLogger(logger)
	}

	if (*memdservers == "") != (*httpservers == "") {
		panic("If one of memdservers or httpservers is present then both must be present")
	}

	var err error
	var memdAddrs []string
	var httpAddrs []string

	var mock *gojcbmock.Mock
	var httpAuth *PasswordAuthProvider
	var memdAuth *PasswordAuthProvider
	if *memdservers == "" {
		if *version != "" {
			panic("Version cannot be specified with mock")
		}
		mpath, err := gojcbmock.GetMockPath()
		if err != nil {
			panic(err.Error())
		}

		mock, err = gojcbmock.NewMock(mpath, 4, 1, 64, []gojcbmock.BucketSpec{
			{Name: "default", Type: gojcbmock.BCouchbase},
			{Name: "memd", Type: gojcbmock.BMemcached},
		}...)

		if err != nil {
			panic(err.Error())
		}
		for _, mcport := range mock.MemcachedPorts() {
			memdAddrs = append(memdAddrs, fmt.Sprintf("127.0.0.1:%d", mcport))
		}

		httpAddrs = []string{fmt.Sprintf("127.0.0.1:%d", mock.EntryPort)}

		*version = mock.Version()

		httpAuth = &PasswordAuthProvider{
			Username: "default",
			Password: "",
		}
		memdAuth = &PasswordAuthProvider{
			Username: "memd",
			Password: "",
		}
	} else {
		memdAddrs = strings.Split(*memdservers, ",")
		httpAddrs = strings.Split(*httpservers, ",")

		if *version == "" {
			*version = defaultServerVersion
		}

		httpAuth = &PasswordAuthProvider{
			Username: *user,
			Password: *password,
		}
		memdAuth = &PasswordAuthProvider{
			Username: *user,
			Password: *password,
		}
	}

	nodeVersion, err := nodeVersionFromString(*version)
	if err != nil {
		panic(fmt.Sprintf("Failed to get node version from string: %v", err))
	}

	useCollections := false
	if *collectionName != "" {
		useCollections = true
	}

	agentConfig := &AgentConfig{
		MemdAddrs: memdAddrs,
		HTTPAddrs: httpAddrs,
		TLSConfig: nil,
		// BucketName:           *bucketName,
		Auth:                 httpAuth,
		AuthMechanisms:       []AuthMechanism{PlainAuthMechanism},
		ConnectTimeout:       5 * time.Second,
		ServerConnectTimeout: 1 * time.Second,
		UseMutationTokens:    true,
		UseKvErrorMaps:       true,
		UseEnhancedErrors:    true,
		UseCollections:       useCollections,
	}

	agent, err := CreateAgent(agentConfig)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to server, %v", err))
	}
	err = agent.SelectBucket(*bucketName, time.Now().Add(2*time.Second))
	if err != nil {
		panic(fmt.Sprintf("Failed to select bucket, %v", err))
	}
	globalAgent = &testNode{
		Agent:          agent,
		Mock:           mock,
		Version:        nodeVersion,
		collectionName: *collectionName,
	}
	timer := time.NewTimer(5 * time.Second)
	ch := make(chan error)
	op, err := globalAgent.PingKvEx(PingKvOptions{}, func(results *PingKvResult, err error) {
		if err != nil {
			ch <- err
		}

		for _, result := range results.Services {
			if result.Error != nil {
				ch <- result.Error
			}
		}

		ch <- nil
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to ping against bucket!: %v", err))
	}

	select {
	case <-timer.C:
		op.Cancel()
		panic("Timed out ping against bucket!")
	case err := <-ch:
		timer.Stop()
		if err != nil {
			panic(fmt.Sprintf("Failed to ping against bucket!: %v", err))
		}
	}

	if globalAgent.SupportsFeature(TestMemdFeature) {
		memdAgentConfig := &AgentConfig{}
		*memdAgentConfig = *agentConfig
		memdAgentConfig.UseCollections = false // memcached buckets don't have support for collections
		memdAgentConfig.BucketName = *memdBucketName
		memdAgentConfig.Auth = memdAuth
		memdAgentConfig.AuthMechanisms = []AuthMechanism{PlainAuthMechanism}
		memdAgent, err := CreateAgent(memdAgentConfig)
		if err != nil {
			panic(fmt.Sprintf("Failed to connect to memcached bucket!: %v", err))
		}
		globalMemdAgent = &testNode{
			Agent:   memdAgent,
			Mock:    mock,
			Version: nodeVersion,
		}

		timer := time.NewTimer(5 * time.Second)
		memdCh := make(chan error)
		op, err := memdAgent.PingKvEx(PingKvOptions{}, func(results *PingKvResult, err error) {
			if err != nil {
				memdCh <- err
			}

			for _, result := range results.Services {
				if result.Error != nil {
					memdCh <- result.Error
				}
			}

			memdCh <- nil
		})
		if err != nil {
			panic(fmt.Sprintf("Failed to ping against memcached bucket!: %v", err))
		}

		select {
		case <-timer.C:
			op.Cancel()
			panic("Timed out ping against memcached bucket!")
		case err := <-memdCh:
			timer.Stop()
			if err != nil {
				panic(fmt.Sprintf("Failed to ping against memcached bucket!: %v", err))
			}
		}
	}

	if *dcpBucketName != "" && globalAgent.SupportsFeature(TestDCPFeature) {
		dcpAgentConfig := &AgentConfig{}
		*dcpAgentConfig = *agentConfig
		dcpAgentConfig.UseCollections = *collectionsDcp
		dcpAgentConfig.EnableStreamID = *collectionsDcp
		dcpAgentConfig.BucketName = *dcpBucketName

		if globalAgent.SupportsFeature(TestDCPExpiryFeature) {
			dcpAgentConfig.UseDcpExpiry = true
		}

		dcpAgent, err := CreateDcpAgent(dcpAgentConfig, "dcp-stream", DcpOpenFlagProducer)
		if err != nil {
			panic("Failed to connect to server")
		}
		globalDCPAgent = &testNode{
			Agent:   dcpAgent,
			Mock:    mock,
			Version: nodeVersion,
		}
		dcpOpAgent, err := CreateAgent(dcpAgentConfig)
		if err != nil {
			panic("Failed to connect to server")
		}
		globalDCPOpAgent = &testNode{
			Agent:   dcpOpAgent,
			Mock:    mock,
			Version: nodeVersion,
		}

	}

	result := m.Run()

	err = agent.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to shut down global agent: %s", err))
	}

	if globalMemdAgent != nil {
		err = globalMemdAgent.Close()
		if err != nil {
			panic(fmt.Sprintf("Failed to shut down global memcached agent: %s", err))
		}
	}

	if globalDCPAgent != nil {
		err = globalDCPAgent.Close()
		if err != nil {
			panic(fmt.Sprintf("Failed to shut down global dcp agent: %s", err))
		}

		err = globalDCPOpAgent.Close()
		if err != nil {
			panic(fmt.Sprintf("Failed to shut down global dcp op agent: %s", err))
		}
	}

	if logger != nil {
		log.Printf("Log Messages Emitted:")
		for i := 0; i < int(LogMaxVerbosity); i++ {
			log.Printf("  (%s): %d", logLevelToString(LogLevel(i)), logger.LogCount[i])
		}

		abnormalLogCount := logger.LogCount[LogError] + logger.LogCount[LogWarn]
		if abnormalLogCount > 0 {
			log.Printf("Detected unexpected logging, failing")
			result = 1
		}
	}

	// Loop for at most a second checking for goroutines leaks, this gives any HTTP goroutines time to shutdown
	start := time.Now()
	var finalGoroutineCount int
	for time.Now().Sub(start) <= 1*time.Second {
		runtime.Gosched()
		finalGoroutineCount = runtime.NumGoroutine()
		if finalGoroutineCount == initialGoroutineCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if finalGoroutineCount != initialGoroutineCount {
		log.Printf("Detected a goroutine leak (%d before != %d after), failing", initialGoroutineCount, finalGoroutineCount)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		result = 1
	} else {
		log.Printf("No goroutines appear to have leaked (%d before == %d after)", initialGoroutineCount, finalGoroutineCount)
	}

	os.Exit(result)
}

// These functions are likely temporary.

type testManifestWithError struct {
	Manifest Manifest
	Err      error
}

func testCreateCollection(name, scopeName, bucketName string, agent *Agent) (*Manifest, error) {
	data := url.Values{}
	data.Set("name", name)

	req := &HTTPRequest{
		Service: MgmtService,
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s/", bucketName, scopeName),
		Method:  "POST",
		Body:    []byte(data.Encode()),
		Headers: make(map[string]string),
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

	uid, err := strconv.Atoi(respBody.UID)
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
	data := url.Values{}
	data.Set("name", name)

	req := &HTTPRequest{
		Service: MgmtService,
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s/%s", bucketName, scopeName, name),
		Method:  "DELETE",
		Headers: make(map[string]string),
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

	uid, err := strconv.Atoi(respBody.UID)
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
