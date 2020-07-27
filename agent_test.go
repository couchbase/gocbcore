package gocbcore

import (
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
)

func (suite *StandardTestSuite) TestCidRetries() {
	suite.EnsureSupportsFeature(TestFeatureCollections)

	agent, s := suite.GetAgentAndHarness()

	bucketName := suite.BucketName
	scopeName := suite.ScopeName
	collectionName := "testCidRetries"

	_, err := testCreateCollection(collectionName, scopeName, bucketName, agent)
	if err != nil {
		suite.T().Logf("Failed to create collection: %v", err)
	}

	// prime the cid map cache
	s.PushOp(agent.GetCollectionID(scopeName, collectionName, GetCollectionIDOptions{},
		func(result *GetCollectionIDResult, err error) {
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
		suite.T().Fatalf("Failed to delete collection: %v", err)
	}

	// recreate
	_, err = testCreateCollection(collectionName, scopeName, bucketName, agent)
	if err != nil {
		suite.T().Fatalf("Failed to create collection: %v", err)
	}

	// Set should succeed as we detect cid unknown, fetch the cid and then retry again. This should happen
	// even if we don't set a retry strategy.
	s.PushOp(agent.Set(SetOptions{
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
	s.PushOp(agent.Get(GetOptions{
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

func (suite *StandardTestSuite) TestBasicOps() {
	agent, s := suite.GetAgentAndHarness()

	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("test"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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
	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("test"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "test")
			suite.AssertOpSpan(nilParents[1], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "test")
		}
	}
}

func (suite *StandardTestSuite) TestCasMismatch() {
	agent, s := suite.GetAgentAndHarness()

	// Set
	var cas Cas
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testCasMismatch"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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
	s.PushOp(agent.Replace(ReplaceOptions{
		Key:            []byte("testCasMismatch"),
		Value:          []byte("{\"key\":\"value\"}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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
	s.PushOp(agent.Replace(ReplaceOptions{
		Key:            []byte("testCasMismatch"),
		Value:          []byte("{\"key\":\"value2\"}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Cas:            cas,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err == nil {
				s.Fatalf("Set operation succeeded but should have failed")
			}

			if !errors.Is(err, ErrCasMismatch) {
				suite.T().Fatalf("Expected CasMismatch error but was %v", err)
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testCasMismatch")
			suite.AssertOpSpan(nilParents[1], "Replace", agent.BucketName(), memd.CmdReplace.Name(), 1, false, "testCasMismatch")
			suite.AssertOpSpan(nilParents[2], "Replace", agent.BucketName(), memd.CmdReplace.Name(), 1, false, "testCasMismatch")
		}
	}
}

func (suite *StandardTestSuite) TestGetReplica() {
	suite.EnsureSupportsFeature(TestFeatureReplicas)
	agent, s := suite.GetAgentAndHarness()

	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testReplica"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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
		s.PushOp(agent.GetOneReplica(GetOneReplicaOptions{
			Key:            []byte("testReplica"),
			ReplicaIdx:     1,
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
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
			suite.T().Fatalf("GetReplica could not locate key")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 2) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testReplica")
			suite.AssertOpSpan(nilParents[1], "GetOneReplica", agent.BucketName(), memd.CmdGetReplica.Name(), 1, true, "testReplica")
		}
	}
}

func (suite *StandardTestSuite) TestDurableWriteGetReplica() {
	suite.EnsureSupportsFeature(TestFeatureReplicas)
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)
	agent, s := suite.GetAgentAndHarness()

	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:                    []byte("testDurableReplica"),
		Value:                  []byte("{}"),
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
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
		s.PushOp(agent.GetOneReplica(GetOneReplicaOptions{
			Key:            []byte("testDurableReplica"),
			ReplicaIdx:     1,
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
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
			suite.T().Fatalf("GetReplica could not locate key")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 2) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testDurableReplica")
			suite.AssertOpSpan(nilParents[1], "GetOneReplica", agent.BucketName(), memd.CmdGetReplica.Name(), 1, true, "testDurableReplica")
		}
	}
}

func (suite *StandardTestSuite) TestAddDurableWriteGetReplica() {
	suite.EnsureSupportsFeature(TestFeatureReplicas)
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Add(AddOptions{
		Key:                    []byte("testAddDurableReplica"),
		Value:                  []byte("{}"),
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
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

	retries := 0
	keyExists := false
	for {
		s.PushOp(agent.GetOneReplica(GetOneReplicaOptions{
			Key:            []byte("testAddDurableReplica"),
			ReplicaIdx:     1,
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
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
			suite.T().Fatalf("GetReplica could not locate key")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 2) {
			suite.AssertOpSpan(nilParents[0], "Add", agent.BucketName(), memd.CmdAdd.Name(), 1, false, "testAddDurableReplica")
			suite.AssertOpSpan(nilParents[1], "GetOneReplica", agent.BucketName(), memd.CmdGetReplica.Name(), 1, true, "testAddDurableReplica")
		}
	}
}

func (suite *StandardTestSuite) TestReplaceDurableWriteGetReplica() {
	suite.EnsureSupportsFeature(TestFeatureReplicas)
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:                    []byte("testReplaceDurableReplica"),
		Value:                  []byte("{}"),
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
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

	s.PushOp(agent.Replace(ReplaceOptions{
		Key:                    []byte("testReplaceDurableReplica"),
		Value:                  []byte("{}"),
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
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

	retries := 0
	keyExists := false
	for {
		s.PushOp(agent.GetOneReplica(GetOneReplicaOptions{
			Key:            []byte("testReplaceDurableReplica"),
			ReplicaIdx:     1,
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
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
			suite.T().Fatalf("GetReplica could not locate key")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 3) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testReplaceDurableReplica")
			suite.AssertOpSpan(nilParents[1], "Replace", agent.BucketName(), memd.CmdReplace.Name(), 1, false, "testReplaceDurableReplica")
			suite.AssertOpSpan(nilParents[2], "GetOneReplica", agent.BucketName(), memd.CmdGetReplica.Name(), 1, true, "testReplaceDurableReplica")
		}
	}
}

func (suite *StandardTestSuite) TestDeleteDurableWriteGetReplica() {
	suite.EnsureSupportsFeature(TestFeatureReplicas)
	suite.EnsureSupportsFeature(TestFeatureEnhancedDurability)
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:                    []byte("testDeleteDurableReplica"),
		Value:                  []byte("{}"),
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
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

	s.PushOp(agent.Delete(DeleteOptions{
		Key:                    []byte("testDeleteDurableReplica"),
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
	}, func(res *DeleteResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Delete operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	retries := 0
	keyNotFound := false
	for {
		s.PushOp(agent.GetOneReplica(GetOneReplicaOptions{
			Key:            []byte("testDeleteDurableReplica"),
			ReplicaIdx:     1,
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
		}, func(res *GetReplicaResult, err error) {
			s.Wrap(func() {
				if errors.Is(err, ErrDocumentNotFound) {
					keyNotFound = true
				} else if err != nil {
					s.Fatalf("GetReplica specific returned error that was not document not found: %v", err)
				}
				if !keyNotFound && res.Cas == Cas(0) {
					s.Fatalf("Invalid cas received")
				}
			})
		}))
		s.Wait(0)
		if keyNotFound {
			break
		}
		retries++
		if retries >= 5 {
			suite.T().Fatalf("GetReplica could always locate key")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 3) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testDeleteDurableReplica")
			suite.AssertOpSpan(nilParents[1], "Delete", agent.BucketName(), memd.CmdDelete.Name(), 1, false, "testDeleteDurableReplica")
			suite.AssertOpSpan(nilParents[2], "GetOneReplica", agent.BucketName(), memd.CmdGetReplica.Name(), 1, true, "testDeleteDurableReplica")
		}
	}
}

func (suite *StandardTestSuite) TestBasicReplace() {
	agent, s := suite.GetAgentAndHarness()

	oldCas := Cas(0)
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testx"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		oldCas = res.Cas
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Replace(ReplaceOptions{
		Key:            []byte("testx"),
		Value:          []byte("[]"),
		Cas:            oldCas,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testx")
			suite.AssertOpSpan(nilParents[1], "Replace", agent.BucketName(), memd.CmdReplace.Name(), 1, false, "testx")
		}
	}
}

func (suite *StandardTestSuite) TestBasicRemove() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testy"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Delete(DeleteOptions{
		Key:            []byte("testy"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *DeleteResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Remove operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testy")
			suite.AssertOpSpan(nilParents[1], "Delete", agent.BucketName(), memd.CmdDelete.Name(), 1, false, "testy")
		}
	}
}

func (suite *StandardTestSuite) TestBasicInsert() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Delete(DeleteOptions{
		Key:            []byte("testz"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *DeleteResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Add(AddOptions{
		Key:            []byte("testz"),
		Value:          []byte("[]"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Delete", agent.BucketName(), memd.CmdDelete.Name(), 1, false, "testz")
			suite.AssertOpSpan(nilParents[1], "Add", agent.BucketName(), memd.CmdAdd.Name(), 1, false, "testz")
		}
	}
}

func (suite *StandardTestSuite) TestBasicCounters() {
	agent, s := suite.GetAgentAndHarness()

	// Counters
	s.PushOp(agent.Delete(DeleteOptions{
		Key:            []byte("testCounters"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *DeleteResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Increment(CounterOptions{
		Key:            []byte("testCounters"),
		Delta:          5,
		Initial:        11,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	s.PushOp(agent.Increment(CounterOptions{
		Key:            []byte("testCounters"),
		Delta:          5,
		Initial:        22,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	s.PushOp(agent.Decrement(CounterOptions{
		Key:            []byte("testCounters"),
		Delta:          3,
		Initial:        65,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(4, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Delete", agent.BucketName(), memd.CmdDelete.Name(), 1, false, "testCounters")
			suite.AssertOpSpan(nilParents[1], "Increment", agent.BucketName(), memd.CmdIncrement.Name(), 1, false, "testCounters")
			suite.AssertOpSpan(nilParents[2], "Increment", agent.BucketName(), memd.CmdIncrement.Name(), 1, false, "testCounters")
			suite.AssertOpSpan(nilParents[3], "Decrement", agent.BucketName(), memd.CmdDecrement.Name(), 1, false, "testCounters")
		}
	}
}

func (suite *StandardTestSuite) TestBasicAdjoins() {
	suite.EnsureSupportsFeature(TestFeatureAdjoin)

	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte("there"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Append(AdjoinOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte(" Frank!"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	s.PushOp(agent.Prepend(AdjoinOptions{
		Key:            []byte("testAdjoins"),
		Value:          []byte("Hello "),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("testAdjoins"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(4, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testAdjoins")
			suite.AssertOpSpan(nilParents[1], "Append", agent.BucketName(), memd.CmdAppend.Name(), 1, false, "testAdjoins")
			suite.AssertOpSpan(nilParents[2], "Prepend", agent.BucketName(), memd.CmdPrepend.Name(), 1, false, "testAdjoins")
			suite.AssertOpSpan(nilParents[3], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "testAdjoins")
		}
	}
}

func (suite *StandardTestSuite) TestExpiry() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testExpiry"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	suite.TimeTravel(2000 * time.Millisecond)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("testExpiry"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		RetryStrategy:  NewBestEffortRetryStrategy(nil),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentNotFound) {
				s.Fatalf("Get should have returned document not found")
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testExpiry")
			suite.AssertOpSpan(nilParents[1], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "testExpiry")
		}
	}
}

func (suite *StandardTestSuite) TestTouch() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testTouch"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Touch(TouchOptions{
		Key:            []byte("testTouch"),
		Expiry:         3,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *TouchResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	suite.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("testTouch"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	}))
	s.Wait(0)

	suite.TimeTravel(2500 * time.Millisecond)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("testTouch"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentNotFound) {
				s.Fatalf("Get should have returned document not found")
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(4, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testTouch")
			suite.AssertOpSpan(nilParents[1], "Touch", agent.BucketName(), memd.CmdTouch.Name(), 1, false, "testTouch")
			suite.AssertOpSpan(nilParents[2], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "testTouch")
			suite.AssertOpSpan(nilParents[3], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "testTouch")
		}
	}
}

func (suite *StandardTestSuite) TestGetAndTouch() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testGetAndTouch"),
		Value:          []byte("{}"),
		Expiry:         1,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetAndTouch(GetAndTouchOptions{
		Key:            []byte("testGetAndTouch"),
		Expiry:         3,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetAndTouchResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	suite.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("testGetAndTouch"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	}))
	s.Wait(0)

	suite.TimeTravel(3000 * time.Millisecond)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("testGetAndTouch"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentNotFound) {
				s.Fatalf("Get should have returned document not found: %v", err)
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(4, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testGetAndTouch")
			suite.AssertOpSpan(nilParents[1], "GetAndTouch", agent.BucketName(), memd.CmdGAT.Name(), 1, false, "testGetAndTouch")
			suite.AssertOpSpan(nilParents[2], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "testGetAndTouch")
			suite.AssertOpSpan(nilParents[3], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "testGetAndTouch")
		}
	}
}

// This test will lock the document for 1 second, it will then perform set requests for up to 2 seconds,
// the operation should succeed within the 2 seconds.
func (suite *StandardTestSuite) TestRetrySet() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testRetrySet"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetAndLock(GetAndLockOptions{
		Key:            []byte("testRetrySet"),
		LockTime:       1,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetAndLockResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("GetAndLock operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testRetrySet"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		RetryStrategy:  NewBestEffortRetryStrategy(nil),
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testRetrySet")
			suite.AssertOpSpan(nilParents[1], "GetAndLock", agent.BucketName(), memd.CmdGetLocked.Name(), 1, false, "testRetrySet")
			suite.AssertOpSpan(nilParents[2], "Set", agent.BucketName(), memd.CmdGet.Name(), 1, true, "testRetrySet")
		}
	}
}

func (suite *StandardTestSuite) TestObserve() {
	suite.EnsureSupportsFeature(TestFeatureReplicas)

	agent, s := suite.GetAgentAndHarness()
	if agent.HasCollectionsSupport() {
		suite.T().Skip("Skipping test as observe does not support collections")
	}

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Observe(ObserveOptions{
		Key:            []byte("testObserve"),
		ReplicaIdx:     1,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *ObserveResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Observe operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testObserve")
			suite.AssertOpSpan(nilParents[1], "Observe", agent.BucketName(), memd.CmdObserve.Name(), 1, false, "")
		}
	}
}

func (suite *StandardTestSuite) TestObserveSeqNo() {
	suite.EnsureSupportsFeature(TestFeatureReplicas)

	agent, s := suite.GetAgentAndHarness()

	origMt := MutationToken{}
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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
	vbID, err := agent.kvMux.KeyToVbucket([]byte("testObserve"))
	if err != nil {
		s.Fatalf("KeyToVbucket operation failed: %v", err)
	}

	s.PushOp(agent.ObserveVb(ObserveVbOptions{
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
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testObserve"),
		Value:          []byte("there"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Second set operation failed: %v", err)
			}

			newMt = res.MutationToken
		})
	}))
	s.Wait(0)

	vbID, err = agent.kvMux.KeyToVbucket([]byte("testObserve"))
	if err != nil {
		s.Fatalf("KeyToVbucket operation failed: %v", err)
	}
	s.PushOp(agent.ObserveVb(ObserveVbOptions{
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(4, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testObserve")
			suite.AssertOpSpan(nilParents[1], "ObserveVb", agent.BucketName(), memd.CmdObserveSeqNo.Name(), 1, false, "")
			suite.AssertOpSpan(nilParents[2], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testObserve")
			suite.AssertOpSpan(nilParents[3], "ObserveVb", agent.BucketName(), memd.CmdObserveSeqNo.Name(), 1, false, "")
		}
	}
}

func (suite *StandardTestSuite) TestRandomGet() {
	agent, s := suite.GetAgentAndHarness()

	distkeys, err := MakeDistKeys(agent, time.Now().Add(2*time.Second))
	suite.Require().Nil(err, err)
	for _, k := range distkeys {
		s.PushOp(agent.Set(SetOptions{
			Key:            []byte(k),
			Value:          []byte("Hello World!"),
			CollectionName: suite.CollectionName,
			ScopeName:      suite.ScopeName,
		}, func(res *StoreResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Couldn't store some items: %v", err)
				}
			})
		}))
		s.Wait(0)
	}

	s.PushOp(agent.GetRandom(GetRandomOptions{
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetRandomResult, err error) {
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(len(distkeys)+1, len(nilParents)) {
			for i, k := range distkeys {
				suite.AssertOpSpan(nilParents[i], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, k)
			}
			suite.AssertOpSpan(nilParents[len(distkeys)], "GetRandom", agent.BucketName(), memd.CmdGetRandom.Name(), 1, false, "")
		}
	}
}

func (suite *StandardTestSuite) TestStats() {
	agent, s := suite.GetAgentAndHarness()

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		suite.T().Fatalf("Failed to get config snapshot: %s", err)
	}
	numServers, err := snapshot.NumServers()
	if err != nil {
		suite.T().Fatalf("Failed to get num servers: %s", err)
	}

	s.PushOp(agent.Stats(StatsOptions{
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(1, len(nilParents)) {
			p := agent.kvMux.NumPipelines()
			suite.AssertTopLevelSpan(nilParents[0], "Stats", agent.BucketName())
			spans := nilParents[0].Spans[memd.CmdStat.Name()]
			if suite.Assert().Equal(p, len(spans)) {
				for i := 0; i < len(spans); i++ {
					span := spans[i]
					suite.Assert().Equal(memd.CmdStat.Name(), span.Name)
					suite.Assert().Equal(1, len(span.Tags))
					suite.Assert().True(span.Finished)
					suite.Assert().Equal(uint32(0), span.Tags["retry"])

					netSpans := span.Spans["rpc"]
					if suite.Assert().Equal(1, len(netSpans)) {
						suite.Assert().Equal("rpc", netSpans[0].Name)
						suite.Assert().Equal(1, len(netSpans[0].Tags))
						suite.Assert().True(netSpans[0].Finished)
						suite.Assert().Equal("client", netSpans[0].Tags["span.kind"])
					}
				}
			}
		}
	}
}

func (suite *StandardTestSuite) TestGetHttpEps() {
	agent, _ := suite.GetAgentAndHarness()

	// Relies on a 3.0.0+ server
	n1qlEpList := agent.N1qlEps()
	if len(n1qlEpList) == 0 {
		suite.T().Fatalf("Failed to retrieve N1QL endpoint list")
	}

	mgmtEpList := agent.MgmtEps()
	if len(mgmtEpList) == 0 {
		suite.T().Fatalf("Failed to retrieve N1QL endpoint list")
	}

	capiEpList := agent.CapiEps()
	if len(capiEpList) == 0 {
		suite.T().Fatalf("Failed to retrieve N1QL endpoint list")
	}
}

func (suite *StandardTestSuite) TestMemcachedBucket() {
	suite.EnsureSupportsFeature(TestFeatureMemd)

	s := suite.GetHarness()
	agent := suite.MemdAgent()

	s.PushOp(agent.Set(SetOptions{
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

	s.PushOp(agent.Get(GetOptions{
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
	_, err := agent.Observe(ObserveOptions{
		Key: []byte("key"),
	}, func(res *ObserveResult, err error) {
		s.Wrap(func() {
			s.Fatalf("Scheduling should fail on memcached buckets!")
		})
	})

	if !errors.Is(err, ErrFeatureNotAvailable) {
		suite.T().Fatalf("Expected observe error for memcached bucket!")
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "key")
			suite.AssertOpSpan(nilParents[1], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "key")
			suite.AssertOpSpan(nilParents[2], "Observe", agent.BucketName(), memd.CmdObserve.Name(), 0, false, "")
		}
	}
}

func (suite *StandardTestSuite) TestFlagsRoundTrip() {
	// Ensure flags are round-tripped with the server correctly.
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("flagskey"),
		Value:          []byte("{}"),
		Flags:          0x99889988,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("flagskey"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

func (suite *StandardTestSuite) TestMetaOps() {
	suite.EnsureSupportsFeature(TestFeatureGetMeta)

	agent, s := suite.GetAgentAndHarness()

	var currentCas Cas

	// Set

	s.PushOp(agent.Set(SetOptions{
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
	s.PushOp(agent.GetMeta(GetMetaOptions{
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

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "test")
			suite.AssertOpSpan(nilParents[1], "GetMeta", agent.BucketName(), memd.CmdGetMeta.Name(), 1, false, "test")
		}
	}
}

func (suite *StandardTestSuite) TestPing() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Ping(PingOptions{}, func(res *PingResult, err error) {
		s.Wrap(func() {
			if len(res.Services) == 0 {
				s.Fatalf("Ping report contained no results")
			}
		})
	}))
	s.Wait(5)
}

func (suite *StandardTestSuite) TestDiagnostics() {
	agent, _ := suite.GetAgentAndHarness()

	report, err := agent.Diagnostics(DiagnosticsOptions{})
	if err != nil {
		suite.T().Fatalf("Failed to fetch diagnostics: %s", err)
	}

	if len(report.MemdConns) == 0 {
		suite.T().Fatalf("Diagnostics report contained no results")
	}

	for _, conn := range report.MemdConns {
		if conn.RemoteAddr == "" {
			suite.T().Fatalf("Diagnostic report contained invalid entry")
		}
	}
}

type testAlternateAddressesRouteConfigMgr struct {
	cfg       *routeConfig
	cfgCalled bool
}

func (taa *testAlternateAddressesRouteConfigMgr) OnNewRouteConfig(cfg *routeConfig) {
	taa.cfgCalled = true
	taa.cfg = cfg
}

func (suite *StandardTestSuite) TestAlternateAddressesEmptyStringConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		SrcMemdAddrs: []string{"192.168.132.234:32799"},
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesAutoConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "auto",
		SrcMemdAddrs: []string{"192.168.132.234:32799"},
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesAutoInternalConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "auto",
		SrcMemdAddrs: []string{"172.17.0.4:11210"},
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.Hostname, port)
		if server != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesDefaultConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "default",
		SrcMemdAddrs: []string{"192.168.132.234:32799"},
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		suite.T().Fatalf("Expected agent networkType to be default, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.Hostname, port)
		if server != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesExternalConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "external",
		SrcMemdAddrs: []string{"192.168.132.234:32799"},
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesExternalConfigNoPorts() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses_without_ports.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "external",
		SrcMemdAddrs: []string{"192.168.132.234:32799"},
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesInvalidConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "invalid",
		SrcMemdAddrs: []string{"192.168.132.234:32799"},
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "invalid" {
		suite.T().Fatalf("Expected agent networkType to be invalid, was %s", networkType)
	}

	if mgr.cfgCalled {
		suite.T().Fatalf("Expected route config to not be propagated, was propagated")
	}
}

func (suite *StandardTestSuite) TestAgentWaitUntilReadyGCCCP() {
	suite.EnsureSupportsFeature(TestFeatureGCCCP)

	cfg := suite.makeAgentConfig(globalTestConfig)
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitUntilReady failed with error: %v", err)
			}
		})
	}))
	s.Wait(6)

	s.PushOp(agent.Ping(PingOptions{
		ServiceTypes: []ServiceType{N1qlService},
		N1QLDeadline: time.Now().Add(5 * time.Second),
	}, func(result *PingResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Ping failed with error: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) VerifyConnectedToBucket(agent *Agent, s *TestSubHarness, test string) {
	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitUntilReady failed with error: %v", err)
			}
		})
	}))
	s.Wait(6)

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte(test),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) TestAgentWaitUntilReadyBucket() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestAgentWaitUntilReadyBucket")
}

func (suite *StandardTestSuite) TestAgentGroupWaitUntilReadyGCCCP() {
	suite.EnsureSupportsFeature(TestFeatureGCCCP)

	cfg := suite.makeAgentGroupConfig(globalTestConfig)
	ag, err := CreateAgentGroup(&cfg)
	suite.Require().Nil(err, err)
	defer ag.Close()
	s := suite.GetHarness()

	s.PushOp(ag.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitUntilReady failed with error: %v", err)
			}
		})
	}))
	s.Wait(6)

	s.PushOp(ag.Ping(PingOptions{
		ServiceTypes: []ServiceType{N1qlService},
		N1QLDeadline: time.Now().Add(5 * time.Second),
	}, func(result *PingResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Ping failed with error: %v", err)
			}
		})
	}))
	s.Wait(0)
}

// This test cannot run against mock as the mock does not respond with 200 status code for all of the endpoints.
func (suite *StandardTestSuite) TestAgentGroupWaitUntilReadyBucket() {
	suite.EnsureSupportsFeature(TestFeaturePingServices)

	cfg := suite.makeAgentGroupConfig(globalTestConfig)
	ag, err := CreateAgentGroup(&cfg)
	suite.Require().Nil(err, err)
	defer ag.Close()
	s := suite.GetHarness()

	err = ag.OpenBucket(globalTestConfig.BucketName)
	suite.Require().Nil(err, err)

	agent := ag.GetAgent("default")
	suite.Require().NotNil(agent)

	suite.VerifyConnectedToBucket(agent, s, "TestAgentGroupWaitUntilReadyBucket")
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyDefaultPort() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "8091" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.HTTPAddrs = []string{addr1}
	cfg.MemdAddrs = []string{}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectHTTPOnlyDefaultPort")
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyDefaultPortSSL() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.HTTPAddrs[0]
	parts := strings.Split(addr1, ":")
	if parts[1] != "8091" {
		suite.T().Skipf("Skipping test due to non default port %s", parts[1])
	}

	cfg.HTTPAddrs = []string{parts[0] + ":" + "18091"}
	cfg.MemdAddrs = []string{}
	cfg.UseTLS = true
	// SkipVerify
	cfg.TLSRootCAProvider = func() *x509.CertPool {
		return nil
	}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectHTTPOnlyDefaultPortSSL")
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyDefaultPortFastFailInvalidBucket() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "8091" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.HTTPAddrs = []string{addr1}
	cfg.MemdAddrs = []string{}
	cfg.BucketName = "idontexist"
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	start := time.Now()
	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err == nil {
				s.Fatalf("WaitUntilReady failed without error")
			}
			if !errors.Is(err, ErrAuthenticationFailure) {
				s.Fatalf("WaitUntilReady should have failed with auth error but was %v", err)
			}
			if time.Since(start) > 5*time.Second {
				s.Fatalf("WaitUntilReady should have failed before the timeout duration, was %s", time.Since(start))
			}
		})
	}))
	s.Wait(6)
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyNonDefaultPort() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port == "8091" {
		suite.T().Skipf("Skipping test due to default port %s", port)
	}

	cfg.HTTPAddrs = []string{addr1}
	cfg.MemdAddrs = []string{}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectHTTPOnlyNonDefaultPort")
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyNonDefaultPortFastFailInvalidBucket() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port == "8091" {
		suite.T().Skipf("Skipping test due to default port %s", port)
	}

	cfg.HTTPAddrs = []string{addr1}
	cfg.MemdAddrs = []string{}
	cfg.BucketName = "idontexist"
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	start := time.Now()
	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err == nil {
				s.Fatalf("WaitUntilReady failed without error")
			}
			if !errors.Is(err, ErrAuthenticationFailure) {
				s.Fatalf("WaitUntilReady should have failed with auth error but was %v", err)
			}
			if time.Since(start) > 5*time.Second {
				s.Fatalf("WaitUntilReady should have failed before the timeout duration, was %s", time.Since(start))
			}
		})
	}))
	s.Wait(6)
}

func (suite *StandardTestSuite) TestConnectMemdOnlyDefaultPort() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no Memd addresses")
	}

	addr1 := cfg.MemdAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "11210" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.HTTPAddrs = []string{}
	cfg.MemdAddrs = []string{addr1}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectMemdOnlyDefaultPort")
}

func (suite *StandardTestSuite) TestConnectMemdOnlyDefaultPortSSL() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no memd addresses")
	}

	addr1 := cfg.MemdAddrs[0]
	parts := strings.Split(addr1, ":")
	if parts[1] != "11210" {
		suite.T().Skipf("Skipping test due to non default port %s", parts[1])
	}

	cfg.HTTPAddrs = []string{}
	cfg.MemdAddrs = []string{parts[0] + ":11207"}
	cfg.UseTLS = true
	// SkipVerify
	cfg.TLSRootCAProvider = func() *x509.CertPool {
		return nil
	}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectMemdOnlyDefaultPortSSL")
}

func (suite *StandardTestSuite) TestConnectMemdOnlyNonDefaultPort() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no memd addresses")
	}

	addr1 := cfg.MemdAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port == "8091" {
		suite.T().Skipf("Skipping test due to default port %s", port)
	}

	cfg.HTTPAddrs = []string{}
	cfg.MemdAddrs = []string{addr1}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectMemdOnlyNonDefaultPort")
}

func (suite *StandardTestSuite) TestConnectMemdOnlyDefaultPortFastFailInvalidBucket() {
	cfg := suite.makeAgentConfig(globalTestConfig)
	if len(cfg.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no memd addresses")
	}

	addr1 := cfg.MemdAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "11210" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.HTTPAddrs = []string{}
	cfg.MemdAddrs = []string{addr1}
	cfg.BucketName = "idontexist"
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	start := time.Now()
	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err == nil {
				s.Fatalf("WaitUntilReady failed without error")
			}
			if !errors.Is(err, ErrAuthenticationFailure) {
				s.Fatalf("WaitUntilReady should have failed with auth error but was %v", err)
			}
			if time.Since(start) > 5*time.Second {
				s.Fatalf("WaitUntilReady should have failed before the timeout duration, was %s", time.Since(start))
			}
		})
	}))
	s.Wait(6)
}

// These functions are likely temporary.

type testManifestWithError struct {
	Manifest Manifest
	Err      error
}

func testCreateScope(name, bucketName string, agent *Agent) (*Manifest, error) {
	data := url.Values{}
	data.Set("name", name)

	req := &HTTPRequest{
		Service:  MgmtService,
		Path:     fmt.Sprintf("/pools/default/buckets/%s/collections", bucketName),
		Method:   "POST",
		Body:     []byte(data.Encode()),
		Headers:  make(map[string]string),
		Deadline: time.Now().Add(10 * time.Second),
	}

	req.Headers["Content-Type"] = "application/x-www-form-urlencoded"

	resCh := make(chan *HTTPResponse)
	errCh := make(chan error)
	_, err := agent.DoHTTPRequest(req, func(response *HTTPResponse, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- response
	})
	if err != nil {
		return nil, err
	}

	var resp *HTTPResponse
	select {
	case respErr := <-errCh:
		if respErr != nil {
			return nil, respErr
		}
	case res := <-resCh:
		resp = res
	}

	if resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("could not create scope, status code: %d", resp.StatusCode)
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close response body")
		}
		return nil, fmt.Errorf("could not create scope, %s", string(data))
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
	waitCh := make(chan testManifestWithError, 1)
	go waitForManifest(agent, uint64(uid), waitCh)

	for {
		select {
		case <-timer.C:
			return nil, errors.New("wait time for scope to become available expired")
		case manifest := <-waitCh:
			if manifest.Err != nil {
				return nil, manifest.Err
			}

			return &manifest.Manifest, nil
		}
	}
}

func testDeleteScope(name, bucketName string, agent *Agent, waitForDeletion bool) (*Manifest, error) {
	data := url.Values{}
	data.Set("name", name)

	req := &HTTPRequest{
		Service:  MgmtService,
		Path:     fmt.Sprintf("/pools/default/buckets/%s/collections/%s", bucketName, name),
		Method:   "DELETE",
		Headers:  make(map[string]string),
		Deadline: time.Now().Add(10 * time.Second),
	}

	resCh := make(chan *HTTPResponse)
	errCh := make(chan error)
	_, err := agent.DoHTTPRequest(req, func(response *HTTPResponse, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- response
	})
	if err != nil {
		return nil, err
	}

	var resp *HTTPResponse
	select {
	case respErr := <-errCh:
		if respErr != nil {
			return nil, respErr
		}
	case res := <-resCh:
		resp = res
	}

	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("could not delete scope, status code: %d", resp.StatusCode)
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close response body")
		}
		return nil, fmt.Errorf("could not delete scope, %s", string(data))
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
	waitCh := make(chan testManifestWithError, 1)
	go waitForManifest(agent, uint64(uid), waitCh)

	for {
		select {
		case <-timer.C:
			return nil, errors.New("wait time for scope to become deleted expired")
		case manifest := <-waitCh:
			if manifest.Err != nil {
				return nil, manifest.Err
			}

			return &manifest.Manifest, nil
		}
	}

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
		Service:  MgmtService,
		Path:     fmt.Sprintf("/pools/default/buckets/%s/collections/%s/", bucketName, scopeName),
		Method:   "POST",
		Body:     []byte(data.Encode()),
		Headers:  make(map[string]string),
		Deadline: time.Now().Add(10 * time.Second),
	}

	req.Headers["Content-Type"] = "application/x-www-form-urlencoded"

	resCh := make(chan *HTTPResponse)
	errCh := make(chan error)
	_, err := agent.DoHTTPRequest(req, func(response *HTTPResponse, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- response
	})
	if err != nil {
		return nil, err
	}

	var resp *HTTPResponse
	select {
	case respErr := <-errCh:
		if respErr != nil {
			return nil, respErr
		}
	case res := <-resCh:
		resp = res
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
	waitCh := make(chan testManifestWithError, 1)
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
		Service:  MgmtService,
		Path:     fmt.Sprintf("/pools/default/buckets/%s/collections/%s/%s", bucketName, scopeName, name),
		Method:   "DELETE",
		Headers:  make(map[string]string),
		Deadline: time.Now().Add(10 * time.Second),
	}

	resCh := make(chan *HTTPResponse)
	errCh := make(chan error)
	_, err := agent.DoHTTPRequest(req, func(response *HTTPResponse, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- response
	})
	if err != nil {
		return nil, err
	}

	var resp *HTTPResponse
	select {
	case respErr := <-errCh:
		if respErr != nil {
			return nil, respErr
		}
	case res := <-resCh:
		resp = res
	}

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
	waitCh := make(chan testManifestWithError, 1)
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
		agent.GetCollectionManifest(GetCollectionManifestOptions{}, func(result *GetCollectionManifestResult, err error) {
			if err != nil {
				log.Println(err.Error())
				close(setCh)
				manifestCh <- testManifestWithError{Err: err}
				return
			}

			err = json.Unmarshal(result.Manifest, &manifest)
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
