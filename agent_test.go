package gocbcore

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *StandardTestSuite) TestPreserveExpirySet() {
	suite.EnsureSupportsFeature(TestFeaturePreserveExpiry)

	agent, s := suite.GetAgentAndHarness()

	expiry := uint32(25)
	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testsetpreserveExpiry"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Expiry:         expiry,
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

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testsetpreserveExpiry"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		PreserveExpiry: true,
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
	s.PushOp(agent.GetMeta(GetMetaOptions{
		Key:            []byte("testsetpreserveExpiry"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetMetaResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("GetMeta operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			expectedExpiry := uint32(time.Now().Unix() + int64(expiry-5))
			if res.Expiry < expectedExpiry {
				s.Fatalf("Invalid expiry received")
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testsetpreserveExpiry")
			suite.AssertOpSpan(nilParents[1], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testsetpreserveExpiry")
			suite.AssertOpSpan(nilParents[2], "GetMeta", agent.BucketName(), memd.CmdGetMeta.Name(), 1, false, "testsetpreserveExpiry")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "Set", 2, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetMeta", 1, false, false)
}

func (suite *StandardTestSuite) TestPreserveExpiryReplace() {
	suite.EnsureSupportsFeature(TestFeaturePreserveExpiry)

	agent, s := suite.GetAgentAndHarness()

	expiry := uint32(25)
	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testreplacepreserveExpiry"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Expiry:         expiry,
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
		Key:            []byte("testreplacepreserveExpiry"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		PreserveExpiry: true,
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

	// Get
	s.PushOp(agent.GetMeta(GetMetaOptions{
		Key:            []byte("testreplacepreserveExpiry"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetMetaResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("GetMeta operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			expectedExpiry := uint32(time.Now().Unix() + int64(expiry-5))
			if res.Expiry < expectedExpiry {
				s.Fatalf("Invalid expiry received, expected %d, was %d", expectedExpiry, res.Expiry)
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testreplacepreserveExpiry")
			suite.AssertOpSpan(nilParents[1], "Replace", agent.BucketName(), memd.CmdReplace.Name(), 1, false, "testreplacepreserveExpiry")
			suite.AssertOpSpan(nilParents[2], "GetMeta", agent.BucketName(), memd.CmdGetMeta.Name(), 1, false, "testreplacepreserveExpiry")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Replace", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetMeta", 1, false, false)
}

func (suite *StandardTestSuite) TestPreserveExpiryAppend() {
	suite.EnsureSupportsFeature(TestFeaturePreserveExpiry)

	agent, s := suite.GetAgentAndHarness()

	expiry := uint32(25)
	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testappendpreserveExpiry"),
		Value:          []byte("hello "),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Expiry:         expiry,
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

	s.PushOp(agent.Append(AdjoinOptions{
		Key:            []byte("testappendpreserveExpiry"),
		Value:          []byte("world"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		PreserveExpiry: true,
	}, func(res *AdjoinResult, err error) {
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

	// Get
	s.PushOp(agent.GetMeta(GetMetaOptions{
		Key:            []byte("testappendpreserveExpiry"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetMetaResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("GetMeta operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			expectedExpiry := uint32(time.Now().Unix() + int64(expiry-5))
			if res.Expiry < expectedExpiry {
				s.Fatalf("Invalid expiry received")
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testappendpreserveExpiry")
			suite.AssertOpSpan(nilParents[1], "Append", agent.BucketName(), memd.CmdAppend.Name(), 1, false, "testappendpreserveExpiry")
			suite.AssertOpSpan(nilParents[2], "GetMeta", agent.BucketName(), memd.CmdGetMeta.Name(), 1, false, "testappendpreserveExpiry")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Append", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetMeta", 1, false, false)
}

func (suite *StandardTestSuite) TestPreserveExpiryIncrement() {
	suite.EnsureSupportsFeature(TestFeaturePreserveExpiry)

	agent, s := suite.GetAgentAndHarness()

	expiry := uint32(25)

	s.PushOp(agent.Increment(CounterOptions{
		Key:            []byte("testincrementpreserveExpiry"),
		Initial:        5,
		Delta:          1,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		PreserveExpiry: true,
		Expiry:         expiry,
	}, func(res *CounterResult, err error) {
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

	s.PushOp(agent.Increment(CounterOptions{
		Key:            []byte("testincrementpreserveExpiry"),
		Delta:          1,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		PreserveExpiry: true,
	}, func(res *CounterResult, err error) {
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

	// Get
	s.PushOp(agent.GetMeta(GetMetaOptions{
		Key:            []byte("testincrementpreserveExpiry"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetMetaResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("GetMeta operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			expectedExpiry := uint32(time.Now().Unix() + int64(expiry-5))
			if res.Expiry < expectedExpiry {
				s.Fatalf("Invalid expiry received")
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Increment", agent.BucketName(), memd.CmdIncrement.Name(), 1, false, "testincrementpreserveExpiry")
			suite.AssertOpSpan(nilParents[2], "GetMeta", agent.BucketName(), memd.CmdGetMeta.Name(), 1, false, "testincrementpreserveExpiry")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "Increment", 2, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetMeta", 1, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Get", 1, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Replace", 2, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetOneReplica", 1, true, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetOneReplica", 1, true, false)
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

	suite.VerifyKVMetrics(suite.meter, "Add", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetOneReplica", 1, true, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Replace", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetOneReplica", 1, true, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Delete", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetOneReplica", 1, true, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Replace", 1, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Delete", 1, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Delete", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Add", 1, false, false)
}

func (suite *StandardTestSuite) TestBasicSetGet() {
	spec := suite.StartTest("kv/crud/SetGet")
	agent := spec.Agent
	s := suite.GetHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("test-doc"),
		Value:          []byte("{}"),
		CollectionName: spec.Collection,
		ScopeName:      spec.Scope,
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

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("test-doc"),
		CollectionName: spec.Collection,
		ScopeName:      spec.Scope,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if !bytes.Equal([]byte("{}"), res.Value) {
				s.Fatalf("Value did not match")
			}
		})
	}))
	s.Wait(0)

	suite.EndTest(spec)

	if suite.Assert().Contains(spec.Tracer.Spans, nil) {
		nilParents := spec.Tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "test-doc")
			suite.AssertOpSpan(nilParents[1], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "test-doc")
		}
	}
	suite.VerifyKVMetrics(spec.Meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(spec.Meter, "Get", 1, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Delete", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Increment", 2, false, false)
	suite.VerifyKVMetrics(suite.meter, "Decrement", 1, false, false)
}

func (suite *StandardTestSuite) TestBasicAdjoins() {
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Append", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Prepend", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Get", 1, false, false)
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

	suite.TimeTravel(3000 * time.Millisecond)

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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Get", 1, false, false)
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

	suite.TimeTravel(3500 * time.Millisecond)

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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Touch", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Get", 2, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetAndTouch", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Get", 2, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 2, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetAndLock", 1, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Observe", 1, false, false)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 2, false, false)
	suite.VerifyKVMetrics(suite.meter, "ObserveVb", 2, false, false)
}

func (suite *StandardTestSuite) TestRandomGet() {
	agent, s := suite.GetAgentAndHarness()

	var mustHaveValue bool
	var scope, collection string
	var durability memd.DurabilityLevel
	var duraTimeout time.Duration
	if suite.SupportsFeature(TestFeatureCollections) {
		mustHaveValue = true
		scope = uuid.NewString()[:6]
		collection = uuid.NewString()[:6]

		_, err := testCreateScope(scope, agent.BucketName(), agent)
		suite.Require().NoError(err)

		_, err = testCreateCollection(collection, scope, agent.BucketName(), agent)
		suite.Require().NoError(err)

		if agent.kvMux.NumReplicas() > 0 {
			// We use persist to force the doc to disk.
			durability = memd.DurabilityLevelPersistToMajority
			duraTimeout = 5 * time.Second
		}
	}

	suite.tracer.Reset()

	distkeys, err := MakeDistKeys(agent, time.Now().Add(2*time.Second))
	suite.Require().Nil(err, err)
	for _, k := range distkeys {
		s.PushOp(agent.Set(SetOptions{
			Key:                    []byte(k),
			Value:                  []byte("Hello World!"),
			CollectionName:         collection,
			ScopeName:              scope,
			DurabilityLevel:        durability,
			DurabilityLevelTimeout: duraTimeout,
		}, func(res *StoreResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Couldn't store some items: %v", err)
				}
			})
		}))
		s.Wait(0)
	}

	var attempts int
	var res *GetRandomResult
	suite.Require().Eventually(func() bool {
		attempts++
		s.PushOp(agent.GetRandom(GetRandomOptions{
			CollectionName: collection,
			ScopeName:      scope,
		}, func(res1 *GetRandomResult, err error) {
			s.Wrap(func() {
				if err != nil {
					suite.T().Logf("Get operation failed: %v", err)
					return
				}
				res = res1
			})
		}))
		s.Wait(0)

		return res != nil
	}, 10*time.Second, 500*time.Millisecond)

	if res.Cas == Cas(0) {
		s.Fatalf("Invalid cas received")
	}
	if len(res.Key) == 0 {
		s.Fatalf("Invalid key returned")
	}
	if mustHaveValue {
		if len(res.Value) == 0 {
			s.Fatalf("No value returned")
		}
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(len(distkeys)+attempts, len(nilParents)) {
			for i, k := range distkeys {
				suite.AssertOpSpan(nilParents[i], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, k)
			}
			suite.AssertOpSpan(nilParents[len(distkeys)], "GetRandom", agent.BucketName(), memd.CmdGetRandom.Name(), 1, false, "")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "Set", len(distkeys), false, false)
	suite.VerifyKVMetrics(suite.meter, "GetRandom", attempts, false, false)
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
					suite.Assert().True(span.Finished)

					netSpans := span.Spans[spanNameDispatchToServer]
					if suite.Assert().Equal(1, len(netSpans)) {
						suite.Assert().Equal(spanNameDispatchToServer, netSpans[0].Name)
						suite.Assert().True(netSpans[0].Finished)
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

	spec := suite.StartTest(TestNameMemcachedBasic)
	defer suite.EndTest(spec)
	s := suite.GetHarness()
	agent := spec.Agent

	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitUntilReady failed with error: %v", err)
			}
		})
	}))
	s.Wait(6)

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("test-doc"),
		Value:          []byte("value"),
		CollectionName: spec.Collection,
		ScopeName:      spec.Scope,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("test-doc"),
		CollectionName: spec.Collection,
		ScopeName:      spec.Scope,
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
		Key:            []byte("key"),
		CollectionName: spec.Collection,
		ScopeName:      spec.Scope,
	}, func(res *ObserveResult, err error) {
		s.Wrap(func() {
			s.Fatalf("Scheduling should fail on memcached buckets!")
		})
	})

	if !errors.Is(err, ErrFeatureNotAvailable) {
		suite.T().Fatalf("Expected observe error for memcached bucket!")
	}

	if suite.Assert().Contains(spec.Tracer.Spans, nil) {
		nilParents := spec.Tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "key")
			suite.AssertOpSpan(nilParents[1], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, "key")
			suite.AssertOpSpan(nilParents[2], "Observe", agent.BucketName(), memd.CmdObserve.Name(), 0, false, "")
		}
	}

	suite.VerifyKVMetrics(spec.Meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(spec.Meter, "Get", 1, false, false)
	suite.VerifyKVMetrics(spec.Meter, "Observe", 1, false, true)
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetMeta", 1, false, false)
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
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesAutoConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "auto",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesAutoInternalConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "auto",
		SrcMemdAddrs: []routeEndpoint{{Address: "172.17.0.4:11210"}},
		UseTLS:       false,
	})

	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		suite.T().Fatalf("Expected agent networkType to be default, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesDefaultConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "default",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "default" {
		suite.T().Fatalf("Expected agent networkType to be default, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesExternalConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "external",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesExternalConfigNoPorts() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses_without_ports.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "external",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
	})
	cfgManager.AddConfigWatcher(mgr)
	cfgManager.OnNewConfig(cfgBk)

	networkType := cfgManager.NetworkType()
	if networkType != "external" {
		suite.T().Fatalf("Expected agent networkType to be external, was %s", networkType)
	}

	for i, server := range mgr.cfg.kvServerList.NonSSLEndpoints {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("couchbase://%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server.Address != cfgBkServer {
			suite.T().Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server.Address)
		}
	}
}

func (suite *StandardTestSuite) TestAlternateAddressesInvalidConfig() {
	cfgBk := suite.LoadConfigFromFile("testdata/bucket_config_with_external_addresses.json")

	mgr := &testAlternateAddressesRouteConfigMgr{}
	cfgManager := newConfigManager(configManagerProperties{
		NetworkType:  "invalid",
		SrcMemdAddrs: []routeEndpoint{{Address: "192.168.132.234:32799"}},
		UseTLS:       false,
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

func (suite *StandardTestSuite) TestAgentWaitForConfigSnapshot() {
	cfg := makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	var snapshot *ConfigSnapshot
	s.PushOp(agent.WaitForConfigSnapshot(time.Now().Add(5*time.Second), WaitForConfigSnapshotOptions{}, func(result *WaitForConfigSnapshotResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitForConfigSnapshot failed with error: %v", err)
			}

			snapshot = result.Snapshot
		})
	}))
	s.Wait(6)

	suite.Assert().True(snapshot.RevID() > -1)
}

func (suite *StandardTestSuite) TestAgentWaitForConfigSnapshotSteadyState() {
	cfg := makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	var snapshot *ConfigSnapshot
	s.PushOp(agent.WaitForConfigSnapshot(time.Now().Add(5*time.Second), WaitForConfigSnapshotOptions{}, func(result *WaitForConfigSnapshotResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitForConfigSnapshot failed with error: %v", err)
			}

			snapshot = result.Snapshot
		})
	}))
	s.Wait(6)

	suite.Assert().True(snapshot.RevID() > -1)

	// Run it again, we know that the agent has already seen a config by now.
	s.PushOp(agent.WaitForConfigSnapshot(time.Now().Add(5*time.Second), WaitForConfigSnapshotOptions{}, func(result *WaitForConfigSnapshotResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitForConfigSnapshot failed with error: %v", err)
			}

			snapshot = result.Snapshot
		})
	}))
	s.Wait(6)

	suite.Assert().True(snapshot.RevID() > -1)
}

func (suite *StandardTestSuite) TestAgentWaitUntilReadyGCCCP() {
	suite.EnsureSupportsFeature(TestFeatureGCCCP)

	cfg := makeAgentConfig(globalTestConfig)
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

func (suite *StandardTestSuite) VerifyConnectedToBucket(agent *Agent, s *TestSubHarness, test, collection, scope string) {
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
		CollectionName: collection,
		ScopeName:      scope,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) VerifyConnectedToBucketHTTP(agent *Agent, bucket string, s *TestSubHarness, test string) {
	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("WaitUntilReady failed with error: %v", err)
			}
		})
	}))
	s.Wait(6)

	req := &HTTPRequest{
		Service:  MgmtService,
		Path:     fmt.Sprintf("/pools/default/buckets/%s", bucket),
		Method:   "GET",
		Headers:  make(map[string]string),
		Deadline: time.Now().Add(2 * time.Second),
	}

	s.PushOp(agent.DoHTTPRequest(req, func(res *HTTPResponse, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for HTTP request: %v", err)
			}

			res.Body.Close()
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) TestAgentWaitUntilReadyBucket() {
	cfg := makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestAgentWaitUntilReadyBucket", suite.CollectionName, suite.ScopeName)
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
	cfg := suite.makeAgentGroupConfig(globalTestConfig)
	ag, err := CreateAgentGroup(&cfg)
	suite.Require().Nil(err, err)
	defer ag.Close()
	s := suite.GetHarness()

	err = ag.OpenBucket(globalTestConfig.BucketName)
	suite.Require().Nil(err, err)

	agent := ag.GetAgent("default")
	suite.Require().NotNil(agent)

	suite.VerifyConnectedToBucket(agent, s, "TestAgentGroupWaitUntilReadyBucket", suite.CollectionName, suite.ScopeName)
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyDefaultPort() {
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.SeedConfig.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "8091" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{addr1}
	cfg.SeedConfig.MemdAddrs = []string{}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectHTTPOnlyDefaultPort", suite.CollectionName, suite.ScopeName)
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyDefaultPortSSL() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.SeedConfig.HTTPAddrs[0]
	parts := strings.Split(addr1, ":")
	if parts[1] != "8091" {
		suite.T().Skipf("Skipping test due to non default port %s", parts[1])
	}

	cfg.SeedConfig.HTTPAddrs = []string{parts[0] + ":" + "18091"}
	cfg.SeedConfig.MemdAddrs = []string{}
	cfg.SecurityConfig.UseTLS = true
	// SkipVerify
	cfg.SecurityConfig.TLSRootCAProvider = func() *x509.CertPool {
		return nil
	}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectHTTPOnlyDefaultPortSSL", suite.CollectionName, suite.ScopeName)
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyDefaultPortFastFailInvalidBucket() {
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	// This test purposefully triggers error cases.
	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	addr1 := cfg.SeedConfig.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "8091" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{addr1}
	cfg.SeedConfig.MemdAddrs = []string{}
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
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.SeedConfig.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port == "8091" {
		suite.T().Skipf("Skipping test due to default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{addr1}
	cfg.SeedConfig.MemdAddrs = []string{}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectHTTPOnlyNonDefaultPort", suite.CollectionName, suite.ScopeName)
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyNonDefaultPortNoBucket() {
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	addr1 := cfg.SeedConfig.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port == "8091" {
		suite.T().Skipf("Skipping test due to default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{addr1}
	cfg.SeedConfig.MemdAddrs = []string{}
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	s.PushOp(agent.WaitUntilReady(time.Now().Add(5*time.Second), WaitUntilReadyOptions{}, func(result *WaitUntilReadyResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrTimeout) {
				s.Fatalf("WaitUntilReady should have timed out but didn't with error: %v", err)
			}
		})
	}))
	s.Wait(6)
}

func (suite *StandardTestSuite) TestConnectHTTPOnlyNonDefaultPortFastFailInvalidBucket() {
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}

	// This test purposefully triggers error cases.
	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	addr1 := cfg.SeedConfig.HTTPAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port == "8091" {
		suite.T().Skipf("Skipping test due to default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{addr1}
	cfg.SeedConfig.MemdAddrs = []string{}
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
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no Memd addresses")
	}

	addr1 := cfg.SeedConfig.MemdAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "11210" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{}
	cfg.SeedConfig.MemdAddrs = []string{addr1}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectMemdOnlyDefaultPort", suite.CollectionName, suite.ScopeName)
}

func (suite *StandardTestSuite) TestConnectMemdOnlyDefaultPortSSL() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no memd addresses")
	}

	addr1 := cfg.SeedConfig.MemdAddrs[0]
	parts := strings.Split(addr1, ":")
	if parts[1] != "11210" {
		suite.T().Skipf("Skipping test due to non default port %s", parts[1])
	}

	cfg.SeedConfig.HTTPAddrs = []string{}
	cfg.SeedConfig.MemdAddrs = []string{parts[0] + ":11207"}
	cfg.SecurityConfig.UseTLS = true
	// SkipVerify
	cfg.SecurityConfig.TLSRootCAProvider = func() *x509.CertPool {
		return nil
	}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectMemdOnlyDefaultPortSSL", suite.CollectionName, suite.ScopeName)
}

func (suite *StandardTestSuite) TestConnectMemdOnlyNonDefaultPort() {
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no memd addresses")
	}

	addr1 := cfg.SeedConfig.MemdAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port == "8091" {
		suite.T().Skipf("Skipping test due to default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{}
	cfg.SeedConfig.MemdAddrs = []string{addr1}
	cfg.BucketName = globalTestConfig.BucketName
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestConnectMemdOnlyNonDefaultPort", suite.CollectionName, suite.ScopeName)
}

func (suite *StandardTestSuite) TestConnectMemdOnlyDefaultPortFastFailInvalidBucket() {
	cfg := makeAgentConfig(globalTestConfig)
	if len(cfg.SeedConfig.MemdAddrs) == 0 {
		suite.T().Skip("Skipping test due to no memd addresses")
	}

	// This test purposefully triggers error cases.
	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	addr1 := cfg.SeedConfig.MemdAddrs[0]
	port := strings.Split(addr1, ":")[1]
	if port != "11210" {
		suite.T().Skipf("Skipping test due to non default port %s", port)
	}

	cfg.SeedConfig.HTTPAddrs = []string{}
	cfg.SeedConfig.MemdAddrs = []string{addr1}
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

// This test tests that given an address any connections to it will be made not using SSL whilst other connections will
// be made using TLS.
func (suite *StandardTestSuite) TestAgentNSServerScheme() {
	suite.EnsureSupportsFeature(TestFeatureSsl)

	defaultAgent := suite.DefaultAgent()
	snapshot, err := defaultAgent.kvMux.PipelineSnapshot()
	suite.Require().Nil(err, err)

	if snapshot.NumPipelines() == 1 {
		suite.T().Skip("Skipping test due to cluster only containing one node")
	}

	srcCfg := makeAgentConfig(globalTestConfig)
	if len(srcCfg.SeedConfig.HTTPAddrs) == 0 {
		suite.T().Skip("Skipping test due to no HTTP addresses")
	}
	seedAddr := srcCfg.SeedConfig.HTTPAddrs[0]
	parts := strings.Split(seedAddr, ":")

	if !net.ParseIP(parts[0]).IsLoopback() {
		suite.T().Skip("Skipping test due to not being loopback address")
	}

	if parts[1] != "8091" && parts[1] != "11210" {
		// This should work with non default ports but it makes the test logic too complicated.
		// This implicitly means that if TLS is enabled then this test won't run.
		suite.T().Skip("Skipping test due to non default ports have been supplied")
	}

	connstr := fmt.Sprintf("ns_server://%s", seedAddr)
	config := AgentConfig{}
	err = config.FromConnStr(connstr)
	suite.Require().Nil(err, err)

	config.IoConfig = IoConfig{
		UseDurations:           true,
		UseMutationTokens:      true,
		UseCollections:         true,
		UseOutOfOrderResponses: true,
	}

	config.SecurityConfig.Auth = globalTestConfig.Authenticator
	config.SecurityConfig.UseTLS = true

	config.SecurityConfig.TLSRootCAProvider = func() *x509.CertPool {
		return nil
	}

	config.BucketName = globalTestConfig.BucketName

	agent, err := CreateAgent(&config)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestAgentNSServerScheme", suite.CollectionName, suite.ScopeName)

	kvMuxState := agent.kvMux.getState()
	kvEps := kvMuxState.kvServerList
	for _, ep := range kvEps {
		epParts := strings.Split(ep.Address, "://")
		hostport := strings.Split(epParts[1], ":")
		if parts[0] == hostport[0] {
			suite.Assert().Equal("couchbase", epParts[0])
			suite.Assert().Equal("11210", hostport[1])
		} else {
			suite.Assert().Equal("couchbases", epParts[0])
			suite.Assert().Equal("11207", hostport[1])
		}
	}

	httpMuxState := agent.httpMux.Get()
	mgmtEps := httpMuxState.mgmtEpList
	for _, ep := range mgmtEps {
		epParts := strings.Split(ep.Address, "://")
		hostport := strings.Split(epParts[1], ":")
		if hostport[0] == parts[0] {
			suite.Assert().Equal("http", epParts[0])
			suite.Assert().Equal(hostport[1], "8091")
		} else {
			suite.Assert().Equal("https", epParts[0])
			suite.Assert().NotEqual(hostport[1], "8091")
		}
	}
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
		Path:     fmt.Sprintf("/pools/default/buckets/%s/scopes", bucketName),
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
		Path:     fmt.Sprintf("/pools/default/buckets/%s/scopes/%s", bucketName, name),
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
		Path:     fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections", bucketName, scopeName),
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
		Path:     fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections/%s", bucketName, scopeName, name),
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

func dumpManifest(agent *Agent, t *testing.T) {
	waitCh := make(chan struct{}, 1)
	_, err := agent.GetCollectionManifest(GetCollectionManifestOptions{}, func(result *GetCollectionManifestResult, err error) {
		if err != nil {
			t.Logf("Failed to Get Collection Manifest: %v", err)
			return
		}

		var manifest Manifest
		err = json.Unmarshal(result.Manifest, &manifest)
		if err != nil {
			t.Logf("Failed to unmarshal manifest: %v", err)
			return
		}

		t.Logf("Manifest: %+v", manifest)

		waitCh <- struct{}{}
	})
	if err != nil {
		t.Logf("Failed to send GetCollectionManifest: %v", err)
		return
	}
	<-waitCh
}
