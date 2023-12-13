package gocbcore

import (
	"errors"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *StandardTestSuite) TestDocumentNotLocked() {
	suite.EnsureSupportsFeature(TestFeatureDocNotLocked)

	agent, s := suite.GetAgentAndHarness()

	docID := uuid.NewString()

	var cas Cas
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte(docID),
		Value:          []byte("test"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			cas = res.Cas
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Unlock(UnlockOptions{
		Key:            []byte(docID),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Cas:            cas,
	}, func(result *UnlockResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentNotLocked) {
				s.Fatalf("Unlock operation failed with unexpected error, should've been not locked: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) TestResourceUnits() {
	suite.EnsureSupportsFeature(TestFeatureResourceUnits)

	agent, s := suite.GetAgentAndHarness()

	docID := uuid.NewString()

	var resourceUnits *ResourceUnitResult
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte(docID),
		Value:          []byte("{\"x\":\"xattrs\"}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}

			resourceUnits = res.Internal.ResourceUnits
		})
	}))
	s.Wait(0)

	if suite.Assert().NotNil(resourceUnits) {
		suite.Require().GreaterOrEqual(1, int(resourceUnits.WriteUnits))
	}

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte(docID),
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

			resourceUnits = res.Internal.ResourceUnits
		})
	}))
	s.Wait(0)

	if suite.Assert().NotNil(resourceUnits) {
		suite.Require().GreaterOrEqual(1, int(resourceUnits.ReadUnits))
	}

	s.PushOp(agent.Touch(TouchOptions{
		Key:            []byte(docID),
		Expiry:         5,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *TouchResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}

			resourceUnits = res.Internal.ResourceUnits
		})
	}))
	s.Wait(0)

	if suite.Assert().NotNil(resourceUnits) {
		suite.Require().GreaterOrEqual(1, int(resourceUnits.ReadUnits))
		suite.Require().GreaterOrEqual(1, int(resourceUnits.WriteUnits))
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(3, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, docID)
			suite.AssertOpSpan(nilParents[1], "Get", agent.BucketName(), memd.CmdGet.Name(), 1, false, docID)
			suite.AssertOpSpan(nilParents[2], "Touch", agent.BucketName(), memd.CmdTouch.Name(), 1, false, docID)
		}
	}

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Get", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "Touch", 1, false, false)
}

// At time of writing compute units were not applied for a failed unlock.
// func (suite *StandardTestSuite) TestResourceUnitsLockedRetries() {
// 	suite.EnsureSupportsFeature(TestFeatureResourceUnits)
//
// 	agent, s := suite.GetAgentAndHarness()
//
// 	docID := uuid.NewString()
//
// 	var resourceUnits *ResourceUnitResult
// 	s.PushOp(agent.Set(SetOptions{
// 		Key:            []byte(docID),
// 		Value:          []byte("{\"x\":\"xattrs\"}"),
// 		CollectionName: suite.CollectionName,
// 		ScopeName:      suite.ScopeName,
// 	}, func(res *StoreResult, err error) {
// 		s.Wrap(func() {
// 			if err != nil {
// 				s.Fatalf("Set operation failed: %v", err)
// 			}
//
// 			resourceUnits = res.Internal.ResourceUnits
// 		})
// 	}))
// 	s.Wait(0)
//
// 	s.PushOp(agent.GetAndLock(GetAndLockOptions{
// 		Key:            []byte(docID),
// 		LockTime:       2,
// 		CollectionName: suite.CollectionName,
// 		ScopeName:      suite.ScopeName,
// 	}, func(res *GetAndLockResult, err error) {
// 		s.Wrap(func() {
// 			if err != nil {
// 				s.Fatalf("Get operation failed: %v", err)
// 			}
// 			if res.Cas == Cas(0) {
// 				s.Fatalf("Invalid cas received")
// 			}
//
// 			resourceUnits = res.Internal.ResourceUnits
// 		})
// 	}))
// 	s.Wait(0)
//
// 	if suite.Assert().NotNil(resourceUnits) {
// 		suite.Require().GreaterOrEqual(1, int(resourceUnits.ReadUnits))
// 		suite.Require().GreaterOrEqual(1, int(resourceUnits.WriteUnits))
// 	}
//
// 	s.PushOp(agent.GetAndLock(GetAndLockOptions{
// 		Key:            []byte(docID),
// 		CollectionName: suite.CollectionName,
// 		ScopeName:      suite.ScopeName,
// 		RetryStrategy:  NewBestEffortRetryStrategy(ControlledBackoff),
// 	}, func(res *GetAndLockResult, err error) {
// 		s.Wrap(func() {
// 			if err != nil {
// 				s.Fatalf("Get operation failed: %v", err)
// 			}
// 			if res.Cas == Cas(0) {
// 				s.Fatalf("Invalid cas received")
// 			}
//
// 			resourceUnits = res.Internal.ResourceUnits
// 		})
// 	}))
// 	s.Wait(5)
//
// 	if suite.Assert().NotNil(resourceUnits) {
// 		suite.Require().GreaterOrEqual(1, int(resourceUnits.ReadUnits))
// 		suite.Require().GreaterOrEqual(1, int(resourceUnits.WriteUnits))
// 	}
//
// 	if suite.Assert().Contains(suite.tracer.Spans, nil) {
// 		nilParents := suite.tracer.Spans[nil]
// 		if suite.Assert().Equal(3, len(nilParents)) {
// 			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, docID)
// 			suite.AssertOpSpan(nilParents[1], "GetAndLock", agent.BucketName(), memd.CmdGetLocked.Name(), 1, true, docID)
// 			suite.AssertOpSpan(nilParents[2], "GetAndLock", agent.BucketName(), memd.CmdGetLocked.Name(), 2, true, docID)
//
// 			if suite.Assert().NotNil(resourceUnits) {
// 				numReqs := len(nilParents[2].Spans[memd.CmdGetLocked.Name()])
// 				suite.Assert().Equal(numReqs, int(resourceUnits.ReadUnits))
// 				suite.Assert().Equal(numReqs, int(resourceUnits.WriteUnits))
// 			}
// 		}
// 	}
//
// 	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
// 	suite.VerifyKVMetrics(suite.meter, "GetAndLock", 3, true, false)
// }

// At time of writing compute units were not supported for get collection ID.
// func (suite *StandardTestSuite) TestResourceUnitsCollectionUnknown() {
// 	suite.EnsureSupportsFeature(TestFeatureResourceUnits)
//
// 	agent, s := suite.GetAgentAndHarness()
//
// 	colName := uuid.NewString()
// 	_, err := testCreateCollection(colName, globalTestConfig.ScopeName, globalTestConfig.BucketName, agent)
// 	suite.Require().Nil(err)
//
// 	// Who knows what's happened during creating the collection.
// 	suite.meter.Reset()
// 	suite.tracer.Reset()
//
// 	docID := uuid.NewString()
//
// 	var resourceUnits *ResourceUnitResult
// 	s.PushOp(agent.Set(SetOptions{
// 		Key:            []byte(docID),
// 		Value:          []byte("{\"x\":\"xattrs\"}"),
// 		CollectionName: colName,
// 		ScopeName:      suite.ScopeName,
// 	}, func(res *StoreResult, err error) {
// 		s.Wrap(func() {
// 			if err != nil {
// 				s.Fatalf("Set operation failed: %v", err)
// 			}
//
// 			resourceUnits = res.Internal.ResourceUnits
// 		})
// 	}))
// 	s.Wait(0)
//
// 	if suite.Assert().NotNil(resourceUnits) {
// 		suite.Require().GreaterOrEqual(1, int(resourceUnits.ReadUnits))
// 		suite.Require().GreaterOrEqual(1, int(resourceUnits.WriteUnits))
// 	}
// }
