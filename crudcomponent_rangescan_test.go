package gocbcore

import (
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/google/uuid"
)

type rangeScanMutation struct {
	cas           Cas
	mutationToken MutationToken
}

type rangeScanMutations struct {
	muts      map[string]rangeScanMutation
	vbuuid    VbUUID
	highSeqNo SeqNo
}

func (suite *StandardTestSuite) setupRangeScan(docIDs []string, value []byte, collection, scope string) *rangeScanMutations {
	agent, s := suite.GetAgentAndHarness()

	muts := &rangeScanMutations{
		muts: make(map[string]rangeScanMutation),
	}
	for i := 0; i < len(docIDs); i++ {
		s.PushOp(agent.Set(SetOptions{
			Key:            []byte(docIDs[i]),
			Value:          value,
			ScopeName:      scope,
			CollectionName: collection,
		}, func(res *StoreResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Set operation failed: %v", err)
				}

				muts.muts[docIDs[i]] = rangeScanMutation{
					cas:           res.Cas,
					mutationToken: res.MutationToken,
				}

				if res.MutationToken.SeqNo > muts.highSeqNo {
					muts.highSeqNo = res.MutationToken.SeqNo
					muts.vbuuid = res.MutationToken.VbUUID
				}
			})
		}))
		s.Wait(0)
	}

	suite.tracer.Reset()
	suite.meter.Reset()

	return muts
}

func (suite *StandardTestSuite) TestRangeScanRangeLargeValues() {
	suite.EnsureSupportsFeature(TestFeatureRangeScan)

	agent, _ := suite.GetAgentAndHarness()

	size := 8192 * 2
	value := make([]byte, size)
	for i := 0; i < size; i++ {
		value[i] = byte(i)
	}

	docIDs := []string{"largevalues-2960", "largevalues-3064", "largevalues-3686", "largevalues-3716", "largevalues-5354",
		"largevalues-5426", "largevalues-6175", "largevalues-6607", "largevalues-6797", "largevalues-7871"}
	muts := suite.setupRangeScan(docIDs, value, suite.CollectionName, suite.ScopeName)

	data := suite.doRangeScan(12,
		RangeScanCreateOptions{
			Range: &RangeScanCreateRangeScanConfig{
				Start: []byte("largevalues"),
				End:   []byte("largevalues\xFF"),
			},
			Snapshot: &RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			ScopeName:      suite.ScopeName,
			CollectionName: suite.CollectionName,
		},
		RangeScanContinueOptions{
			Deadline: time.Now().Add(10 * time.Second),
		},
	)

	itemsMap := make(map[string]RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id, mut := range muts.muts {
		item, ok := itemsMap[id]
		if suite.Assert().True(ok) {
			suite.Assert().Equal(mut.cas, item.Cas)
			suite.Assert().Equal(mut.mutationToken.SeqNo, item.SeqNo)
			suite.Assert().Equal(value, item.Value)
		}
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 2) {
			suite.AssertOpSpan(nilParents[0], "RangeScanCreate", agent.BucketName(), memd.CmdRangeScanCreate.Name(), 1, false, "")
			suite.AssertOpSpan(nilParents[1], "RangeScanContinue", agent.BucketName(), memd.CmdRangeScanContinue.Name(), 1, false, "")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "RangeScanCreate", 1, false, false)
}

func (suite *StandardTestSuite) TestRangeScanRangeSmallValues() {
	suite.EnsureSupportsFeature(TestFeatureRangeScan)

	agent, _ := suite.GetAgentAndHarness()

	value := []byte(`{"barry": "sheen"}`)

	docIDs := []string{"rangesmallvalues-1023", "rangesmallvalues-1751", "rangesmallvalues-2202",
		"rangesmallvalues-2392", "rangesmallvalues-2570", "rangesmallvalues-4132", "rangesmallvalues-4640",
		"rangesmallvalues-5836", "rangesmallvalues-7283", "rangesmallvalues-7313"}
	muts := suite.setupRangeScan(docIDs, value, suite.CollectionName, suite.ScopeName)

	data := suite.doRangeScan(12,
		RangeScanCreateOptions{
			Range: &RangeScanCreateRangeScanConfig{
				Start: []byte("rangesmallvalues"),
				End:   []byte("rangesmallvalues\xFF"),
			},
			Snapshot: &RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			ScopeName:      suite.ScopeName,
			CollectionName: suite.CollectionName,
		},
		RangeScanContinueOptions{
			Deadline: time.Now().Add(10 * time.Second),
		},
	)

	itemsMap := make(map[string]RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id, mut := range muts.muts {
		item, ok := itemsMap[id]
		if suite.Assert().True(ok) {
			suite.Assert().Equal(mut.cas, item.Cas)
			suite.Assert().Equal(mut.mutationToken.SeqNo, item.SeqNo)
			suite.Assert().Equal(value, item.Value)
		}
	}

	suite.verifyRangeScanTelemetry(agent)
}

func (suite *StandardTestSuite) TestRangeScanRangeCollectionRetry() {
	suite.EnsureSupportsFeature(TestFeatureRangeScan)

	agent, _ := suite.GetAgentAndHarness()

	collectionName := strings.Replace(uuid.NewString(), "-", "", -1)
	_, err := testCreateCollection(collectionName, suite.ScopeName, suite.BucketName, agent)
	suite.Require().Nil(err, err)
	defer testDeleteCollection(collectionName, suite.ScopeName, suite.BucketName, agent, false)

	value := "value"
	docIDs := []string{"rangecollectionretry-9695", "rangecollectionretry-24520", "rangecollectionretry-90825",
		"rangecollectionretry-119677", "rangecollectionretry-150939", "rangecollectionretry-170176",
		"rangecollectionretry-199557", "rangecollectionretry-225568", "rangecollectionretry-231302",
		"rangecollectionretry-245898"}
	muts := suite.setupRangeScan(docIDs, []byte(value), collectionName, suite.ScopeName)

	// We're going to force a refresh so we need to delete the collection from our cache.
	agent.collections.mapLock.Lock()
	delete(agent.collections.idMap, suite.ScopeName+"."+collectionName)
	agent.collections.mapLock.Unlock()

	data := suite.doRangeScan(12,
		RangeScanCreateOptions{
			Range: &RangeScanCreateRangeScanConfig{
				Start: []byte("rangecollectionretry"),
				End:   []byte("rangecollectionretry\xFF"),
			},
			KeysOnly:       true,
			ScopeName:      suite.ScopeName,
			CollectionName: collectionName,
			Snapshot: &RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
		},
		RangeScanContinueOptions{
			Deadline: time.Now().Add(10 * time.Second),
		},
	)

	itemsMap := make(map[string]RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id := range muts.muts {
		item, ok := itemsMap[id]
		if suite.Assert().True(ok) {
			suite.Assert().Zero(item.Cas)
			suite.Assert().Zero(item.SeqNo)
			suite.Assert().Empty(item.Value)
		}
	}

	suite.verifyRangeScanTelemetry(agent)
}

func (suite *StandardTestSuite) TestRangeScanRangeKeysOnly() {
	suite.EnsureSupportsFeature(TestFeatureRangeScan)

	agent, _ := suite.GetAgentAndHarness()

	value := "value"
	docIDs := []string{"rangekeysonly-1269", "rangekeysonly-2048", "rangekeysonly-4378", "rangekeysonly-7159",
		"rangekeysonly-8898", "rangekeysonly-8908", "rangekeysonly-19559", "rangekeysonly-20808",
		"rangekeysonly-20998", "rangekeysonly-25889"}
	muts := suite.setupRangeScan(docIDs, []byte(value), suite.CollectionName, suite.ScopeName)

	data := suite.doRangeScan(12,
		RangeScanCreateOptions{
			Range: &RangeScanCreateRangeScanConfig{
				Start: []byte("rangekeysonly"),
				End:   []byte("rangekeysonly\xFF"),
			},
			Snapshot: &RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			KeysOnly:       true,
			ScopeName:      suite.ScopeName,
			CollectionName: suite.CollectionName,
		},
		RangeScanContinueOptions{
			Deadline: time.Now().Add(10 * time.Second),
		},
	)

	itemsMap := make(map[string]RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id := range muts.muts {
		item, ok := itemsMap[id]
		if suite.Assert().True(ok) {
			suite.Assert().Zero(item.Cas)
			suite.Assert().Zero(item.SeqNo)
			suite.Assert().Empty(item.Value)
		}
	}

	suite.verifyRangeScanTelemetry(agent)
}

func (suite *StandardTestSuite) TestRangeScanSamplingKeysOnly() {
	suite.EnsureSupportsFeature(TestFeatureRangeScan)

	agent, _ := suite.GetAgentAndHarness()

	scopeName := "rangeScanSampleKeysOnly"
	collectionName := "rangeScan"
	_, err := testCreateScope(scopeName, suite.BucketName, agent)
	suite.Require().Nil(err, err)
	defer testDeleteScope(scopeName, suite.BucketName, agent, false)
	_, err = testCreateCollection(collectionName, scopeName, suite.BucketName, agent)
	suite.Require().Nil(err, err)
	defer testDeleteCollection(collectionName, scopeName, suite.BucketName, agent, false)

	value := "value"
	docIDs := []string{"samplescankeys-170", "samplescankeys-602", "samplescankeys-792", "samplescankeys-3978",
		"samplescankeys-6869", "samplescankeys-9038", "samplescankeys-10806", "samplescankeys-10996",
		"samplescankeys-11092", "samplescankeys-11102"}
	muts := suite.setupRangeScan(docIDs, []byte(value), collectionName, scopeName)

	data := suite.doRangeScan(12,
		RangeScanCreateOptions{
			Sampling: &RangeScanCreateRandomSamplingConfig{
				Samples: 10,
			},
			Snapshot: &RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			KeysOnly:       true,
			ScopeName:      scopeName,
			CollectionName: collectionName,
		},
		RangeScanContinueOptions{
			Deadline: time.Now().Add(10 * time.Second),
		},
	)

	itemsMap := make(map[string]RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id := range muts.muts {
		item, ok := itemsMap[id]
		if suite.Assert().True(ok) {
			suite.Assert().Zero(item.Cas)
			suite.Assert().Zero(item.SeqNo)
			suite.Assert().Empty(item.Value)
		}
	}

	suite.verifyRangeScanTelemetry(agent)
}

func (suite *StandardTestSuite) TestRangeScanRangeCancellation() {
	suite.EnsureSupportsFeature(TestFeatureRangeScan)

	agent, s := suite.GetAgentAndHarness()

	value := "value"
	docIDs := []string{"rangescancancel-2746", "rangescancancel-37795", "rangescancancel-63440", "rangescancancel-116036",
		"rangescancancel-136879", "rangescancancel-156589", "rangescancancel-196316", "rangescancancel-203197",
		"rangescancancel-243428", "rangescancancel-257242"}

	var highSeqNo SeqNo
	var vbUUID VbUUID
	for i := 0; i < len(docIDs); i++ {
		s.PushOp(agent.Set(SetOptions{
			Key:            []byte(docIDs[i]),
			Value:          []byte(value),
			ScopeName:      suite.ScopeName,
			CollectionName: suite.CollectionName,
		}, func(res *StoreResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Set operation failed: %v", err)
				}

				highSeqNo = res.MutationToken.SeqNo
				vbUUID = res.MutationToken.VbUUID
			})
		}))
		s.Wait(0)
	}

	suite.tracer.Reset()
	suite.meter.Reset()

	var createRes RangeScanCreateResult
	s.PushOp(agent.RangeScanCreate(12, RangeScanCreateOptions{
		Range: &RangeScanCreateRangeScanConfig{
			Start: []byte("rangescancancel"),
			End:   []byte("rangescancancel\xFF"),
		},
		Snapshot: &RangeScanCreateSnapshotRequirements{
			VbUUID: vbUUID,
			SeqNo:  highSeqNo,
		},
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(res RangeScanCreateResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("RangeScanCreate operation failed: %v", err)
			}

			createRes = res
		})
	}))
	s.Wait(0)

	s.PushOp(createRes.RangeScanCancel(RangeScanCancelOptions{}, func(result *RangeScanCancelResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("RangeScanCancel operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) TestRangeScanConnectionInvalid() {
	suite.EnsureSupportsFeature(TestFeatureRangeScan)

	agent, s := suite.GetAgentAndHarness()

	scopeName := "rangeScanConnectionInvalid"
	collectionName := "rangeScan"
	_, err := testCreateScope(scopeName, suite.BucketName, agent)
	suite.Require().Nil(err, err)
	defer testDeleteScope(scopeName, suite.BucketName, agent, false)
	_, err = testCreateCollection(collectionName, scopeName, suite.BucketName, agent)
	suite.Require().Nil(err, err)
	defer testDeleteCollection(collectionName, scopeName, suite.BucketName, agent, false)

	value := "value"
	docIDs := []string{"rangekeysonly-1269", "rangekeysonly-2048", "rangekeysonly-4378", "rangekeysonly-7159",
		"rangekeysonly-8898", "rangekeysonly-8908", "rangekeysonly-19559", "rangekeysonly-20808",
		"rangekeysonly-20998", "rangekeysonly-25889"}
	muts := suite.setupRangeScan(docIDs, []byte(value), collectionName, scopeName)

	var createRes *rangeScanCreateResult
	s.PushOp(agent.RangeScanCreate(12,
		RangeScanCreateOptions{
			Sampling: &RangeScanCreateRandomSamplingConfig{
				Samples: 10,
			},
			Snapshot: &RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			KeysOnly:       true,
			ScopeName:      scopeName,
			CollectionName: collectionName,
		}, func(res RangeScanCreateResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("RangeScanCreate operation failed: %v", err)
				}

				createRes = res.(*rangeScanCreateResult)
			})
		}))
	s.Wait(0)

	createRes.connID = "somethingwrong"

	_, err = createRes.RangeScanContinue(RangeScanContinueOptions{
		Deadline: time.Now().Add(10 * time.Second),
	}, func(items []RangeScanItem) {
	}, func(result *RangeScanContinueResult, err error) {
	})
	suite.Require().ErrorIs(err, ErrConnectionIDInvalid)
}

func (suite *StandardTestSuite) verifyRangeScanTelemetry(agent *Agent) {
	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(2, len(nilParents)) {
			suite.AssertOpSpan(nilParents[0], "RangeScanCreate", agent.BucketName(), memd.CmdRangeScanCreate.Name(), 1, false, "")
			suite.AssertOpSpan(nilParents[1], "RangeScanContinue", agent.BucketName(), memd.CmdRangeScanContinue.Name(), 1, false, "")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "RangeScanCreate", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "RangeScanContinue", 1, false, false)
}

func (suite *StandardTestSuite) doRangeScan(vbID uint16, opts RangeScanCreateOptions,
	contOpts RangeScanContinueOptions) []RangeScanItem {
	agent, s := suite.GetAgentAndHarness()

	var createRes RangeScanCreateResult
	s.PushOp(agent.RangeScanCreate(vbID, opts, func(res RangeScanCreateResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("RangeScanCreate operation failed: %v", err)
			}

			createRes = res
		})
	}))
	s.Wait(0)

	var data []RangeScanItem
	for {
		more := make(chan struct{}, 1)
		s.PushOp(createRes.RangeScanContinue(contOpts, func(items []RangeScanItem) {
			data = append(data, items...)
		}, func(res *RangeScanContinueResult, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("RangeScanContinue operation failed: %v", err)
					return
				}

				if res.Complete {
					close(more)
					return
				}

				if res.More {
					more <- struct{}{}
				}
			})
		}))
		s.Wait(0)

		_, cont := <-more
		if !cont {
			break
		}
	}

	return data
}
