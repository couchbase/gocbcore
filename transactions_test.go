package gocbcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *StandardTestSuite) fetchStagedOpData(key string, agent *Agent, opHarness *TestSubHarness) (jsonMutationType, []byte, bool) {
	var res *LookupInResult
	var err error
	opHarness.PushOp(agent.LookupIn(LookupInOptions{
		Key: []byte(key),
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  "txn.op.type",
			},
			{
				Op:    memd.SubDocOpGet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  "txn.op.stgd",
			},
		},
		Flags:          memd.SubdocDocFlagAccessDeleted,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *LookupInResult, opErr error) {
		opHarness.Wrap(func() {
			err = opErr
			res = result
		})
	}))
	opHarness.Wait(0)

	if err != nil {
		return "", nil, false
	}

	var opType string
	err = json.Unmarshal(res.Ops[0].Value, &opType)
	if err != nil {
		return "", nil, false
	}

	stgdData := res.Ops[1].Value

	var exists bool
	opHarness.PushOp(agent.GetMeta(GetMetaOptions{
		Key:            []byte(key),
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *GetMetaResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				exists = false
				return
			}

			exists = result.Deleted == 0
		})
	}))
	opHarness.Wait(0)

	return jsonMutationType(opType), stgdData, exists
}

func (suite *StandardTestSuite) assertStagedDoc(key string, expOpType jsonMutationType, expStgdData []byte,
	expTombstone bool, agent *Agent, opHarness *TestSubHarness) {
	stgdOpType, stgdData, docExists := suite.fetchStagedOpData(key, agent, opHarness)

	suite.Assert().Equal(
		expOpType,
		stgdOpType,
		fmt.Sprintf("%s had an incorrect op type", key))
	suite.Assert().Equal(
		expStgdData,
		stgdData,
		fmt.Sprintf("%s had an incorrect staged data", key))
	suite.Assert().Equal(
		expTombstone,
		!docExists,
		fmt.Sprintf("%s document state did not match", key))
}

func (suite *StandardTestSuite) assertDocNotStaged(key string, agent *Agent, opHarness *TestSubHarness) {
	var res *LookupInResult
	var err error
	opHarness.PushOp(agent.LookupIn(LookupInOptions{
		Key: []byte(key),
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  "txn",
			},
		},
		Flags:          memd.SubdocDocFlagAccessDeleted,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *LookupInResult, opErr error) {
		opHarness.Wrap(func() {
			err = opErr
			res = result
		})
	}))
	opHarness.Wait(0)

	suite.Require().Nil(err, err)

	suite.Require().True(errors.Is(res.Ops[0].Err, ErrPathNotFound))
}

func (suite *StandardTestSuite) initTransactionAndAttempt(agent *Agent) (*TransactionsManager, *Transaction) {
	txns, err := InitTransactions(&TransactionsConfig{
		DurabilityLevel: TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime: 60 * time.Second,
	})
	suite.Require().Nil(err, err)

	txn, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, err)

	// Start the attempt
	err = txn.NewAttempt()
	suite.Require().Nil(err, err)

	return txns, txn
}

func (suite *StandardTestSuite) TestTransactionsInsertTxn1GetTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy2 := []byte(`{"name":"mike"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	_, err := testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertDoc`),
		Value:          testDummy2,
	})
	suite.Require().Nil(err, "insert of insertDoc failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	_, err = testBlkGet(txn2, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertDoc`),
	})
	suite.Require().True(errors.Is(err, ErrDocumentNotFound), "insertDoc get from T2 should have failed")

	suite.assertStagedDoc("insertDoc", jsonMutationInsert, testDummy2, true, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("insertDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsReplaceTxn1GetTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)
	testDummy2 := []byte(`{"name":"mike"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("replaceDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	replaceGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceDoc`),
	})
	suite.Require().Nil(err, "replaceDoc get failed")

	_, err = testBlkReplace(txn, TransactionReplaceOptions{
		Document: replaceGetRes,
		Value:    testDummy2,
	})
	suite.Require().Nil(err, "replaceDoc replace failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	getOfReplace, err := testBlkGet(txn2, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceDoc`),
	})
	suite.Require().Nil(err, "replaceDoc get from T2 should have succeeded")

	suite.Assert().Equal(testDummy1, getOfReplace.Value, "replaceDoc get from T2 should have right data")

	suite.assertStagedDoc("replaceDoc", jsonMutationReplace, testDummy2, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("replaceDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsRemoveTxn1GetTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("removeDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	removeGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeDoc`),
	})
	suite.Require().Nil(err, "removeDoc get failed")
	log.Printf("removeDoc get result: %+v", removeGetRes)

	removeRes, err := testBlkRemove(txn, TransactionRemoveOptions{
		Document: removeGetRes,
	})
	suite.Require().Nil(err, "removeRes remove failed")
	log.Printf("removeRes remove result: %+v", removeRes)

	txn = suite.serializeUnserializeTxn(txns, txn)

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	getOfRemove, err := testBlkGet(txn2, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeDoc`),
	})
	suite.Require().Nil(err, "removeDoc get from T2 should have succeeded")

	suite.Assert().Equal(testDummy1, getOfRemove.Value, "removeDoc get from T2 should have right data")

	suite.assertStagedDoc("removeDoc", jsonMutationRemove, []byte{}, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("removeDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsReplaceTxn1InsertTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)
	testDummy2 := []byte(`{"name":"mike"}`)
	testDummy3 := []byte(`{"name":"frank"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("replaceToInsertDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	replaceToInsertGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceToInsertDoc`),
	})
	suite.Require().Nil(err, "replaceToInsertDoc get failed")

	_, err = testBlkReplace(txn, TransactionReplaceOptions{
		Document: replaceToInsertGetRes,
		Value:    testDummy2,
	})
	suite.Require().Nil(err, "replaceToInsertDoc replace failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	// Cannot insert after serialize.

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	_, err = testBlkInsert(txn2, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceToInsertDoc`),
		Value:          testDummy3,
	})
	suite.Assert().Error(err, "replaceToInsertDoc insert from T2 should have failed")

	suite.assertStagedDoc("replaceToInsertDoc", jsonMutationReplace, testDummy2, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("replaceToInsertDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsReplaceTxn1ReplaceTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)
	testDummy2 := []byte(`{"name":"mike"}`)
	testDummy3 := []byte(`{"name":"frank"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("replaceToReplaceDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	replaceToReplaceGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceToReplaceDoc`),
	})
	suite.Require().Nil(err, "replaceToReplaceDoc get1 failed")

	replaceToReplaceReplaceRes, err := testBlkReplace(txn, TransactionReplaceOptions{
		Document: replaceToReplaceGetRes,
		Value:    testDummy2,
	})
	suite.Require().Nil(err, "replaceToReplaceDoc replace failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	replaceToReplaceGet2Res, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceToReplaceDoc`),
	})
	suite.Require().Nil(err, "replaceToReplaceDoc get2 failed")
	suite.Assert().Equal(replaceToReplaceReplaceRes.Cas, replaceToReplaceGet2Res.Cas, "replaceToReplaceDoc replace and get cas did not match")
	suite.Assert().Equal(replaceToReplaceReplaceRes.Meta, replaceToReplaceGet2Res.Meta, "replaceToReplaceDoc replace and get meta did not match")
	log.Printf("replaceToReplaceDoc get2 result: %+v", replaceToReplaceGet2Res)

	replaceToReplaceReplace2Res, err := testBlkReplace(txn, TransactionReplaceOptions{
		Document: replaceToReplaceGet2Res,
		Value:    testDummy3,
	})
	suite.Require().Nil(err, "replaceToReplaceDoc replace failed")
	log.Printf("replaceToReplaceDoc replace result: %+v", replaceToReplaceReplace2Res)

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	_, err = testBlkReplace(txn2, TransactionReplaceOptions{
		Document: replaceToReplaceGet2Res,
		Value:    testDummy1,
	})
	suite.Assert().Error(err, "replaceToReplaceDoc replace from T2 should have failed")

	suite.assertStagedDoc("replaceToReplaceDoc", jsonMutationReplace, testDummy3, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("replaceToReplaceDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsReplaceTxn1RemoveTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)
	testDummy2 := []byte(`{"name":"mike"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("replaceToRemoveDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	replaceToRemoveGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceToRemoveDoc`),
	})
	suite.Require().Nil(err, "replaceToRemoveDoc get1 failed")

	replaceToRemoveReplaceRes, err := testBlkReplace(txn, TransactionReplaceOptions{
		Document: replaceToRemoveGetRes,
		Value:    testDummy2,
	})
	suite.Require().Nil(err, "replaceToRemoveDoc replace failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	replaceToRemoveGet2Res, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`replaceToRemoveDoc`),
	})
	suite.Require().Nil(err, "replaceToRemoveDoc get2 failed")
	suite.Assert().Equal(replaceToRemoveReplaceRes.Cas, replaceToRemoveGet2Res.Cas, "replaceToRemoveDoc replace and get cas did not match")
	suite.Assert().Equal(replaceToRemoveReplaceRes.Meta, replaceToRemoveGet2Res.Meta, "replaceToRemoveDoc replace and get meta did not match")
	log.Printf("replaceToRemoveDoc get2 result: %+v", replaceToRemoveGet2Res)

	replaceToRemoveRemoveRes, err := testBlkRemove(txn, TransactionRemoveOptions{
		Document: replaceToRemoveGet2Res,
	})
	suite.Require().Nil(err, "replaceToRemoveDoc remove failed")
	log.Printf("replaceToRemoveDoc remove result: %+v", replaceToRemoveRemoveRes)

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	_, err = testBlkRemove(txn2, TransactionRemoveOptions{
		Document: replaceToRemoveGet2Res,
	})
	suite.Assert().Error(err, "replaceToRemoveDoc remove from T2 should have failed")

	suite.assertStagedDoc("replaceToRemoveDoc", jsonMutationRemove, []byte{}, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("replaceToRemoveDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsRemoveTxn1InsertTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)
	testDummy3 := []byte(`{"name":"frank"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("removeToInsertDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	removeToInsertGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeToInsertDoc`),
	})
	suite.Require().Nil(err, "removeToInsertDoc get failed")

	_, err = testBlkRemove(txn, TransactionRemoveOptions{
		Document: removeToInsertGetRes,
	})
	suite.Require().Nil(err, "removeToInsertDoc remove failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	_, err = testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeToInsertDoc`),
		Value:          testDummy3,
	})
	suite.Require().Nil(err, "removeToInsertDoc insert failed")

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	_, err = testBlkInsert(txn2, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeToInsertDoc`),
		Value:          testDummy1,
	})
	suite.Assert().Error(err, "removeToInsertDoc insert from T2 should have failed")

	suite.assertStagedDoc("removeToInsertDoc", jsonMutationReplace, testDummy3, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("removeToInsertDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsRemoveTxn1ReplaceTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)
	testDummy3 := []byte(`{"name":"frank"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("removeToReplaceDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	removeToReplaceGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeToReplaceDoc`),
	})
	suite.Require().Nil(err, "removeToReplaceDoc get failed")

	_, err = testBlkRemove(txn, TransactionRemoveOptions{
		Document: removeToReplaceGetRes,
	})
	suite.Require().Nil(err, "removeToReplaceDoc remove failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	_, err = testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeToReplaceDoc`),
	})
	suite.Require().NotNil(err, "removeToReplaceDoc get2 should have failed")

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	_, err = testBlkReplace(txn2, TransactionReplaceOptions{
		Document: removeToReplaceGetRes,
		Value:    testDummy3,
	})
	suite.Assert().Error(err, "removeDoc replace from T2 should have failed")

	suite.assertStagedDoc("removeToReplaceDoc", jsonMutationRemove, []byte{}, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("removeToReplaceDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsRemoveTxn1RemoveTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy1 := []byte(`{"name":"joel"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	opHarness.PushOp(agent.Set(SetOptions{
		Key:            []byte("removeToRemoveDoc"),
		Value:          testDummy1,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}, func(result *StoreResult, err error) {
		opHarness.Wrap(func() {
			if err != nil {
				opHarness.Fatalf("Set command failed: %v", err)
			}
		})
	}))
	opHarness.Wait(0)

	removeToRemoveGetRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeToRemoveDoc`),
	})
	suite.Require().Nil(err, "removeToRemoveDoc get failed")

	_, err = testBlkRemove(txn, TransactionRemoveOptions{
		Document: removeToRemoveGetRes,
	})
	suite.Require().Nil(err, "removeToRemoveDoc remove failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	_, err = testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`removeToRemoveDoc`),
	})
	suite.Require().NotNil(err, "removeToRemoveDoc get2 should have failed")

	suite.assertStagedDoc("removeToRemoveDoc", jsonMutationRemove, []byte{}, false, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("removeToRemoveDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsInsertTxn1InsertTxn2() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy2 := []byte(`{"name":"mike"}`)
	testDummy3 := []byte(`{"name":"frank"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	_, err := testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertToInsertDoc`),
		Value:          testDummy2,
	})
	suite.Require().Nil(err, "insertToInsertDoc insert failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	// No insert here, that'd fail the commit.

	txn2, err := txns.BeginTransaction(nil)
	suite.Require().Nil(err, "txn2 begin failed")

	err = txn2.NewAttempt()
	suite.Require().Nil(err, "txn2 attempt start failed")

	_, err = testBlkInsert(txn2, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertToInsertDoc`),
		Value:          testDummy3,
	})
	suite.Require().Error(err, "insertToInsertDoc insert from T2 should have failed")

	suite.assertStagedDoc("insertToInsertDoc", jsonMutationInsert, testDummy2, true, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("insertToInsertDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsInsertReplace() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy2 := []byte(`{"name":"mike"}`)
	testDummy3 := []byte(`{"name":"frank"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	insertToReplaceInsertRes, err := testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertToReplaceDoc`),
		Value:          testDummy2,
	})
	suite.Require().Nil(err, "insertToReplaceDoc insert failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	insertToReplaceGet2Res, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertToReplaceDoc`),
	})
	suite.Require().Nil(err, "insertToReplaceDoc get2 failed")
	suite.Assert().Equal(insertToReplaceInsertRes.Cas, insertToReplaceGet2Res.Cas, "insertToReplaceDoc insert and get cas did not match")
	suite.Assert().Equal(insertToReplaceInsertRes.Meta, insertToReplaceGet2Res.Meta, "insertToReplaceDoc insert and get meta did not match")
	log.Printf("insertToReplaceDoc get2 result: %+v", insertToReplaceGet2Res)

	insertToReplaceReplaceRes, err := testBlkReplace(txn, TransactionReplaceOptions{
		Document: insertToReplaceInsertRes,
		Value:    testDummy3,
	})
	suite.Require().Nil(err, "insertToReplaceDoc replace failed")
	log.Printf("insertToReplaceDoc replace result: %+v", insertToReplaceReplaceRes)

	// Impossible to have a txn2 with a replace.

	suite.assertStagedDoc("insertToReplaceDoc", jsonMutationInsert, testDummy3, true, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged("insertToReplaceDoc", agent, opHarness)
}

func (suite *StandardTestSuite) TestTransactionsInsertRemove() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy2 := []byte(`{"name":"mike"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	insertToRemoveInsertRes, err := testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertToRemoveDoc`),
		Value:          testDummy2,
	})
	suite.Require().Nil(err, "insertToRemoveDoc insert failed")

	txn = suite.serializeUnserializeTxn(txns, txn)

	insertToRemoveGet2Res, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(`insertToRemoveDoc`),
	})
	suite.Require().Nil(err, "insertToRemoveDoc get2 failed")
	suite.Assert().Equal(insertToRemoveInsertRes.Cas, insertToRemoveGet2Res.Cas, "insertToRemoveDoc insert and get cas did not match")
	suite.Assert().Equal(insertToRemoveInsertRes.Meta, insertToRemoveGet2Res.Meta, "insertToRemoveDoc insert and get meta did not match")

	_, err = testBlkRemove(txn, TransactionRemoveOptions{
		Document: insertToRemoveGet2Res,
	})
	suite.Require().Nil(err, "insertToRemoveDoc remove failed")

	// Impossible to have a txn2 with a remove.

	suite.assertStagedDoc("insertToRemoveDoc", "", nil, true, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertStagedDoc("insertToRemoveDoc", "", nil, true, agent, opHarness)
}

// This test ensures that the addLostCleanupLocation function is registered on an attempt
// after serialization/deserialization of the transaction.
func (suite *StandardTestSuite) TestTransactionsInsertRemoveEarlySerialize() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy2 := []byte(`{"name":"mike"}`)

	txns, txn := suite.initTransactionAndAttempt(agent)

	// We do this before any ops so that we trigger addLostCleanupLocation after setting the ATR to pending.
	txn = suite.serializeUnserializeTxn(txns, txn)

	docId := []byte(uuid.NewString())
	insertToRemoveInsertRes, err := testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            docId,
		Value:          testDummy2,
	})
	suite.Require().Nil(err, "Early serialization insert failed")

	insertToRemoveGet2Res, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            docId,
	})
	suite.Require().Nil(err, "Early serialization get2 failed")
	suite.Assert().Equal(insertToRemoveInsertRes.Cas, insertToRemoveGet2Res.Cas, "Early serialization insert and get cas did not match")
	suite.Assert().Equal(insertToRemoveInsertRes.Meta, insertToRemoveGet2Res.Meta, "Early serialization insert and get meta did not match")

	_, err = testBlkRemove(txn, TransactionRemoveOptions{
		Document: insertToRemoveGet2Res,
	})
	suite.Require().Nil(err, "Early serialization remove failed")

	// Impossible to have a txn2 with a remove.

	suite.assertStagedDoc(string(docId), "", nil, true, agent, opHarness)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertStagedDoc(string(docId), "", nil, true, agent, opHarness)
}

func (suite *StandardTestSuite) serializeUnserializeTxn(txns *TransactionsManager, txn *Transaction) *Transaction {
	txnBytes, err := testBlkSerialize(txn)
	suite.Require().Nil(err, "txn serialize failed")

	txn, err = txns.ResumeTransactionAttempt(txnBytes, nil)
	suite.Require().Nil(err, err, "txn resume failed")

	return txn
}

func (suite *StandardTestSuite) TestTransactionsLogger() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	agent, opHarness := suite.GetAgentAndHarness()

	testDummy2 := []byte(`{"name":"mike"}`)

	txns, err := InitTransactions(&TransactionsConfig{
		DurabilityLevel: TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime: 60 * time.Second,
	})
	suite.Require().Nil(err, err)

	logger := NewInMemoryTransactionLogger()
	txn, err := txns.BeginTransaction(&TransactionOptions{
		TransactionLogger: logger,
	})
	suite.Require().Nil(err, err)

	// Start the attempt
	err = txn.NewAttempt()
	suite.Require().Nil(err, err)

	key := uuid.NewString()
	_, err = testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(key),
		Value:          testDummy2,
	})
	suite.Require().Nil(err, "insert failed")

	_, err = testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(key),
	})
	suite.Require().Nil(err, "get failed")

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	suite.assertDocNotStaged(key, agent, opHarness)

	entries := logger.Logs()
	for _, entry := range entries {
		suite.Assert().NotZero(entry.Level)
		items := strings.SplitN(entry.String(), " ", 3)
		if suite.Assert().Len(items, 3) {
			suite.Assert().Len(items[0], 12)
			suite.Assert().Equal(txn.ID()[:5]+"/"+txn.Attempt().ID[:5], items[1])
			suite.Assert().NotEmpty(items[2])
		}
	}
}

func (suite *StandardTestSuite) TestTransactionsInsertGetReplaceRemoveCommitUnits() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	suite.EnsureSupportsFeature(TestFeatureResourceUnits)
	agent, _ := suite.GetAgentAndHarness()

	val1 := []byte(`{"name":"mike"}`)
	val2 := []byte(`{"name":"dave"}`)

	txns, err := InitTransactions(&TransactionsConfig{
		DurabilityLevel: TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime: 60 * time.Second,
	})
	suite.Require().Nil(err, err)

	opts := &TransactionOptions{}
	var readUnits uint32
	var writeUnits uint32
	var numOps uint32
	opts.Internal.ResourceUnitCallback = func(result *ResourceUnitResult) {
		readUnits += uint32(result.ReadUnits)
		writeUnits += uint32(result.WriteUnits)
		numOps++
	}
	txn, err := txns.BeginTransaction(opts)
	suite.Require().Nil(err, err)

	// Start the attempt
	err = txn.NewAttempt()
	suite.Require().Nil(err, err)

	key := uuid.NewString()

	_, err = testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(key),
		Value:          val1,
	})
	suite.Require().Nil(err, "insert failed")

	// We expect there to be 2 writes - the atr and the doc itself.
	suite.Assert().Equal(uint32(2), writeUnits)
	suite.Assert().Equal(uint32(0), readUnits)
	suite.Assert().Equal(uint32(2), numOps)

	getRes, err := testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(key),
	})
	suite.Require().Nil(err, "get failed")

	// Get should add a read - of the doc.
	suite.Assert().Equal(uint32(2), writeUnits)
	suite.Assert().Equal(uint32(1), readUnits)
	suite.Assert().Equal(uint32(3), numOps)

	_, err = testBlkReplace(txn, TransactionReplaceOptions{
		Document: getRes,
		Value:    val2,
	})
	suite.Require().Nil(err, "replace failed")

	// Replace should add a write and a read (as one op) - of the doc.
	suite.Assert().Equal(uint32(3), writeUnits)
	suite.Assert().Equal(uint32(2), readUnits)
	suite.Assert().Equal(uint32(4), numOps)

	getRes, err = testBlkGet(txn, TransactionGetOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(key),
	})
	suite.Require().Nil(err, "get failed")

	// Get should add a read - of the doc.
	suite.Assert().Equal(uint32(3), writeUnits)
	suite.Assert().Equal(uint32(3), readUnits)
	suite.Assert().Equal(uint32(5), numOps)

	_, err = testBlkRemove(txn, TransactionRemoveOptions{
		Document: getRes,
	})
	suite.Require().Nil(err, "remove failed")

	// Remove should add a write and a read (as one op) - of the doc.
	suite.Assert().Equal(uint32(4), writeUnits)
	suite.Assert().Equal(uint32(4), readUnits)
	suite.Assert().Equal(uint32(6), numOps)

	err = testBlkCommit(txn)
	suite.Require().Nil(err, "commit failed")

	// Commit should add a write and a read (as one op) - of the atr and the doc.
	suite.Assert().Equal(uint32(6), writeUnits)
	suite.Assert().Equal(uint32(6), readUnits)
	suite.Assert().Equal(uint32(8), numOps)
}

func (suite *StandardTestSuite) TestTransactionsInsertRollbacktUnits() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)
	suite.EnsureSupportsFeature(TestFeatureResourceUnits)
	agent, _ := suite.GetAgentAndHarness()

	val1 := []byte(`{"name":"mike"}`)
	txns, err := InitTransactions(&TransactionsConfig{
		DurabilityLevel: TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime: 60 * time.Second,
	})
	suite.Require().Nil(err, err)

	opts := &TransactionOptions{}
	var readUnits uint32
	var writeUnits uint32
	var numOps uint32
	opts.Internal.ResourceUnitCallback = func(result *ResourceUnitResult) {
		readUnits += uint32(result.ReadUnits)
		writeUnits += uint32(result.WriteUnits)
		numOps++
	}
	txn, err := txns.BeginTransaction(opts)
	suite.Require().Nil(err, err)

	// Start the attempt
	err = txn.NewAttempt()
	suite.Require().Nil(err, err)

	key := uuid.NewString()

	_, err = testBlkInsert(txn, TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
		Key:            []byte(key),
		Value:          val1,
	})
	suite.Require().Nil(err, "insert failed")

	// We expect there to be 2 writes - the atr and the doc itself.
	suite.Assert().Equal(uint32(2), writeUnits)
	suite.Assert().Equal(uint32(0), readUnits)
	suite.Assert().Equal(uint32(2), numOps)

	err = testBlkRollback(txn)
	suite.Require().Nil(err, "rollback failed")

	// Rollback should add 3 writes and 3 reads - there are 2 requests against the atr and one against the doc.
	suite.Assert().Equal(uint32(5), writeUnits)
	suite.Assert().Equal(uint32(3), readUnits)
	suite.Assert().Equal(uint32(5), numOps)
}
