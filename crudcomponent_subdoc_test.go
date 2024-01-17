package gocbcore

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *StandardTestSuite) TestSubdocXattrs() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testXattr"),
		Value:          []byte("{\"x\":\"xattrs\"}"),
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

	mutateOps := []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagMkDirP,
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
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}
	s.PushOp(agent.MutateIn(MutateInOptions{
		Key:            []byte("testXattr"),
		Ops:            mutateOps,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "xatest",
		},
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
		},
	}
	s.PushOp(agent.LookupIn(LookupInOptions{
		Key:            []byte("testXattr"),
		Ops:            lookupOps,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
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

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "MutateIn", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "LookupIn", 1, false, false)
}

func (suite *StandardTestSuite) TestSubdocXattrsReorder() {
	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testXattrReorder"),
		Value:          []byte("{\"x\":\"xattrs\", \"y\":\"yattrs\" }"),
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

	// This should reorder the ops before sending to the server.
	// We put the delete last to ensure that the xattr order is preserved.
	mutateOps := []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagMkDirP,
			Path:  "xatest.test",
			Value: []byte("\"test value\""),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "xatest.ytest",
			Value: []byte("\"test value2\""),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "xatest.ztest",
			Value: []byte("\"test valuez\""),
		},
		{
			Op:    memd.SubDocOpDelete,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "xatest.ztest",
		},
	}
	s.PushOp(agent.MutateIn(MutateInOptions{
		Key:            []byte("testXattrReorder"),
		Ops:            mutateOps,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Mutate operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if len(res.Ops) != 5 {
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
			if res.Ops[3].Err != nil {
				s.Fatalf("MutateIn operation 4 failed: %v", res.Ops[2].Err)
			}
			if res.Ops[4].Err != nil {
				s.Fatalf("MutateIn operation 5 failed: %v", res.Ops[2].Err)
			}
		})
	}))
	s.Wait(0)

	lookupOps := []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "xatest.test",
		},
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
		},
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "xatest.ytest",
		},
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "xatest.ztest",
		},
	}
	s.PushOp(agent.LookupIn(LookupInOptions{
		Key:            []byte("testXattrReorder"),
		Ops:            lookupOps,
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *LookupInResult, err error) {
		s.Wrap(func() {
			if len(res.Ops) != 4 {
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
			if res.Ops[3].Err == nil {
				s.Fatalf("Lookup operation 4 should have failed")
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

// Create As Deleted
func (suite *StandardTestSuite) TestTombstones() {
	suite.EnsureSupportsFeature(TestFeatureCreateDeleted)

	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.MutateIn(MutateInOptions{
		Key:   []byte("TestInsertTombstoneWithXattr"),
		Flags: memd.SubdocDocFlagCreateAsDeleted | memd.SubdocDocFlagAccessDeleted | memd.SubdocDocFlagMkDoc,
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Value: []byte("{\"test\":\"test\"}"),
				Path:  "test",
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.LookupIn(LookupInOptions{
		Key:   []byte("TestInsertTombstoneWithXattr"),
		Flags: memd.SubdocDocFlagAccessDeleted,
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Path:  "test",
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *LookupInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}

			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if len(res.Ops) != 1 {
				s.Fatalf("LookupIn operation wrong count was %d", len(res.Ops))
			}
			if res.Ops[0].Err != nil {
				s.Fatalf("LookupIn operation failed: %v", res.Ops[0].Err)
			}
			if len(res.Ops[0].Value) == 0 {
				s.Fatalf("LookupIn operation returned no value")
			}
			if !res.Internal.IsDeleted {
				s.Fatalf("LookupIn operation should have return IDeleted==true")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.LookupIn(LookupInOptions{
		Key: []byte("TestInsertTombstoneWithXattr"),
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Path:  "test",
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *LookupInResult, err error) {
		s.Wrap(func() {
			if err == nil {
				s.Fatalf("Get operation should have failed")
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) TestReplaceBodyWithXattr() {
	suite.EnsureSupportsFeature(TestFeatureReplaceBodyWithXattr)

	agent, s := suite.GetAgentAndHarness()

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testReplaceBodyWithXattr"),
		Value:          []byte("{\"mybody\":\"isnotxattrs\"}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.MutateIn(MutateInOptions{
		Key: []byte("testReplaceBodyWithXattr"),
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Path:  "mybodyafterward",
				Value: []byte("{\"mybody\":\"willbethexattrafterwardthough\"}"),
				Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagMkDirP,
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("MutateIn operation failed %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.MutateIn(MutateInOptions{
		Key: []byte("testReplaceBodyWithXattr"),
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpReplaceBodyWithXattr,
				Path:  "mybodyafterward",
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("MutateIn operation failed %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get(GetOptions{
		Key:            []byte("testReplaceBodyWithXattr"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}

			var body map[string]string
			err = json.Unmarshal(res.Value, &body)
			if err != nil {
				s.Fatalf("Unmarshal failed %v", err)
			}

			if len(body) != 1 {
				s.Fatalf("Expected body contain one key, was %v", body)
			}

			val, ok := body["mybody"]
			if !ok {
				s.Fatalf("Expected body contain mybody, was %v", body)
			}

			if val != "willbethexattrafterwardthough" {
				s.Fatalf("Expected mybody value to be willbethexattrafterwardthough was %v", val)
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) TestMutateInNoOps() {
	agent := suite.DefaultAgent()

	_, err := agent.MutateIn(MutateInOptions{
		Key: []byte("TestMutateInNoOps"),
	}, func(result *MutateInResult, err error) {
	})
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected invalid argument error but was %v", err)
	}
}

func (suite *StandardTestSuite) TestMutateInInsertString() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinInsertString")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{})

	expectedVal := suite.mustMarshal("bar2")
	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictAdd,
			Path:  "foo2",
			Value: expectedVal,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	suite.Require().Contains(target, "foo2")
	if !bytes.Equal(expectedVal, target["foo2"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(target["foo2"]), string(val))
	}
}

func (suite *StandardTestSuite) TestMutateInRemove() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinRemove")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": "bar",
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:   memd.SubDocOpDelete,
			Path: "foo",
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	suite.Require().NotContains(target, "foo2")
}

func (suite *StandardTestSuite) TestMutateInInsertStringExists() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinInsertStringExists")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": "bar",
	})

	expectedVal := suite.mustMarshal("bar2")
	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictAdd,
			Path:  "foo",
			Value: expectedVal,
		},
	}, key, cas, 0)
	if !errors.Is(err, ErrPathExists) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathExists, err)
	}
}

func (suite *StandardTestSuite) TestMutateInReplaceString() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinReplaceString")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": "bar",
	})

	expectedVal := suite.mustMarshal("bar2")
	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpReplace,
			Path:  "foo",
			Value: expectedVal,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	suite.Require().Contains(target, "foo")
	if !bytes.Equal(expectedVal, target["foo"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(target["foo2"]), string(val))
	}
}

func (suite *StandardTestSuite) TestMutateInReplaceFullDoc() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinReplaceFullDoc")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": "bar",
	})

	expectedVal := suite.mustMarshal(map[string]interface{}{
		"foo2": "bar2",
	})
	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpSetDoc,
			Path:  "",
			Value: expectedVal,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)

	if !bytes.Equal(expectedVal, val) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(val))
	}
}

func (suite *StandardTestSuite) TestMutateInReplaceStringDoesntExist() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinReplaceStringDoesntExist")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{})

	expectedVal := suite.mustMarshal("bar2")
	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpReplace,
			Path:  "foo",
			Value: expectedVal,
		},
	}, key, cas, 0)
	if !errors.Is(err, ErrPathNotFound) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathNotFound, err)
	}
}

func (suite *StandardTestSuite) TestMutateInSetString() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinSetString")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": "bar",
	})

	expectedVal := suite.mustMarshal("bar2")
	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "foo",
			Value: expectedVal,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	suite.Require().Contains(target, "foo")
	if !bytes.Equal(expectedVal, target["foo"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(target["foo2"]), string(val))
	}
}

func (suite *StandardTestSuite) TestMutateInSetStringDoesNotExist() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinSetStringDoesNotExist")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{})

	expectedVal := suite.mustMarshal("bar")
	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "foo",
			Value: expectedVal,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	suite.Require().Contains(target, "foo")
	if !bytes.Equal(expectedVal, target["foo"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(target["foo2"]), string(val))
	}
}

func (suite *StandardTestSuite) TestMutateInArrayAppend() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinArrayAppend")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": []string{"hello"},
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpArrayPushLast,
			Path:  "foo",
			Value: suite.mustMarshal("world"),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	expectedVal := suite.mustMarshal([]string{"hello", "world"})
	suite.Require().Contains(target, "foo")
	if !bytes.Equal(expectedVal, target["foo"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(target["foo"]))
	}
}

func (suite *StandardTestSuite) TestMutateInArrayAddUnique() {
	if suite.IsMockServer() {
		suite.T().Skip("Test skipped due to mock bug")
	}

	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinArrayAddUnique")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": []string{"hello", "world"},
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpArrayAddUnique,
			Path:  "foo",
			Value: suite.mustMarshal("cruel"),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	expectedVal := suite.mustMarshal([]string{"hello", "world", "cruel"})
	suite.Require().Contains(target, "foo")
	if !bytes.Equal(expectedVal, target["foo"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(target["foo"]))
	}
}

func (suite *StandardTestSuite) TestMutateInArrayAddUniqueAlreadyExists() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinArrayAddUniqueAlreadyExist")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": []string{"hello", "world", "cruel"},
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpArrayAddUnique,
			Path:  "foo",
			Value: suite.mustMarshal("cruel"),
		},
	}, key, cas, 0)
	if !errors.Is(err, ErrPathExists) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathNotFound, err)
	}
}

func (suite *StandardTestSuite) TestMutateInCounterIncrement() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinCounterIncrement")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": 10,
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpCounter,
			Path:  "foo",
			Value: suite.mustMarshal(5),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	expectedVal := suite.mustMarshal(15)
	suite.Require().Contains(target, "foo")
	if !bytes.Equal(expectedVal, target["foo"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(target["foo"]))
	}
}

func (suite *StandardTestSuite) TestMutateInCounterDecrement() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinCounterDecrement")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": 10,
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpCounter,
			Path:  "foo",
			Value: suite.mustMarshal(-5),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, _ := suite.mustGetDoc(agent, s, key)
	var target map[string]json.RawMessage
	err = json.Unmarshal(val, &target)
	suite.Require().Nil(err)

	expectedVal := suite.mustMarshal(5)
	suite.Require().Contains(target, "foo")
	if !bytes.Equal(expectedVal, target["foo"]) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(target["foo"]))
	}
}

func (suite *StandardTestSuite) TestMutateInRemoveXattr() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinRemoveXAttr")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: []byte("\"bar\""),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)
	suite.Require().Nil(err, err)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDelete,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	if !errors.Is(val.Ops[0].Err, ErrPathNotFound) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathNotFound, err)
	}
}

func (suite *StandardTestSuite) TestMutateInRemoveXattrDoesNotExist() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinRemoveXAttrNotExist")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDelete,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key, cas, 0)
	if !errors.Is(err, ErrPathNotFound) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathNotFound, err)
	}
}

func (suite *StandardTestSuite) TestMutateInInsertXattrExists() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinInsertXAttrExists")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: []byte("\"bar\""),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)
	suite.Require().Nil(err, err)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictAdd,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: []byte("\"bar\""),
		},
	}, key, cas, 0)
	if !errors.Is(err, ErrPathExists) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathExists, err)
	}
}

func (suite *StandardTestSuite) TestMutateInReplaceXattr() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinReplaceXAttr")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: []byte("\"bar\""),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)
	suite.Require().Nil(err, err)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpReplace,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
			Value: []byte("\"bar2\""),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	res, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	if !bytes.Equal([]byte("\"bar2\""), res.Ops[0].Value) {
		suite.T().Fatalf("Expected value to be %v but was %v", "\"bar2\"", string(res.Ops[0].Value))
	}
}

func (suite *StandardTestSuite) TestMutateInReplaceXattrNotExist() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinReplaceXAttrNotExist")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)
	suite.Require().Nil(err, err)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpReplace,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
			Value: []byte("\"bar2\""),
		},
	}, key, cas, 0)
	if !errors.Is(err, ErrPathNotFound) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathNotFound, err)
	}
}

func (suite *StandardTestSuite) TestMutateInSetXattr() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinSetXattr")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: []byte("\"bar\""),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)
	suite.Require().Nil(err, err)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
			Value: []byte("\"bar2\""),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	res, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	if !bytes.Equal([]byte("\"bar2\""), res.Ops[0].Value) {
		suite.T().Fatalf("Expected value to be %v but was %v", "\"bar2\"", string(res.Ops[0].Value))
	}
}

func (suite *StandardTestSuite) TestMutateInSetXattrNotExist() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinSetXAttrNotExist")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)
	suite.Require().Nil(err, err)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
			Value: []byte("\"bar2\""),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	res, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	if !bytes.Equal([]byte("\"bar2\""), res.Ops[0].Value) {
		suite.T().Fatalf("Expected value to be %v but was %v", "\"bar2\"", string(res.Ops[0].Value))
	}
}

func (suite *StandardTestSuite) TestMutateInArrayAppendXattr() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinArrayAppendXattr")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal([]string{"hello"}),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpArrayPushLast,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal("world"),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	expectedVal := suite.mustMarshal([]string{"hello", "world"})
	if !bytes.Equal(expectedVal, val.Ops[0].Value) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(val.Ops[0].Value))
	}
}

func (suite *StandardTestSuite) TestMutateInArrayAddUniqueXattr() {
	if suite.IsMockServer() {
		suite.T().Skip("Test skipped due to mock bug")
	}

	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinArrayAddUniqueXattr")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal([]string{"hello", "world"}),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpArrayAddUnique,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal("cruel"),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	expectedVal := suite.mustMarshal([]string{"hello", "world", "cruel"})
	if !bytes.Equal(expectedVal, val.Ops[0].Value) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(val.Ops[0].Value))
	}
}

func (suite *StandardTestSuite) TestMutateInArrayAddUniqueAlreadyExistsXattr() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinArrayAddUniqueAlreadyExistXattr")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal([]string{"hello", "world", "cruel"}),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpArrayAddUnique,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal("cruel"),
		},
	}, key, cas, 0)
	if !errors.Is(err, ErrPathExists) {
		suite.T().Fatalf("Expected error to be %v but was %v", ErrPathNotFound, err)
	}
}

func (suite *StandardTestSuite) TestMutateInCounterIncrementXattr() {
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinCounterIncrementXattr")
	cas, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal(10),
		},
		{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}, key, 0, memd.SubdocDocFlagMkDoc)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpCounter,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "foo",
			Value: suite.mustMarshal(5),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	expectedVal := suite.mustMarshal(15)
	if !bytes.Equal(expectedVal, val.Ops[0].Value) {
		suite.T().Fatalf("Expected value to be %v but was %v", string(expectedVal), string(val.Ops[0].Value))
	}
}

func (suite *StandardTestSuite) TestMutateInExpandMacroCas() {
	suite.EnsureSupportsFeature(TestFeatureExpandMacros)
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinExpandMacroCas")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "foo",
			Value: suite.mustMarshal("${Mutation.CAS}"),
			Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "foo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	var resultCas string
	err = json.Unmarshal(val.Ops[0].Value, &resultCas)
	suite.Require().Nil(err, err)

	// We should improve this to check the actual cas value.
	suite.Require().NotEqual("${Mutation.CAS}", resultCas)
}

func (suite *StandardTestSuite) TestMutateInExpandMacroCRC32() {
	suite.EnsureSupportsFeature(TestFeatureExpandMacros)
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinExpandMacroCRC32")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": "bar",
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "xfoo",
			Value: suite.mustMarshal("${Mutation.value_crc32c}"),
			Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		},
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "foo",
			Value: suite.mustMarshal("value"),
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	// We need to do 2 ops because older server versions only allow a single xattr lookup.
	val, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "xfoo",
			Flags: memd.SubdocFlagXattrPath,
		},
		{
			Op:   memd.SubDocOpGet,
			Path: "foo",
		},
	}, key)
	suite.Require().Nil(err, err)

	suite.Require().Nil(val.Ops[0].Err, val.Ops[0].Err)

	documentVal, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "$document",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	suite.Require().Nil(documentVal.Ops[0].Err, documentVal.Ops[0].Err)

	var resultCRC string
	err = json.Unmarshal(val.Ops[0].Value, &resultCRC)
	suite.Require().Nil(err, err)

	// We pull the actual crc value from the doc metadata.
	crcStruct := struct {
		CRC32 string `json:"value_crc32c,omitempty"`
	}{}
	err = json.Unmarshal(documentVal.Ops[0].Value, &crcStruct)
	suite.Require().Nil(err, err)

	suite.Require().Equal(crcStruct.CRC32, resultCRC)
}

func (suite *StandardTestSuite) TestMutateInExpandMacroSeqNo() {
	suite.EnsureSupportsFeature(TestFeatureExpandMacros)
	suite.EnsureSupportsFeature(TestFeatureExpandMacrosSeqNo)
	agent, s := suite.GetAgentAndHarness()
	key := []byte("mutateinExpandMacroSeqNo")
	cas := suite.mustSetDoc(agent, s, key, map[string]interface{}{
		"foo": "bar",
	})

	_, err := suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "xfoo",
			Value: suite.mustMarshal("${Mutation.seqno}"),
			Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		},
	}, key, cas, 0)
	suite.Require().Nil(err, err)

	val, err := suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "xfoo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	suite.Require().Nil(val.Ops[0].Err, val.Ops[0].Err)

	var seqno string
	err = json.Unmarshal(val.Ops[0].Value, &seqno)
	suite.Require().Nil(err, err)

	suite.Require().NotZero(seqno)

	_, err = suite.mutateIn(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Path:  "xfoo",
			Value: suite.mustMarshal("${Mutation.seqno}"),
			Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		},
	}, key, val.Cas, 0)
	suite.Require().Nil(err, err)

	val, err = suite.lookupDoc(agent, s, []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Path:  "xfoo",
			Flags: memd.SubdocFlagXattrPath,
		},
	}, key)
	suite.Require().Nil(err, err)

	suite.Require().Nil(val.Ops[0].Err, val.Ops[0].Err)

	var seqno2 string
	err = json.Unmarshal(val.Ops[0].Value, &seqno2)
	suite.Require().Nil(err, err)

	suite.Require().NotZero(seqno2)

	// We test that performing 2 mutations creates a sequential sequence number.
	seqnoInt, err := strconv.ParseInt(strings.Replace(seqno, "0x", "", -1), 16, 64)
	suite.Require().Nil(err, err)

	seqno2Int, err := strconv.ParseInt(strings.Replace(seqno2, "0x", "", -1), 16, 64)
	suite.Require().Nil(err, err)

	suite.Require().Greater(seqno2Int, seqnoInt)
}

func (suite *StandardTestSuite) TestPreserveExpiryMutateIn() {
	suite.EnsureSupportsFeature(TestFeaturePreserveExpiry)

	agent, s := suite.GetAgentAndHarness()

	expiry := uint32(25)
	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testmutateinpreserveExpiry"),
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

	ops := []SubDocOp{
		{
			Op:    memd.SubDocOpDictSet,
			Value: []byte("{}"),
			Path:  "test",
		},
	}

	s.PushOp(agent.MutateIn(MutateInOptions{
		Key:            []byte("testmutateinpreserveExpiry"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		PreserveExpiry: true,
		Ops:            ops,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Mutatein operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	// Get
	s.PushOp(agent.GetMeta(GetMetaOptions{
		Key:            []byte("testmutateinpreserveExpiry"),
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
			suite.AssertOpSpan(nilParents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "testmutateinpreserveExpiry")
			suite.AssertOpSpan(nilParents[1], "MutateIn", agent.BucketName(), memd.CmdSubDocMultiMutation.Name(), 1, false, "testmutateinpreserveExpiry")
			suite.AssertOpSpan(nilParents[2], "GetMeta", agent.BucketName(), memd.CmdGetMeta.Name(), 1, false, "testmutateinpreserveExpiry")
		}
	}

	suite.VerifyKVMetrics(suite.meter, "Set", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "MutateIn", 1, false, false)
	suite.VerifyKVMetrics(suite.meter, "GetMeta", 1, false, false)
}

func (suite *StandardTestSuite) TestSubdocCasMismatch() {
	agent, s := suite.GetAgentAndHarness()
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testSubdocCasMismatch"),
		Value:          []byte("{\"x\":\"xattrs\", \"y\":\"yattrs\" }"),
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
	s.PushOp(agent.MutateIn(MutateInOptions{
		Key: []byte("testSubdocCasMismatch"),
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagNone,
				Path:  "x",
				Value: []byte("\"x value\""),
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Cas:            1234,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrCasMismatch) {
				s.Fatalf("Mutate operation should have failed with Cas Mismatch but was: %v", err)
			}
		})
	}))
	s.Wait(0)
	// With Add flag
	s.PushOp(agent.MutateIn(MutateInOptions{
		Key: []byte("testSubdocCasMismatch"),
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagNone,
				Path:  "x",
				Value: []byte("\"x value\""),
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		Flags:          memd.SubdocDocFlagAddDoc,
	}, func(res *MutateInResult, err error) {
		s.Wrap(func() {
			if !errors.Is(err, ErrDocumentExists) {
				s.Fatalf("Mutate operation should have failed with Exists but was: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func (suite *StandardTestSuite) TestLookupInReplicaReads() {
	suite.EnsureSupportsFeature(TestFeatureSubdocReplicaReads)

	agent, s := suite.GetAgentAndHarness()

	snap, err := agent.ConfigSnapshot()
	suite.Require().NoError(err, err)

	numReplicas, err := snap.NumReplicas()
	suite.Require().NoError(err, err)

	if numReplicas == 0 {
		suite.T().Skip("Skipping test due to no replicas configured")
	}

	key := []byte(uuid.NewString()[:6])
	value := []byte(`{"key":"value"}`)
	s.PushOp(agent.Set(SetOptions{
		Key:                    key,
		Value:                  value,
		CollectionName:         suite.CollectionName,
		ScopeName:              suite.ScopeName,
		DurabilityLevel:        memd.DurabilityLevelMajority,
		DurabilityLevelTimeout: 10 * time.Second,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.LookupIn(LookupInOptions{
		Key:   key,
		Flags: memd.SubdocDocFlagReplicaRead,
		Ops: []SubDocOp{
			{
				Op:   memd.SubDocOpGet,
				Path: "key",
			},
		},
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		ReplicaIdx:     1,
	}, func(res *LookupInResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}

			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if len(res.Ops) != 1 {
				s.Fatalf("LookupIn operation wrong count was %d", len(res.Ops))
			}
			if res.Ops[0].Err != nil {
				s.Fatalf("LookupIn operation failed: %v", res.Ops[0].Err)
			}
			if len(res.Ops[0].Value) == 0 {
				s.Fatalf("LookupIn operation returned no value")
			}
		})
	}))
	s.Wait(0)
}
