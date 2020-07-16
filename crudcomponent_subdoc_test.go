package gocbcore

import (
	"bytes"

	"github.com/couchbase/gocbcore/v9/memd"
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
