// Copyright 2021 Couchbase
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gocbcore

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *UnitTestSuite) TestParseCas() {
	// assertEquals(1539336197457L, ActiveTransactionRecord.parseMutationCAS("0x000058a71dd25c15"));
	cas, err := parseCASToMilliseconds("0x000058a71dd25c15")
	suite.Require().Nil(err)

	suite.Require().Equal(int64(1539336197457), cas)
}

func (suite *StandardTestSuite) TestLostCleanupProcessClientSuccessfulTxn() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)

	agent, s := suite.GetAgentAndTxnHarness()
	h := suite.GetHarness()

	h.PushOp(agent.Delete(DeleteOptions{
		Key: clientRecordKey,
	}, func(result *DeleteResult, err error) {
		h.Wrap(func() {
			if err != nil && !errors.Is(err, ErrDocumentNotFound) {
				s.Fatalf("Remove operation failed: %v", err)
			}
		})
	}))
	h.Wait(0)

	transactions, err := InitTransactions(&TransactionsConfig{
		DurabilityLevel: TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime:      500 * time.Millisecond,
		KeyValueTimeout:     2500 * time.Millisecond,
		CleanupLostAttempts: false,
	})
	if err != nil {
		log.Printf("InitTransactions failed: %+v", err)
		panic(err)
	}

	clientUUID := uuid.New().String()
	config := &TransactionsConfig{}
	config.DurabilityLevel = TransactionDurabilityLevelNone
	config.BucketAgentProvider = func(bucketName string) (*Agent, string, error) {
		// We can always return just this one agent as we only actually
		// use a single bucket for this entire test.
		return agent, "", nil
	}
	config.CleanupWindow = 1 * time.Second
	config.ExpirationTime = 500 * time.Millisecond
	config.KeyValueTimeout = 2500 * time.Millisecond
	config.Internal.Hooks = nil
	config.Internal.CleanUpHooks = &TransactionDefaultCleanupHooks{}
	config.Internal.ClientRecordHooks = &TransactionDefaultClientRecordHooks{}
	config.Internal.NumATRs = 1
	cleaner := newStdLostTransactionCleaner(config)
	cleaner.locations = map[TransactionLostATRLocation]chan struct{}{
		{
			BucketName: "default",
		}: make(chan struct{}),
	}
	cleaner.uuid = clientUUID

	cleaner.process(agent, "", "", "", func(err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("process operation failed: %v", err)
			}
		})
	})
	s.Wait(0)

	// Ensure that this cleaner has added itself to the client record
	ops := []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "records.clients." + clientUUID,
		},
	}

	h.PushOp(agent.LookupIn(LookupInOptions{
		Key: clientRecordKey,
		Ops: ops,
	}, func(res *LookupInResult, err error) {
		h.Wrap(func() {
			if err != nil {
				h.Fatalf("Lookup operation failed: %v", err)
			}
			if res.Ops[0].Err != nil {
				h.Fatalf("Lookup operation 0 should not have failed, was: %v", res.Ops[0].Err)
			}
		})
	}))
	h.Wait(0)

	suite.Require().Nil(transactions.Close())
	cleaner.Close()

	// Ensure that the cleaner has removed itself from the client record.
	ops = []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "records",
		},
	}

	h.PushOp(agent.LookupIn(LookupInOptions{
		Key: clientRecordKey,
		Ops: ops,
	}, func(res *LookupInResult, err error) {
		h.Wrap(func() {
			if err != nil {
				h.Fatalf("Lookup operation failed: %v", err)
			}
			if res.Ops[0].Err != nil {
				h.Fatalf("Lookup operation 0 failed: %v", res.Ops[0].Err)
			}
			var resultingClients jsonClientRecords
			if err := json.Unmarshal(res.Ops[0].Value, &resultingClients); err != nil {
				h.Fatalf("Unmarshal operation failed: %v", err)
			}
			if len(resultingClients.Clients) != 0 {
				h.Fatalf("Client records should have been empty: %v", resultingClients)
			}
		})
	}))
	h.Wait(0)
}

func (suite *StandardTestSuite) TestLostCleanupCleansUpExpiredClients() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)

	agent, s := suite.GetAgentAndTxnHarness()
	h := suite.GetHarness()

	// Create an expired client record that will be cleaned up
	expiredClientID := uuid.New().String()
	newClient := jsonClientRecords{
		Clients: map[string]jsonClientRecord{
			expiredClientID: {
				HeartbeatMS: "0x000056a9039a4416",
				ExpiresMS:   1,
			},
		},
	}

	b, err := json.Marshal(newClient)
	suite.Require().Nil(err)

	h.PushOp(agent.MutateIn(MutateInOptions{
		Key: clientRecordKey,
		Ops: []SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagMkDirP,
				Path:  "records",
				Value: b,
			},
		},
		Flags: memd.SubdocDocFlagMkDoc,
	}, func(result *MutateInResult, err error) {
		h.Wrap(func() {
			if err != nil {
				s.Fatalf("MutateIn operation failed: %v", err)
			}
		})
	}))
	h.Wait(0)

	clientUUID := uuid.New().String()
	config := &TransactionsConfig{}
	config.DurabilityLevel = TransactionDurabilityLevelNone
	config.BucketAgentProvider = func(bucketName string) (*Agent, string, error) {
		// We can always return just this one agent as we only actually
		// use a single bucket for this entire test.
		return agent, "", nil
	}
	config.CleanupWindow = 1 * time.Second
	config.ExpirationTime = 500 * time.Millisecond
	config.KeyValueTimeout = 2500 * time.Millisecond
	config.Internal.Hooks = nil
	config.Internal.CleanUpHooks = &TransactionDefaultCleanupHooks{}
	config.Internal.ClientRecordHooks = &TransactionDefaultClientRecordHooks{}
	config.Internal.NumATRs = 1024
	cleaner := newStdLostTransactionCleaner(config)

	cleaner.ProcessClient(agent, "", "", "", clientUUID, func(details *TransactionClientRecordDetails, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("ProcessClient operation failed: %v", err)
			}
		})
	})
	s.Wait(0)

	ops := []SubDocOp{
		{
			Op:    memd.SubDocOpGet,
			Flags: memd.SubdocFlagXattrPath,
			Path:  "records",
		},
	}

	h.PushOp(agent.LookupIn(LookupInOptions{
		Key: clientRecordKey,
		Ops: ops,
	}, func(res *LookupInResult, err error) {
		h.Wrap(func() {
			if err != nil {
				h.Fatalf("Lookup operation failed: %v", err)
			}
			if res.Ops[0].Err != nil {
				h.Fatalf("Lookup operation 0 failed: %v", err)
			}

			var resultingClients jsonClientRecords
			if err := json.Unmarshal(res.Ops[0].Value, &resultingClients); err != nil {
				h.Fatalf("Unmarshal failed: %v", err)
			}
			if len(resultingClients.Clients) != 1 {
				h.Fatalf("Expected client records to have 1 client: %v", resultingClients)
			}
			if _, ok := resultingClients.Clients[expiredClientID]; ok {
				h.Fatalf("Expected client records not contain old client id %s: %v", expiredClientID, resultingClients)
			}
		})
	}))
	h.Wait(0)
}

func (suite *StandardTestSuite) TestCustomATRLocationAutomaticallyAddedToCleanup() {
	suite.EnsureSupportsFeature(TestFeatureTransactions)

	agent, _ := suite.GetAgentAndTxnHarness()

	loc := TransactionATRLocation{
		Agent:          agent,
		ScopeName:      suite.ScopeName,
		CollectionName: suite.CollectionName,
	}

	cfg := &TransactionsConfig{
		DurabilityLevel: TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime:      500 * time.Millisecond,
		KeyValueTimeout:     2500 * time.Millisecond,
		CleanupLostAttempts: true,
		CustomATRLocation:   loc,
		CleanupWatchATRs:    true,
	}

	transactions, err := InitTransactions(cfg)
	if err != nil {
		log.Printf("InitTransactions failed: %+v", err)
		panic(err)
	}
	defer transactions.Close()

	suite.Require().Eventually(func() bool {
		locs := transactions.Internal().CleanupLocations()
		if len(locs) == 0 {
			return false
		}

		cLoc := locs[0]
		return cLoc.BucketName == loc.Agent.BucketName() && cLoc.ScopeName == loc.ScopeName &&
			cLoc.CollectionName == loc.CollectionName
	}, 5*time.Second, 100*time.Millisecond)

}
