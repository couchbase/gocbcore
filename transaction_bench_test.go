package gocbcore

import (
	"fmt"
	"github.com/google/uuid"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkTransactionInsertGet(b *testing.B) {
	suite := GetBenchSuite()
	suite.EnsureSupportsFeature(TestFeatureTransactions, b)
	b.ReportAllocs()

	agent := suite.GetAgent()

	key := []byte(uuid.New().String())
	val := []byte(`{"name":"mike"}`)

	cfg := &TransactionsConfig{
		DurabilityLevel: TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime: 60 * time.Second,
	}
	var i uint32
	suite.RunParallelTxn(b, cfg, func(txn *Transaction, cb func(error)) error {
		keyNum := atomic.AddUint32(&i, 1)
		key := []byte(fmt.Sprintf("%s-%d", key, keyNum))

		return txn.Insert(TransactionInsertOptions{
			Agent:          agent,
			ScopeName:      suite.ScopeName,
			CollectionName: suite.CollectionName,
			Key:            key,
			Value:          val,
		}, func(result *TransactionGetResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			err = txn.Get(TransactionGetOptions{
				Agent:          agent,
				ScopeName:      suite.ScopeName,
				CollectionName: suite.CollectionName,
				Key:            key,
			}, func(result *TransactionGetResult, err error) {
				if err != nil {
					cb(err)
					return
				}

				cb(nil)
			})
			if err != nil {
				cb(err)
				return
			}
		})
	})
}
