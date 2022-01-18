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

func testBlkGet(txn *Transaction, opts TransactionGetOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Get(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkInsert(txn *Transaction, opts TransactionInsertOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Insert(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkReplace(txn *Transaction, opts TransactionReplaceOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Replace(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkRemove(txn *Transaction, opts TransactionRemoveOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Remove(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkCommit(txn *Transaction) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Commit(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkRollback(txn *Transaction) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Rollback(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkSerialize(txn *Transaction) (txnBytesOut []byte, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.SerializeAttempt(func(txnBytes []byte, err error) {
		txnBytesOut = txnBytes
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}
