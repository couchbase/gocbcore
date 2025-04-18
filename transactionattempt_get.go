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
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (t *transactionAttempt) Get(opts TransactionGetOptions, cb TransactionGetCallback) error {
	return t.get(opts, func(res *TransactionGetResult, err error) {
		if err != nil {
			t.logger.logInfof(t.id, "Get failed %s", err)
			if !t.ShouldRollback() {
				t.ensureCleanUpRequest()
			}

			cb(nil, err)
			return
		}

		cb(res, nil)
	})
}

func (t *transactionAttempt) get(
	opts TransactionGetOptions,
	cb func(*TransactionGetResult, error),
) error {
	forceNonFatal := t.enableNonFatalGets

	t.logger.logInfof(t.id, "Performing get for %s non fatal enabled: %t", newLoggableDocKey(
		opts.Agent.BucketName(),
		opts.ScopeName,
		opts.CollectionName,
		opts.Key,
	), forceNonFatal)

	t.beginOpAndLock(func(unlock func(), endOp func()) {
		endAndCb := func(result *TransactionGetResult, err error) {
			endOp()
			cb(result, err)
		}

		err := t.checkCanPerformOpLocked()
		if err != nil {
			unlock()
			endAndCb(nil, err)
			return
		}

		unlock()

		t.checkExpiredAtomic(hookGet, opts.Key, false, func(cerr *classifiedError) {
			if cerr != nil {
				endAndCb(nil, t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    true,
					ShouldNotRollback: false,
					Reason:            TransactionErrorReasonTransactionExpired,
				}))
				return
			}

			t.mavRead(opts.Agent, opts.OboUser, opts.ScopeName, opts.CollectionName, opts.Key, opts.NoRYOW,
				"", forceNonFatal, opts.ServerGroup, func(doc *transactionGetDoc, err error) {
					if err != nil {
						endAndCb(nil, err)
						return
					}

					t.hooks.AfterGetComplete(opts.Key, func(err error) {
						if err != nil {
							endAndCb(nil, t.operationFailed(operationFailedDef{
								Cerr:              classifyHookError(err),
								CanStillCommit:    forceNonFatal,
								ShouldNotRetry:    true,
								ShouldNotRollback: true,
								Reason:            TransactionErrorReasonTransactionFailed,
							}))
							return
						}

						var docMeta *TransactionMutableItemMeta
						if doc.TxnMeta != nil {
							docMeta = &TransactionMutableItemMeta{
								TransactionID: doc.TxnMeta.ID.Transaction,
								AttemptID:     doc.TxnMeta.ID.Attempt,
								OperationID:   doc.TxnMeta.ID.Operation,
								ATR: TransactionMutableItemMetaATR{
									BucketName:     doc.TxnMeta.ATR.BucketName,
									ScopeName:      doc.TxnMeta.ATR.ScopeName,
									CollectionName: doc.TxnMeta.ATR.CollectionName,
									DocID:          doc.TxnMeta.ATR.DocID,
								},
								ForwardCompat: jsonForwardCompatToForwardCompat(doc.TxnMeta.ForwardCompat),
							}
						}

						endAndCb(&TransactionGetResult{
							agent:          opts.Agent,
							oboUser:        opts.OboUser,
							scopeName:      opts.ScopeName,
							collectionName: opts.CollectionName,
							key:            opts.Key,
							Value:          doc.Body,
							Cas:            doc.Cas,
							Meta:           docMeta,
						}, nil)
					})
				})
		})
	})

	return nil
}

func (t *transactionAttempt) mavRead(
	agent *Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	disableRYOW bool,
	resolvingATREntry string,
	forceNonFatal bool,
	serverGroup string,
	cb func(*transactionGetDoc, error),
) {
	t.fetchDocWithMeta(
		agent,
		oboUser,
		scopeName,
		collectionName,
		key,
		forceNonFatal,
		transactionFetchReplicaOptions{
			serverGroup:  serverGroup,
			withFallback: false, // We do not fall back to non-preferred server group reads for individual gets.
		},
		time.Time{},
		func(doc *transactionGetDoc, err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.performMavLogic(agent, oboUser, scopeName, collectionName, key, disableRYOW, resolvingATREntry, forceNonFatal, serverGroup, doc, cb)
		})
}

func (t *transactionAttempt) performMavLogic(
	agent *Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	disableRYOW bool,
	resolvingATREntry string,
	forceNonFatal bool,
	serverGroup string,
	doc *transactionGetDoc,
	cb func(*transactionGetDoc, error),
) {
	if disableRYOW {
		if doc.TxnMeta != nil && doc.TxnMeta.ID.Attempt == t.id {
			t.logger.logInfof(t.id, "Disable RYOW set and tnx meta is not nil, resetting meta to nil")
			// This is going to be a RYOW, we can just clear the TxnMeta which
			// will cause us to fall into the block below.
			doc.TxnMeta = nil
		}
	}

	// Doc not involved in another transaction.
	if doc.TxnMeta == nil {
		if doc.Deleted {
			cb(nil, wrapError(ErrDocumentNotFound, "doc was a tombstone"))
			return
		}

		t.logger.logInfof(t.id, "Txn meta is nil, returning result")
		cb(doc, nil)
		return
	}

	if doc.TxnMeta.ID.Attempt == t.id {
		switch doc.TxnMeta.Operation.Type {
		case jsonMutationInsert:
			t.logger.logInfof(t.id, "Doc already in txn as insert, using staged value")
			cb(&transactionGetDoc{
				Body: doc.TxnMeta.Operation.Staged,
				Cas:  doc.Cas,
			}, nil)
		case jsonMutationReplace:
			t.logger.logInfof(t.id, "Doc already in txn as replace, using staged value")
			cb(&transactionGetDoc{
				Body: doc.TxnMeta.Operation.Staged,
				Cas:  doc.Cas,
			}, nil)
		case jsonMutationRemove:
			cb(nil, wrapError(ErrDocumentNotFound, "doc was a staged remove"))
		default:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrIllegalState, "unexpected staged mutation type")),
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			}))
		}
		return
	}

	if doc.TxnMeta.ID.Attempt == resolvingATREntry {
		if doc.Deleted {
			cb(nil, wrapError(ErrDocumentNotFound, "doc was a staged tombstone during resolution"))
			return
		}

		t.logger.logInfof(t.id, "Completed ATR resolution")
		cb(&transactionGetDoc{
			Body: doc.Body,
			Cas:  doc.Cas,
		}, nil)
		return
	}

	docFc := jsonForwardCompatToForwardCompat(doc.TxnMeta.ForwardCompat)

	t.checkForwardCompatability(
		key,
		agent.BucketName(),
		scopeName,
		collectionName,
		forwardCompatStageGets,
		docFc,
		forceNonFatal,
		func(err *TransactionOperationFailedError) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.getTxnState(
				doc.TxnMeta.ATR.BucketName,
				doc.TxnMeta.ATR.ScopeName,
				doc.TxnMeta.ATR.CollectionName,
				doc.TxnMeta.ATR.DocID,
				doc.TxnMeta.ID.Attempt,
				func(attempt *jsonAtrAttempt, expiry time.Time, err *classifiedError) {
					if err != nil {
						cb(nil, t.operationFailed(operationFailedDef{
							Cerr:              err,
							CanStillCommit:    forceNonFatal,
							ShouldNotRetry:    false,
							ShouldNotRollback: false,
							Reason:            TransactionErrorReasonTransactionFailed,
						}))
						return
					}

					if attempt == nil {
						t.logger.logInfof(t.id, "ATR entry missing, rerunning mav read")
						// The ATR entry is missing, it's likely that we just raced the other transaction
						// cleaning up it's documents and then cleaning itself up.  Lets run ATR resolution.
						t.mavRead(agent, oboUser, scopeName, collectionName, key, disableRYOW, doc.TxnMeta.ID.Attempt, forceNonFatal, serverGroup, cb)
						return
					}

					atmptFc := jsonForwardCompatToForwardCompat(attempt.ForwardCompat)
					t.checkForwardCompatability(
						key,
						agent.BucketName(),
						scopeName,
						collectionName,
						forwardCompatStageGetsReadingATR, atmptFc, forceNonFatal, func(err *TransactionOperationFailedError) {
							if err != nil {
								cb(nil, err)
								return
							}

							state := jsonAtrState(attempt.State)
							if state == jsonAtrStateCommitted || state == jsonAtrStateCompleted {
								switch doc.TxnMeta.Operation.Type {
								case jsonMutationInsert:
									t.logger.logInfof(t.id, "Doc already in txn as insert, using staged value")
									cb(&transactionGetDoc{
										Body:    doc.TxnMeta.Operation.Staged,
										Cas:     doc.Cas,
										TxnMeta: doc.TxnMeta,
									}, nil)
								case jsonMutationReplace:
									t.logger.logInfof(t.id, "Doc already in txn as replace, using staged value")
									cb(&transactionGetDoc{
										Body:    doc.TxnMeta.Operation.Staged,
										Cas:     doc.Cas,
										TxnMeta: doc.TxnMeta,
									}, nil)
								case jsonMutationRemove:
									cb(nil, wrapError(ErrDocumentNotFound, "doc was a staged remove"))
								default:
									cb(nil, t.operationFailed(operationFailedDef{
										Cerr: classifyError(
											wrapError(ErrIllegalState, "unexpected staged mutation type")),
										ShouldNotRetry:    false,
										ShouldNotRollback: false,
									}))
								}
								return
							}

							if doc.Deleted {
								cb(nil, wrapError(ErrDocumentNotFound, "doc was a tombstone"))
								return
							}

							cb(&transactionGetDoc{
								Body:    doc.Body,
								Cas:     doc.Cas,
								TxnMeta: doc.TxnMeta,
							}, nil)
						})
				})
		})
}

type transactionFetchReplicaOptions struct {
	serverGroup  string
	withFallback bool
}

func (t *transactionAttempt) fetchDocWithMeta(
	agent *Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	forceNonFatal bool,
	replicaOpts transactionFetchReplicaOptions,
	deadline time.Time,
	cb func(*transactionGetDoc, error),
) {
	ecCb := func(doc *transactionGetDoc, cerr *classifiedError) {
		if cerr == nil {
			cb(doc, nil)
			return
		}

		t.ReportResourceUnitsError(cerr.Source)

		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			cb(nil, wrapError(ErrDocumentNotFound, "doc was not found"))
		case TransactionErrorClassFailTransient:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			}))
		case TransactionErrorClassFailHard:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailed,
			}))
		default:
			if errors.Is(cerr.Source, ErrDocumentUnretrievable) {
				cb(nil, ErrDocumentUnretrievable)
				return
			}

			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			}))
		}

	}

	t.hooks.BeforeDocGet(key, func(err error) {
		if err != nil {
			ecCb(nil, classifyHookError(err))
			return
		}

		if deadline.IsZero() && t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
		}

		handler := func(result *LookupInResult, err error) {
			if err != nil {
				ecCb(nil, classifyError(err))
				return
			}

			t.ReportResourceUnits(result.Internal.ResourceUnits)

			if result.Ops[0].Err != nil {
				ecCb(nil, classifyError(result.Ops[0].Err))
				return
			}

			var meta *transactionDocMeta
			if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
				ecCb(nil, classifyError(err))
				return
			}

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				// Doc is currently in a txn.
				var txnMetaVal jsonTxnXattr
				if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
					ecCb(nil, classifyError(err))
					return
				}

				txnMeta = &txnMetaVal
			}

			var docBody []byte
			if result.Ops[2].Err == nil {
				docBody = result.Ops[2].Value
			}

			ecCb(&transactionGetDoc{
				Body:    docBody,
				TxnMeta: txnMeta,
				DocMeta: meta,
				Cas:     result.Cas,
				Deleted: result.Internal.IsDeleted,
			}, nil)
		}

		if replicaOpts.serverGroup == "" {
			_, err = agent.LookupIn(LookupInOptions{
				ScopeName:      scopeName,
				CollectionName: collectionName,
				Key:            key,
				Ops: []SubDocOp{
					{
						Op:    memd.SubDocOpGet,
						Path:  "$document",
						Flags: memd.SubdocFlagXattrPath,
					},
					{
						Op:    memd.SubDocOpGet,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
					},
					{
						Op:    memd.SubDocOpGetDoc,
						Path:  "",
						Flags: 0,
					},
				},
				Deadline: deadline,
				Flags:    memd.SubdocDocFlagAccessDeleted,
				User:     oboUser,
			}, handler)
			if err != nil {
				ecCb(nil, classifyError(err))
			}
		} else {
			_, err = agent.crud.LookupInServerGroup(replicaOpts.serverGroup, replicaOpts.withFallback, LookupInOptions{
				ScopeName:      scopeName,
				CollectionName: collectionName,
				Key:            key,
				Ops: []SubDocOp{
					{
						Op:    memd.SubDocOpGet,
						Path:  "$document",
						Flags: memd.SubdocFlagXattrPath,
					},
					{
						Op:    memd.SubDocOpGet,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
					},
					{
						Op:    memd.SubDocOpGetDoc,
						Path:  "",
						Flags: 0,
					},
				},
				Deadline: deadline,
				// Flags:    memd.SubdocDocFlagAccessDeleted, See: MB-63241
				User: oboUser,
			}, handler)
			if err != nil {
				ecCb(nil, classifyError(err))
			}
		}
	})
}
