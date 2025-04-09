package gocbcore

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

func (t *transactionAttempt) GetMulti(opts TransactionGetMultiOptions, cb TransactionGetMultiCallback) error {
	return t.getMulti(opts, func(res *TransactionGetMultiResult, err error) {
		if err != nil {
			if !t.ShouldRollback() {
				t.ensureCleanUpRequest()
			}

			cb(nil, err)
			return
		}

		cb(res, nil)
	})
}

// transactionGetMultiSignal is used for control flow during a GetMulti operation.
// See transactionGetMultiManager.handleSignal for how they are handled.
type transactionGetMultiSignal uint8

const (
	// transactionGetMultiSignalContinue indicates that we should continue to the next step of the GetMulti algorithm.
	transactionGetMultiSignalContinue transactionGetMultiSignal = iota

	// transactionGetMultiSignalRetry indicates that we should retry starting from the beginning of the GetMulti
	// algorithm after a backoff, without resetting the operation state.
	transactionGetMultiSignalRetry

	// transactionGetMultiSignalRetry indicates that the GetMulti operation is complete, and we should return the
	// results.
	transactionGetMultiSignalCompleted

	// transactionGetMultiSignalBoundExceeded indicates that a time bound for a particular step of the algorithm was
	// exceeded.
	transactionGetMultiSignalBoundExceeded

	// transactionGetMultiSignalResetAndRetry indicates that we should retry starting from the beginning of the GetMulti
	// algorithm after a backoff, and reset all mutable operation state.
	transactionGetMultiSignalResetAndRetry
)

func (t *transactionAttempt) getMulti(
	opts TransactionGetMultiOptions,
	cb func(*TransactionGetMultiResult, error),
) error {
	t.logger.logInfof(t.id, "Performing getMulti for %d documents. non fatal enabled: %t", len(opts.Specs), t.enableNonFatalGets)

	t.beginOpAndLock(func(unlock func(), endOp func()) {
		endAndCb := func(result *TransactionGetMultiResult, err error) {
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

		mgr := newTransactionGetMultiManager(t, opts)
		mgr.doGetMulti(func(err error) {
			if err != nil {
				endAndCb(nil, err)
				return
			}
			endAndCb(mgr.createResult(), nil)
		})
	})

	return nil
}

const (
	getMultiParallelismLimit = 100
)

type transactionGetMultiSpecResult struct {
	Spec     TransactionGetMultiSpec
	Internal *transactionGetDoc
}

func (r *transactionGetMultiSpecResult) docExists() bool {
	return r.Internal != nil
}

type transactionGetMultiMode uint8

const (
	transactionGetMultiModePrioritiseLatency transactionGetMultiMode = iota
	transactionGetMultiModeDisableReadSkewDetection
	transactionGetMultiModePrioritiseReadSkewDetection
)

type transactionGetMultiPhase uint8

const (
	transactionGetMultiPhaseFirstDocFetch transactionGetMultiPhase = iota
	transactionGetMultiPhaseSubsequentToFirstDocFetch
	transactionGetMultiPhaseDiscoveredDocsInT1
	transactionGetMultiPhaseResolvingT1ATREntryMissing
)

type transactionGetMultiManager struct {
	txnAttempt    *transactionAttempt
	oboUser       string
	forceNonFatal bool
	serverGroup   string
	originalSpecs []TransactionGetMultiSpec

	backoffCalculator BackoffCalculator
	retryAttempts     uint32
	deadline          time.Time
	toFetch           []TransactionGetMultiSpec
	alreadyFetched    []*transactionGetMultiSpecResult
	mode              transactionGetMultiMode
	phase             transactionGetMultiPhase
}

func newTransactionGetMultiManager(t *transactionAttempt, opts TransactionGetMultiOptions) *transactionGetMultiManager {
	for idx := range opts.Specs {
		// This will allow us to return the results in the order that the specs were given.
		opts.Specs[idx].originalIdx = idx
	}

	mgr := &transactionGetMultiManager{
		txnAttempt:        t,
		oboUser:           opts.OboUser,
		forceNonFatal:     t.enableNonFatalGets,
		serverGroup:       opts.ServerGroup,
		originalSpecs:     opts.Specs,
		toFetch:           opts.Specs,
		backoffCalculator: ExponentialBackoff(0, 0, 0),
		deadline:          time.Now().Add(t.keyValueTimeout),
	}

	switch opts.Mode {
	case TransactionGetMultiModeUnset, TransactionGetMultiModePrioritiseLatency:
		mgr.mode = transactionGetMultiModePrioritiseLatency
	case TransactionGetMultiModeDisableReadSkewDetection:
		mgr.mode = transactionGetMultiModeDisableReadSkewDetection
	case TransactionGetMultiModePrioritiseReadSkewDetection:
		mgr.mode = transactionGetMultiModePrioritiseReadSkewDetection
	}

	copy(mgr.toFetch, opts.Specs)
	return mgr
}

func (mgr *transactionGetMultiManager) reset() {
	copy(mgr.toFetch, mgr.originalSpecs)
	mgr.alreadyFetched = nil
	if mgr.phase != transactionGetMultiPhaseFirstDocFetch {
		mgr.phase = transactionGetMultiPhaseSubsequentToFirstDocFetch
	}
}

func (mgr *transactionGetMultiManager) createResult() *TransactionGetMultiResult {
	res := &TransactionGetMultiResult{
		Values: make(map[int][]byte),
	}
	for _, specRes := range mgr.alreadyFetched {
		if specRes.docExists() {
			res.Values[specRes.Spec.originalIdx] = specRes.Internal.Body
		}
	}
	return res
}

func (mgr *transactionGetMultiManager) doGetMulti(cb func(error)) {
	mgr.txnAttempt.checkExpiredAtomic(hookGetMulti, []byte{}, false, func(cerr *classifiedError) {
		if cerr != nil {
			cb(mgr.txnAttempt.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionExpired,
			}))
			return
		}
		mgr.fetchMultipleDocs(func(signal transactionGetMultiSignal, err error) {
			if err != nil {
				cb(err)
				return
			}
			mgr.handleSignal(signal, cb, func() {
				mgr.documentDisambiguation(func(signal transactionGetMultiSignal, err error) {
					if err != nil {
						cb(err)
						return
					}
					mgr.handleSignal(signal, cb, func() {
						mgr.readSkewResolution(func(signal transactionGetMultiSignal, err error) {
							if err != nil {
								cb(err)
								return
							}
							mgr.handleSignal(signal, cb, func() {
								cb(mgr.txnAttempt.operationFailed(operationFailedDef{
									Cerr:              classifyError(wrapError(ErrIllegalState, "unexpected continue signal from read skew resolution")),
									CanStillCommit:    mgr.forceNonFatal,
									ShouldNotRollback: false,
									ShouldNotRetry:    false,
								}))
							})
						})
					})
				})
			})
		})
	})
}

func (mgr *transactionGetMultiManager) fetchIndividualDoc(spec TransactionGetMultiSpec, cb func(transactionGetMultiSignal, *transactionGetMultiSpecResult, error)) {
	// This is the backoff calculator used in the individual fetch which is different from the overall GetMulti backoff calculator.
	backoff := ExponentialBackoff(0, 0, 0)

	mgr.fetchIndividualDocWithBackoffCalculator(spec, backoff, 0, cb)
}

func (mgr *transactionGetMultiManager) handleFetchIndividualDocError(err error, spec TransactionGetMultiSpec, backoff BackoffCalculator, retryAttempts uint32, cb func(transactionGetMultiSignal, *transactionGetMultiSpecResult, error)) {
	var e *TransactionOperationFailedError
	if errors.As(err, &e) {
		cb(0, nil, err)
		return
	}
	if errors.Is(err, ErrDocumentUnretrievable) {
		cb(0, &transactionGetMultiSpecResult{Spec: spec}, nil)
		return
	}
	if errors.Is(err, ErrTimeout) {
		if mgr.mode == transactionGetMultiModePrioritiseReadSkewDetection {
			<-time.After(backoff(retryAttempts))
			retryAttempts++
			mgr.fetchIndividualDocWithBackoffCalculator(spec, backoff, retryAttempts, cb)
			return
		} else {
			cb(transactionGetMultiSignalBoundExceeded, nil, nil)
			return
		}
	}
	classifiedErr := classifyError(err)
	switch classifiedErr.Class {
	case TransactionErrorClassFailDocNotFound:
		cb(0, &transactionGetMultiSpecResult{Spec: spec}, nil)
		return
	case TransactionErrorClassFailTransient:
		<-time.After(backoff(retryAttempts))
		retryAttempts++
		mgr.fetchIndividualDocWithBackoffCalculator(spec, backoff, retryAttempts, cb)
		return
	case TransactionErrorClassFailHard:
		cb(0, nil, mgr.txnAttempt.operationFailed(operationFailedDef{
			Cerr:              classifiedErr,
			CanStillCommit:    mgr.forceNonFatal,
			ShouldNotRollback: true,
			ShouldNotRetry:    true,
		}))
		return
	default:
		cb(0, nil, mgr.txnAttempt.operationFailed(operationFailedDef{
			Cerr:              classifiedErr,
			CanStillCommit:    mgr.forceNonFatal,
			ShouldNotRollback: false,
			ShouldNotRetry:    true,
		}))
		return
	}
}

func (mgr *transactionGetMultiManager) fetchIndividualDocWithBackoffCalculator(spec TransactionGetMultiSpec, backoff BackoffCalculator, retryAttempts uint32, cb func(transactionGetMultiSignal, *transactionGetMultiSpecResult, error)) {
	mgr.txnAttempt.checkExpiredAtomic(hookGetMultiIndividualDocument, spec.Key, false, func(cerr *classifiedError) {
		if cerr != nil {
			cb(0, nil, mgr.txnAttempt.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    mgr.forceNonFatal,
				Reason:            TransactionErrorReasonTransactionExpired,
				ShouldNotRollback: true,
			}))
			return
		}

		if backoff == nil {
			backoff = ExponentialBackoff(0, 0, 0)
		}

		mgr.txnAttempt.fetchDocWithMeta(
			spec.Agent,
			mgr.oboUser,
			spec.ScopeName,
			spec.CollectionName,
			spec.Key,
			mgr.forceNonFatal,
			transactionFetchReplicaOptions{
				serverGroup:  mgr.serverGroup,
				withFallback: true, // For GetMulti we allow fallback to a replica outside the preferred server group.
			},
			mgr.deadline,
			func(doc *transactionGetDoc, err error) {
				if err != nil {
					mgr.handleFetchIndividualDocError(err, spec, backoff, retryAttempts, cb)
					return
				}

				tm := doc.TxnMeta
				if tm == nil {
					cb(transactionGetMultiSignalContinue, &transactionGetMultiSpecResult{
						Spec:     spec,
						Internal: doc,
					}, nil)
					return
				}
				docFc := jsonForwardCompatToForwardCompat(tm.ForwardCompat)

				mgr.txnAttempt.checkForwardCompatability(
					spec.Key,
					spec.Agent.BucketName(),
					spec.ScopeName,
					spec.CollectionName,
					forwardCompatStageGetMultiGets,
					docFc,
					mgr.forceNonFatal,
					func(err *TransactionOperationFailedError) {
						if err != nil {
							mgr.handleFetchIndividualDocError(err, spec, backoff, retryAttempts, cb)
							return
						}

						mgr.txnAttempt.checkForwardCompatability(
							spec.Key,
							spec.Agent.BucketName(),
							spec.ScopeName,
							spec.CollectionName,
							forwardCompatStageGets,
							docFc,
							mgr.forceNonFatal,
							func(err *TransactionOperationFailedError) {
								if err != nil {
									mgr.handleFetchIndividualDocError(err, spec, backoff, retryAttempts, cb)
									return
								}
								cb(transactionGetMultiSignalContinue, &transactionGetMultiSpecResult{
									Spec:     spec,
									Internal: doc,
								}, nil)
							})
					})
			})
	})

}

func (mgr *transactionGetMultiManager) fetchMultipleDocs(cb func(transactionGetMultiSignal, error)) {
	mgr.txnAttempt.logger.logInfof(mgr.txnAttempt.id, "GetMulti - fetching multiple docs, toFetch: %d documents", len(mgr.toFetch))
	type fetchResult struct {
		signal  transactionGetMultiSignal
		specRes *transactionGetMultiSpecResult
		err     error
	}
	resCh := make(chan fetchResult, len(mgr.toFetch))

	go func() {
		wg := sync.WaitGroup{}
		var stop atomic.Bool
		var parallelismGuard chan struct{}
		if len(mgr.toFetch) < getMultiParallelismLimit {
			parallelismGuard = make(chan struct{}, len(mgr.toFetch))
		} else {
			parallelismGuard = make(chan struct{}, getMultiParallelismLimit)
		}

		for len(mgr.toFetch) > 0 && !stop.Load() {
			var spec TransactionGetMultiSpec
			spec, mgr.toFetch = mgr.toFetch[0], mgr.toFetch[1:]

			parallelismGuard <- struct{}{}
			wg.Add(1)
			mgr.fetchIndividualDoc(spec, func(signal transactionGetMultiSignal, specRes *transactionGetMultiSpecResult, err error) {
				if err != nil || signal == transactionGetMultiSignalBoundExceeded {
					stop.Store(false)
				}
				resCh <- fetchResult{
					signal:  signal,
					specRes: specRes,
					err:     err,
				}
				wg.Done()
				<-parallelismGuard
			})
		}
		wg.Wait()
		close(resCh)
	}()

	go func() {
		for res := range resCh {
			if res.signal == transactionGetMultiSignalBoundExceeded {
				cb(res.signal, nil)
				return
			}
			if res.err != nil {
				cb(0, res.err)
				return
			}
			mgr.alreadyFetched = append(mgr.alreadyFetched, res.specRes)
		}
		mgr.toFetch = nil

		switch mgr.mode {
		case transactionGetMultiModeDisableReadSkewDetection:
			cb(transactionGetMultiSignalCompleted, nil)
			return
		case transactionGetMultiModePrioritiseLatency:
			mgr.deadline = time.Now().Add(100 * time.Millisecond)
		case transactionGetMultiModePrioritiseReadSkewDetection:
			mgr.deadline = mgr.txnAttempt.expiryTime
		}
		if mgr.phase == transactionGetMultiPhaseFirstDocFetch {
			mgr.phase = transactionGetMultiPhaseSubsequentToFirstDocFetch
		}

		cb(transactionGetMultiSignalContinue, nil)
	}()
}

func (mgr *transactionGetMultiManager) documentDisambiguation(cb func(transactionGetMultiSignal, error)) {
	if len(mgr.alreadyFetched) == 1 {
		mgr.txnAttempt.logger.logDebugf(mgr.txnAttempt.id, "GetMulti - only one document fetched, performing standard MAV read logic.")

		specRes := mgr.alreadyFetched[0]

		if !specRes.docExists() {
			// We could not find the document. No need to perform MAV logic.
			cb(transactionGetMultiSignalCompleted, nil)
			return
		}

		mgr.txnAttempt.performMavLogic(
			specRes.Spec.Agent,
			mgr.oboUser,
			specRes.Spec.ScopeName,
			specRes.Spec.CollectionName,
			specRes.Spec.Key,
			false,
			"",
			mgr.forceNonFatal,
			mgr.serverGroup,
			specRes.Internal,
			func(doc *transactionGetDoc, err error) {
				if err != nil {
					cb(0, err)
					return
				}
				mgr.alreadyFetched[0].Internal = doc
				cb(transactionGetMultiSignalCompleted, nil)
			})
		return
	}

	txnIDs := make(map[string]struct{})

	for _, doc := range mgr.alreadyFetched {
		if !doc.docExists() || doc.Internal.TxnMeta == nil {
			continue
		}
		if txnID := doc.Internal.TxnMeta.ID.Transaction; txnID != "" && txnID != mgr.txnAttempt.transactionID {
			txnIDs[txnID] = struct{}{}
		}
	}

	switch len(txnIDs) {
	case 0:
		// No proof of read skew, nothing we can do – we are done
		cb(transactionGetMultiSignalCompleted, nil)
	case 1:
		// One other transaction is involved, read skew might have happened. Continue to read skew resolution.
		cb(transactionGetMultiSignalContinue, nil)
	default:
		mgr.txnAttempt.logger.logDebugf(mgr.txnAttempt.id, "Documents known to be involved in %d other transactions. Too complex to detect read skew. Resetting & retrying.", len(txnIDs))
		// The situation is too complex for us to resolve
		cb(transactionGetMultiSignalResetAndRetry, nil)
	}
}

func (mgr *transactionGetMultiManager) readSkewResolution(cb func(transactionGetMultiSignal, error)) {
	mgr.txnAttempt.logger.logDebugf(mgr.txnAttempt.id, "GetMulti - read skew resolution, alreadyFetched: %d documents", len(mgr.alreadyFetched))

	if len(mgr.alreadyFetched) < 2 {
		mgr.txnAttempt.logger.logWarnf(mgr.txnAttempt.id, "Unexpected number of documents for read skew resolution: %d. At least 2 expected.", len(mgr.alreadyFetched))
		cb(transactionGetMultiSignalResetAndRetry, nil)
		return
	}
	var otherTxnAttemptID string
	var atr jsonTxnXattrATR
	for _, doc := range mgr.alreadyFetched {
		if !doc.docExists() {
			continue
		}
		if attemptID := doc.Internal.TxnMeta.ID.Attempt; attemptID != "" && attemptID != mgr.txnAttempt.id {
			if otherTxnAttemptID == "" {
				otherTxnAttemptID = attemptID
				atr = doc.Internal.TxnMeta.ATR
			} else if attemptID != otherTxnAttemptID {
				mgr.txnAttempt.logger.logWarnf(mgr.txnAttempt.id, "Documents involved in more than one other transaction during read skew resolution. This should not be the case.")
				cb(transactionGetMultiSignalResetAndRetry, nil)
				return
			}
		}
	}
	if otherTxnAttemptID == "" {
		mgr.txnAttempt.logger.logWarnf(mgr.txnAttempt.id, "No other transaction found during read skew resolution. This should not be the case.")
		cb(transactionGetMultiSignalResetAndRetry, nil)
	}

	mgr.txnAttempt.getTxnState(
		atr.BucketName,
		atr.ScopeName,
		atr.CollectionName,
		atr.DocID,
		otherTxnAttemptID,
		func(attempt *jsonAtrAttempt, expiry time.Time, classifiedErr *classifiedError) {
			if classifiedErr != nil {
				if errors.Is(classifiedErr.Source, ErrTimeout) {
					if mgr.mode == transactionGetMultiModePrioritiseReadSkewDetection {
						cb(transactionGetMultiSignalRetry, nil)
						return
					} else {
						cb(transactionGetMultiSignalBoundExceeded, nil)
						return
					}
				} else {
					cb(0, mgr.txnAttempt.operationFailed(operationFailedDef{
						CanStillCommit:    mgr.forceNonFatal,
						Cerr:              classifiedErr,
						ShouldNotRollback: false,
						ShouldNotRetry:    false,
					}))
					return
				}
			}

			fetchedInT1, fetchedNotInT1 := mgr.partitionAlreadyFetched(otherTxnAttemptID)

			if attempt == nil {
				// T1's ATR entry is missing.
				if mgr.phase == transactionGetMultiPhaseResolvingT1ATREntryMissing {
					if len(fetchedInT1) > 0 {
						cb(transactionGetMultiSignalCompleted, nil)
						return
					} else {
						cb(transactionGetMultiSignalResetAndRetry, nil)
						return
					}
				} else {
					mgr.toFetch = make([]TransactionGetMultiSpec, len(fetchedInT1))
					for idx, specRes := range fetchedInT1 {
						mgr.toFetch[idx] = specRes.Spec
					}
					mgr.alreadyFetched = fetchedNotInT1
					mgr.phase = transactionGetMultiPhaseResolvingT1ATREntryMissing
					cb(transactionGetMultiSignalRetry, nil)
					return
				}
			}

			state := jsonAtrState(attempt.State)
			if state == jsonAtrStatePending || state == jsonAtrStateAborted {
				cb(transactionGetMultiSignalCompleted, nil)
				return
			}

			if state == jsonAtrStateCommitted && mgr.phase == transactionGetMultiPhaseSubsequentToFirstDocFetch {
				wereInT1 := mgr.documentsAlreadyFetchedWereInTransaction(attempt)
				if len(wereInT1) == 0 {
					for _, specRes := range fetchedInT1 {
						// We are working with pointers, so modifying the elements of fetchedInT1 will modify the elements of mgr.alreadyFetched
						specRes.Internal.Body = specRes.Internal.TxnMeta.Operation.Staged
					}
					cb(transactionGetMultiSignalCompleted, nil)
					return
				} else {
					mgr.phase = transactionGetMultiPhaseDiscoveredDocsInT1

					mgr.toFetch = make([]TransactionGetMultiSpec, len(wereInT1))
					for idx, specRes := range wereInT1 {
						mgr.toFetch[idx] = specRes.Spec
					}

					newAlreadyFetched := make([]*transactionGetMultiSpecResult, len(mgr.alreadyFetched)-len(wereInT1))
					newIdx := 0
					for _, doc := range mgr.alreadyFetched {
						if doc.docExists() && !slices.Contains(wereInT1, doc) {
							newAlreadyFetched[newIdx] = doc
							newIdx++
						}
					}
					mgr.alreadyFetched = newAlreadyFetched

					cb(transactionGetMultiSignalRetry, nil)
					return
				}
			}

			if state == jsonAtrStateCommitted && mgr.phase == transactionGetMultiPhaseDiscoveredDocsInT1 {
				for _, specRes := range fetchedInT1 {
					// We are working with pointers, so modifying the elements of fetchedInT1 will modify the elements of mgr.alreadyFetched
					specRes.Internal.Body = specRes.Internal.TxnMeta.Operation.Staged
				}
				cb(transactionGetMultiSignalCompleted, nil)
				return
			}

			cb(transactionGetMultiSignalResetAndRetry, nil)
		},
	)
}

func (mgr *transactionGetMultiManager) partitionAlreadyFetched(attemptID string) ([]*transactionGetMultiSpecResult, []*transactionGetMultiSpecResult) {
	var fetchedInT1, fetchedNotInT1 []*transactionGetMultiSpecResult
	for _, doc := range mgr.alreadyFetched {
		if !doc.docExists() {
			continue
		}
		if doc.Internal.TxnMeta.ID.Attempt == attemptID {
			fetchedInT1 = append(fetchedInT1, doc)
		} else {
			fetchedNotInT1 = append(fetchedNotInT1, doc)
		}
	}
	return fetchedInT1, fetchedNotInT1
}

func (mgr *transactionGetMultiManager) documentsAlreadyFetchedWereInTransaction(attempt *jsonAtrAttempt) []*transactionGetMultiSpecResult {
	var wereInT1 []*transactionGetMultiSpecResult

	for _, doc := range mgr.alreadyFetched {
		if !doc.docExists() {
			continue
		}

		atrMutation := jsonAtrMutation{
			BucketName:     doc.Spec.Agent.BucketName(),
			ScopeName:      doc.Spec.ScopeName,
			CollectionName: doc.Spec.CollectionName,
			DocID:          string(doc.Spec.Key),
		}

		if doc.Internal.TxnMeta == nil && (slices.Contains(attempt.Inserts, atrMutation) || slices.Contains(attempt.Replaces, atrMutation) || slices.Contains(attempt.Removes, atrMutation)) {
			// When we fetched the document it had no transactional metadata, but now we discover it was involved in T1.
			wereInT1 = append(wereInT1, doc)
		}
	}

	return wereInT1
}

func (mgr *transactionGetMultiManager) handleSignal(signal transactionGetMultiSignal, cb func(error), continueAction func()) {
	switch signal {
	case transactionGetMultiSignalContinue:
		continueAction()
		return
	case transactionGetMultiSignalRetry:
		<-time.After(mgr.backoffCalculator(mgr.retryAttempts))
		mgr.retryAttempts++
		mgr.doGetMulti(cb)
		return
	case transactionGetMultiSignalResetAndRetry:
		mgr.reset()
		<-time.After(mgr.backoffCalculator(mgr.retryAttempts))
		mgr.retryAttempts++
		mgr.doGetMulti(cb)
		return
	case transactionGetMultiSignalCompleted:
		cb(nil)
		return
	case transactionGetMultiSignalBoundExceeded:
		if len(mgr.alreadyFetched) == len(mgr.originalSpecs) {
			// We have something for every document. Return this to the user
			cb(nil)
			return
		} else {
			// We've run out of time and don't have something for every doc
			cb(mgr.txnAttempt.operationFailed(operationFailedDef{
				Cerr:              classifyError(errors.New("bound for operation exceeded")),
				CanStillCommit:    mgr.forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			}))
			return
		}
	}
}
