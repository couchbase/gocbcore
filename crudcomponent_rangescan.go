package gocbcore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/snappy"

	"github.com/couchbase/gocbcore/v10/memd"
)

type rangeScanCreateRequest struct {
	Collection string                   `json:"collection,omitempty"`
	KeyOnly    bool                     `json:"key_only,omitempty"`
	Range      *rangeScanCreateRange    `json:"range,omitempty"`
	Sampling   *rangeScanCreateSample   `json:"sampling,omitempty"`
	Snapshot   *rangeScanCreateSnapshot `json:"snapshot_requirements,omitempty"`
}

type rangeScanCreateRange struct {
	Start          string `json:"start,omitempty"`
	End            string `json:"end,omitempty"`
	ExclusiveStart string `json:"excl_start,omitempty"`
	ExclusiveEnd   string `json:"excl_end,omitempty"`
}

type rangeScanCreateSample struct {
	Seed    uint64 `json:"seed,omitempty"`
	Samples uint64 `json:"samples"`
}

type rangeScanCreateSnapshot struct {
	VbUUID      string `json:"vb_uuid"`
	SeqNo       uint64 `json:"seqno"`
	SeqNoExists bool   `json:"seqno_exists,omitempty"`
	Timeout     uint64 `json:"timeout_ms,omitempty"`
}

func (crud *crudComponent) RangeScanCreate(vbID uint16, opts RangeScanCreateOptions, cb RangeScanCreateCallback) (PendingOp, error) {
	if crud.featureVerifier.HasBucketCapabilityStatus(BucketCapabilityRangeScan, BucketCapabilityStatusUnsupported) {
		return nil, errFeatureNotAvailable
	}
	tracer := crud.tracer.StartTelemeteryHandler(metricValueServiceKeyValue, "RangeScanCreate", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		res := RangeScanCreateResult{}
		res.ScanUUUID = resp.Value
		res.KeysOnly = opts.KeysOnly

		tracer.Finish()
		cb(&res, nil)
	}

	var userFrame *memd.UserImpersonationFrame
	if len(opts.User) > 0 {
		userFrame = &memd.UserImpersonationFrame{
			User: []byte(opts.User),
		}
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	createReq, err := opts.toRequest()
	if err != nil {
		return nil, err
	}

	value, err := json.Marshal(createReq)
	if err != nil {
		return nil, err
	}

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:                  memd.CmdMagicReq,
			Command:                memd.CmdRangeScanCreate,
			Datatype:               uint8(memd.DatatypeFlagJSON),
			Cas:                    0,
			Extras:                 nil,
			Key:                    nil,
			Value:                  value,
			UserImpersonationFrame: userFrame,
			Vbucket:                vbID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		RetryStrategy:    opts.RetryStrategy,
		ScopeName:        opts.ScopeName,
		CollectionName:   opts.CollectionName,
	}

	op, err := crud.cidMgr.Dispatch(req)
	if err != nil {
		tracer.Finish()
		return nil, err
	}

	if !opts.Deadline.IsZero() {
		start := time.Now()
		req.SetTimer(time.AfterFunc(opts.Deadline.Sub(start), func() {
			connInfo := req.ConnectionInfo()
			count, reasons := req.Retries()
			req.cancelWithCallbackAndFinishTracer(&TimeoutError{
				InnerError:         errUnambiguousTimeout,
				OperationID:        "RangeScanCreate",
				Opaque:             req.Identifier(),
				TimeObserved:       time.Since(start),
				RetryReasons:       reasons,
				RetryAttempts:      count,
				LastDispatchedTo:   connInfo.lastDispatchedTo,
				LastDispatchedFrom: connInfo.lastDispatchedFrom,
				LastConnectionID:   connInfo.lastConnectionID,
			}, tracer)
		}))
	}

	return op, nil
}

func (crud *crudComponent) RangeScanContinue(scanUUID []byte, vbID uint16, opts RangeScanContinueOptions, dataCb RangeScanContinueDataCallback,
	actionCb RangeScanContinueActionCallback) (PendingOp, error) {
	if crud.featureVerifier.HasBucketCapabilityStatus(BucketCapabilityRangeScan, BucketCapabilityStatusUnsupported) {
		return nil, errFeatureNotAvailable
	}
	tracer := crud.tracer.StartTelemeteryHandler(metricValueServiceKeyValue, "RangeScanContinue", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			actionCb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			actionCb(nil, errProtocol)
			return
		}

		keysOnlyFlag := binary.BigEndian.Uint32(resp.Extras[0:])

		items, err := parseRangeScanData(resp.Value, keysOnlyFlag == 0, crud.disableDecompression)
		if err != nil {
			tracer.Finish()
			actionCb(nil, err)
			return
		}

		if len(resp.Value) > 0 {
			dataCb(items)
		}

		res := RangeScanContinueResult{
			More:     resp.Status == memd.StatusRangeScanMore,
			Complete: resp.Status == memd.StatusRangeScanComplete,
		}

		if res.More || res.Complete {

			// This is effectively the same as calling cancelReqTrace, this will set the cmd and net spans to
			// nil on the request - meaning that the internal cancel below will not cause issues when it calls
			// cancelReqTrace.
			stopNetTrace(req, resp, resp.remoteAddr, resp.sourceAddr)
			stopCmdTrace(req)

			// As this is a persistent request, we must manually cancel it to remove
			// it from the pending ops list.
			req.internalCancel(nil)

			tracer.Finish()

			actionCb(&res, nil)
		}
	}

	var userFrame *memd.UserImpersonationFrame
	if len(opts.User) > 0 {
		userFrame = &memd.UserImpersonationFrame{
			User: []byte(opts.User),
		}
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	if len(scanUUID) != 16 {
		return nil, wrapError(errInvalidArgument, fmt.Sprintf("scanUUID must be 16 bytes, was %d", len(scanUUID)))
	}

	var deadlineMs uint32
	if !opts.Deadline.IsZero() {
		deadlineMs = uint32(time.Until(opts.Deadline).Milliseconds())
	}

	extraBuf := make([]byte, 28)
	copy(extraBuf[:16], scanUUID)
	binary.BigEndian.PutUint32(extraBuf[16:], opts.MaxCount)
	binary.BigEndian.PutUint32(extraBuf[20:], deadlineMs)
	binary.BigEndian.PutUint32(extraBuf[24:], opts.MaxBytes)

	// Note that collection and scope aren't used here. That means that on a collection unknown from the server
	// we will not attempt to refresh the CID.
	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:                  memd.CmdMagicReq,
			Command:                memd.CmdRangeScanContinue,
			Cas:                    0,
			Extras:                 extraBuf,
			Key:                    nil,
			Value:                  nil,
			UserImpersonationFrame: userFrame,
			Vbucket:                vbID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		RetryStrategy:    opts.RetryStrategy,
		Persistent:       true,
	}

	op, err := crud.cidMgr.Dispatch(req)
	if err != nil {
		tracer.Finish()
		return nil, err
	}

	if !opts.Deadline.IsZero() {
		start := time.Now()
		req.SetTimer(time.AfterFunc(opts.Deadline.Sub(start), func() {
			connInfo := req.ConnectionInfo()
			count, reasons := req.Retries()
			req.cancelWithCallbackAndFinishTracer(&TimeoutError{
				InnerError:         errUnambiguousTimeout,
				OperationID:        "RangeScanContinue",
				Opaque:             req.Identifier(),
				TimeObserved:       time.Since(start),
				RetryReasons:       reasons,
				RetryAttempts:      count,
				LastDispatchedTo:   connInfo.lastDispatchedTo,
				LastDispatchedFrom: connInfo.lastDispatchedFrom,
				LastConnectionID:   connInfo.lastConnectionID,
			}, tracer)
		}))
	}

	return op, nil
}

func (crud *crudComponent) RangeScanCancel(scanUUID []byte, vbID uint16, opts RangeScanCancelOptions, cb RangeScanCancelCallback) (PendingOp, error) {
	if crud.featureVerifier.HasBucketCapabilityStatus(BucketCapabilityRangeScan, BucketCapabilityStatusUnsupported) {
		return nil, errFeatureNotAvailable
	}
	tracer := crud.tracer.StartTelemeteryHandler(metricValueServiceKeyValue, "RangeScanCancel", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		tracer.Finish()
		cb(&RangeScanCancelResult{}, nil)
	}

	var userFrame *memd.UserImpersonationFrame
	if len(opts.User) > 0 {
		userFrame = &memd.UserImpersonationFrame{
			User: []byte(opts.User),
		}
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = crud.defaultRetryStrategy
	}

	if len(scanUUID) != 16 {
		return nil, wrapError(errInvalidArgument, fmt.Sprintf("scanUUID must be 16 bytes, was %d", len(scanUUID)))
	}

	extraBuf := make([]byte, 16)
	copy(extraBuf[:16], scanUUID)

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:                  memd.CmdMagicReq,
			Command:                memd.CmdRangeScanCancel,
			Cas:                    0,
			Extras:                 extraBuf,
			Key:                    nil,
			Value:                  nil,
			UserImpersonationFrame: userFrame,
			Vbucket:                vbID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		RetryStrategy:    opts.RetryStrategy,
	}

	op, err := crud.cidMgr.Dispatch(req)
	if err != nil {
		tracer.Finish()
		return nil, err
	}

	if !opts.Deadline.IsZero() {
		start := time.Now()
		req.SetTimer(time.AfterFunc(opts.Deadline.Sub(start), func() {
			connInfo := req.ConnectionInfo()
			count, reasons := req.Retries()
			req.cancelWithCallbackAndFinishTracer(&TimeoutError{
				InnerError:         errUnambiguousTimeout,
				OperationID:        "RangeScanCreate",
				Opaque:             req.Identifier(),
				TimeObserved:       time.Since(start),
				RetryReasons:       reasons,
				RetryAttempts:      count,
				LastDispatchedTo:   connInfo.lastDispatchedTo,
				LastDispatchedFrom: connInfo.lastDispatchedFrom,
				LastConnectionID:   connInfo.lastConnectionID,
			}, tracer)
		}))
	}

	return op, nil
}

func parseRangeScanData(data []byte, keysOnly bool, disableDecompression bool) ([]RangeScanItem, error) {
	if keysOnly {
		return parseRangeScanKeys(data), nil
	}

	return parseRangeScanDocs(data, disableDecompression)
}

func parseRangeScanLebEncoded(data []byte) ([]byte, uint64) {
	keyLen, n := binary.Uvarint(data)
	keyLen = keyLen + uint64(n)
	return data[uint64(n):keyLen], keyLen
}

func parseRangeScanKeys(data []byte) []RangeScanItem {
	var keys []RangeScanItem
	var i uint64
	dataLen := uint64(len(data))
	for {
		if i >= dataLen {
			break
		}

		key, n := parseRangeScanLebEncoded(data[i:])
		keys = append(keys, RangeScanItem{
			Key: key,
		})
		i = i + n
	}

	return keys
}

func parseRangeScanItem(data []byte, disableDecompression bool) (RangeScanItem, uint64, error) {
	flags := binary.BigEndian.Uint32(data[0:])
	expiry := binary.BigEndian.Uint32(data[4:])
	seqno := binary.BigEndian.Uint64(data[8:])
	cas := binary.BigEndian.Uint64(data[16:])
	datatype := data[24]
	key, n := parseRangeScanLebEncoded(data[25:])
	value, n2 := parseRangeScanLebEncoded(data[25+n:])

	isCompressed := (datatype & uint8(memd.DatatypeFlagCompressed)) != 0
	if isCompressed && !disableDecompression {
		newValue, err := snappy.Decode(nil, value)
		if err != nil {
			return RangeScanItem{}, 0, nil
		}

		value = newValue
		datatype = datatype & ^uint8(memd.DatatypeFlagCompressed)
	}

	return RangeScanItem{
		Value:    value,
		Key:      key,
		Flags:    flags,
		Cas:      Cas(cas),
		Expiry:   expiry,
		SeqNo:    SeqNo(seqno),
		Datatype: datatype,
	}, 25 + n + n2, nil
}

func parseRangeScanDocs(data []byte, disableDecompression bool) ([]RangeScanItem, error) {
	var items []RangeScanItem
	var i uint64
	dataLen := uint64(len(data))
	for {
		if i >= dataLen {
			break
		}

		item, n, err := parseRangeScanItem(data[i:], disableDecompression)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		i = i + n
	}

	return items, nil
}
