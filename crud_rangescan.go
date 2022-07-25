package gocbcore

import (
	"encoding/base64"
	"strconv"
	"time"
)

// RangeScanCreateOptions encapsulates the parameters for a RangeScanCreate operation.
// Volatile: This API is subject to change at any time.
type RangeScanCreateOptions struct {
	RetryStrategy RetryStrategy

	// Deadline will also be sent as a part of the payload if Snapshot is not nil.
	Deadline time.Time

	CollectionName string
	ScopeName      string

	CollectionID uint32
	// Note: if set then KeysOnly on RangeScanContinueOptions *must* also be set.
	KeysOnly bool
	Range    *RangeScanCreateRangeScanConfig
	Sampling *RangeScanCreateRandomSamplingConfig
	Snapshot *RangeScanCreateSnapshotRequirements

	// Internal: This should never be used and is not supported.
	User string

	TraceContext RequestSpanContext
}

func (opts RangeScanCreateOptions) toRequest() (*rangeScanCreateRequest, error) {
	if opts.Range != nil && opts.Sampling != nil {
		return nil, wrapError(errInvalidArgument, "only one of range and sampling can be set")
	}
	if opts.Range == nil && opts.Sampling == nil {
		return nil, wrapError(errInvalidArgument, "one of range and sampling must set")
	}

	var collection string
	if opts.CollectionID != 0 {
		collection = strconv.FormatUint(uint64(opts.CollectionID), 16)
	}
	createReq := &rangeScanCreateRequest{
		Collection: collection,
		KeyOnly:    opts.KeysOnly,
	}

	if opts.Range != nil {
		if len(opts.Range.Start) > 0 && len(opts.Range.ExclusiveStart) > 0 {
			return nil, wrapError(errInvalidArgument, "only one of start and exclusive start within range can be set")
		}
		if len(opts.Range.End) > 0 && len(opts.Range.ExclusiveEnd) > 0 {
			return nil, wrapError(errInvalidArgument, "only one of end and exclusive end within range can be set")
		}
		if (len(opts.Range.Start) == 0 && len(opts.Range.End) > 0) ||
			(len(opts.Range.Start) > 0 && len(opts.Range.End) == 0) {
			return nil, wrapError(errInvalidArgument, "start and end within range must both be set")
		}
		if (len(opts.Range.ExclusiveStart) == 0 && len(opts.Range.ExclusiveEnd) > 0) ||
			(len(opts.Range.ExclusiveStart) > 0 && len(opts.Range.ExclusiveEnd) == 0) {
			return nil, wrapError(errInvalidArgument, "exclusive start and exclusive end within range must both be set")
		}

		createReq.Range = &rangeScanCreateRange{}
		if len(opts.Range.Start) > 0 {
			createReq.Range.Start = base64.StdEncoding.EncodeToString(opts.Range.Start)
		}
		if len(opts.Range.End) > 0 {
			createReq.Range.End = base64.StdEncoding.EncodeToString(opts.Range.End)
		}
		if len(opts.Range.ExclusiveStart) > 0 {
			createReq.Range.ExclusiveStart = base64.StdEncoding.EncodeToString(opts.Range.ExclusiveStart)
		}
		if len(opts.Range.ExclusiveEnd) > 0 {
			createReq.Range.ExclusiveEnd = base64.StdEncoding.EncodeToString(opts.Range.ExclusiveEnd)
		}
	}

	if opts.Sampling != nil {
		if opts.Sampling.Samples == 0 {
			return nil, wrapError(errInvalidArgument, "samples within sampling must be set")
		}

		createReq.Sampling = &rangeScanCreateSample{
			Seed:    opts.Sampling.Seed,
			Samples: opts.Sampling.Samples,
		}
	}

	if opts.Snapshot != nil {
		if opts.Snapshot.VbUUID == 0 {
			return nil, wrapError(errInvalidArgument, "vbuuid within snapshot must be set")
		}
		if opts.Snapshot.SeqNo == 0 {
			return nil, wrapError(errInvalidArgument, "seqno within snapshot must be set")
		}

		createReq.Snapshot = &rangeScanCreateSnapshot{
			VbUUID:      strconv.FormatUint(uint64(opts.Snapshot.VbUUID), 10),
			SeqNo:       uint64(opts.Snapshot.SeqNo),
			SeqNoExists: opts.Snapshot.SeqNoExists,
		}
		createReq.Snapshot.Timeout = uint64(time.Until(opts.Deadline).Milliseconds())
	}

	return createReq, nil
}

// RangeScanCreateRangeScanConfig is the configuration available for performing a range scan.
type RangeScanCreateRangeScanConfig struct {
	Start          []byte
	End            []byte
	ExclusiveStart []byte
	ExclusiveEnd   []byte
}

// RangeScanCreateRandomSamplingConfig is the configuration available for performing a random sampling.
type RangeScanCreateRandomSamplingConfig struct {
	Seed    uint64
	Samples uint64
}

// RangeScanCreateSnapshotRequirements is the set of requirements that the vbucket snapshot must meet in-order for
// the request to be successful.
type RangeScanCreateSnapshotRequirements struct {
	VbUUID      VbUUID
	SeqNo       SeqNo
	SeqNoExists bool
}

// RangeScanCreateResult encapsulates the result of a RangeScanCreate operation.
// Volatile: This API is subject to change at any time.
type RangeScanCreateResult struct {
	ScanUUUID []byte
	KeysOnly  bool
}

// RangeScanContinueOptions encapsulates the parameters for a RangeScanContinue operation.
// Volatile: This API is subject to change at any time.
type RangeScanContinueOptions struct {
	RetryStrategy RetryStrategy

	// Deadline will also be sent as a part of the payload if not zero.
	Deadline time.Time

	MaxCount uint32
	MaxBytes uint32

	// Internal: This should never be used and is not supported.
	User string

	TraceContext RequestSpanContext
}

// RangeScanItem encapsulates an iterm returned during a range scan.
type RangeScanItem struct {
	Value    []byte
	Key      []byte
	Flags    uint32
	Cas      Cas
	Expiry   uint32
	SeqNo    SeqNo
	Datatype uint8
}

// RangeScanContinueResult encapsulates the result of a RangeScanContinue operation.
// Volatile: This API is subject to change at any time.
type RangeScanContinueResult struct {
	More     bool
	Complete bool
}

// RangeScanCancelOptions encapsulates the parameters for a RangeScanCancel operation.
// Volatile: This API is subject to change at any time.
type RangeScanCancelOptions struct {
	RetryStrategy RetryStrategy
	Deadline      time.Time

	// Internal: This should never be used and is not supported.
	User string

	TraceContext RequestSpanContext
}

// RangeScanCancelResult encapsulates the result of a RangeScanCancel operation.
// Volatile: This API is subject to change at any time.
type RangeScanCancelResult struct{}
