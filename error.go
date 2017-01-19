package gocbcore

import (
	"errors"
	"fmt"
	"strings"
)

// MultiError encapsulates multiple errors that may be returned by one method.
type MultiError struct {
	Errors []error
}

func (e *MultiError) add(err error) {
	if multiErr, ok := err.(*MultiError); ok {
		e.Errors = append(e.Errors, multiErr.Errors...)
	} else {
		e.Errors = append(e.Errors, err)
	}
}

func (e *MultiError) get() error {
	if len(e.Errors) == 0 {
		return nil
	} else if len(e.Errors) == 1 {
		return e.Errors[0]
	} else {
		return e
	}
}

func (e *MultiError) Error() string {
	var errors []string
	for _, err := range e.Errors {
		errors = append(errors, err.Error())
	}
	return strings.Join(errors, ", ")
}

// SubDocMutateError encapsulates errors that occur during sub-document mutations.
type SubDocMutateError struct {
	Err     error
	OpIndex int
}

func (e SubDocMutateError) Error() string {
	return fmt.Sprintf("subdocument mutation %d failed (%s)", e.OpIndex, e.Err.Error())
}

type timeoutError struct {
}

func (e timeoutError) Error() string {
	return "operation has timed out"
}
func (e timeoutError) Timeout() bool {
	return true
}

type networkError struct {
}

func (e networkError) Error() string {
	return "betwork error"
}

// Included for legacy support.
func (e networkError) NetworkError() bool {
	return true
}

type overloadError struct {
}

func (e overloadError) Error() string {
	return "queue overflowed"
}
func (e overloadError) Overload() bool {
	return true
}

type shutdownError struct {
}

func (e shutdownError) Error() string {
	return "connection shut down"
}

// Legacy
func (e shutdownError) ShutdownError() bool {
	return true
}

type memdError struct {
	code statusCode
}

func (e memdError) Error() string {
	switch e.code {
	case statusSuccess:
		return "success"
	case statusKeyNotFound:
		return "key not found"
	case statusKeyExists:
		return "key already exists"
	case statusTooBig:
		return "document value was too large"
	case statusNotStored:
		return "document could not be stored"
	case statusBadDelta:
		return "invalid delta was passed"
	case statusNotMyVBucket:
		return "operation sent to incorrect server"
	case statusNoBucket:
		return "not connected to a bucket"
	case statusAuthStale:
		return "authenication context is stale, try re-authenticating"
	case statusAuthError:
		return "authentication error"
	case statusAuthContinue:
		return "more authentication steps needed"
	case statusRangeError:
		return "requested value is outside range"
	case statusAccessError:
		return "no access"
	case statusNotInitialized:
		return "cluster is being initialized, requests are blocked"
	case statusRollback:
		return "rollback is required"
	case statusUnknownCommand:
		return "unknown command was received"
	case statusOutOfMemory:
		return "server is out of memory"
	case statusNotSupported:
		return "server does not support this command"
	case statusInternalError:
		return "internal server error"
	case statusBusy:
		return "server is busy, try again later"
	case statusTmpFail:
		return "temporary failure occurred, try again later"
	case statusSubDocPathNotFound:
		return "sub-document path does not exist"
	case statusSubDocPathMismatch:
		return "type of element in sub-document path conflicts with type in document"
	case statusSubDocPathInvalid:
		return "malformed sub-document path"
	case statusSubDocPathTooBig:
		return "sub-document contains too many components"
	case statusSubDocDocTooDeep:
		return "existing document contains too many levels of nesting"
	case statusSubDocCantInsert:
		return "subdocument operation would invalidate the JSON"
	case statusSubDocNotJson:
		return "existing document is not valid JSON"
	case statusSubDocBadRange:
		return "existing numeric value is too large"
	case statusSubDocBadDelta:
		return "numeric operation would yield a number that is too large, or " +
			"a zero delta was specified"
	case statusSubDocPathExists:
		return "given path already exists in the document"
	case statusSubDocValueTooDeep:
		return "value is too deep to insert"
	case statusSubDocBadCombo:
		return "incorrectly matched subdocument operation types"
	case statusSubDocBadMulti:
		return "could not execute one or more multi lookups or mutations"
	default:
		return fmt.Sprintf("an unknown error occurred (%d)", e.code)
	}
}
func (e memdError) Temporary() bool {
	return e.code == statusOutOfMemory || e.code == statusTmpFail || e.code == statusBusy
}

/* Legacy MemdError Handlers */
func (e memdError) Success() bool {
	return e.code == statusSuccess
}
func (e memdError) KeyNotFound() bool {
	return e.code == statusKeyNotFound
}
func (e memdError) KeyExists() bool {
	return e.code == statusKeyExists
}
func (e memdError) AuthStale() bool {
	return e.code == statusAuthStale
}
func (e memdError) AuthError() bool {
	return e.code == statusAuthError
}
func (e memdError) AuthContinue() bool {
	return e.code == statusAuthContinue
}
func (e memdError) ValueTooBig() bool {
	return e.code == statusTooBig
}
func (e memdError) NotStored() bool {
	return e.code == statusNotStored
}
func (e memdError) BadDelta() bool {
	return e.code == statusBadDelta
}
func (e memdError) NotMyVBucket() bool {
	return e.code == statusNotMyVBucket
}
func (e memdError) NoBucket() bool {
	return e.code == statusNoBucket
}
func (e memdError) RangeError() bool {
	return e.code == statusRangeError
}
func (e memdError) AccessError() bool {
	return e.code == statusAccessError
}
func (e memdError) NotIntializedError() bool {
	return e.code == statusNotInitialized
}
func (e memdError) Rollback() bool {
	return e.code == statusRollback
}
func (e memdError) UnknownCommandError() bool {
	return e.code == statusUnknownCommand
}
func (e memdError) NotSupportedError() bool {
	return e.code == statusNotSupported
}
func (e memdError) InternalError() bool {
	return e.code == statusInternalError
}
func (e memdError) BusyError() bool {
	return e.code == statusBusy
}

type streamEndError struct {
	code streamEndStatus
}

func (e streamEndError) Error() string {
	switch e.code {
	case streamEndOK:
		return "success"
	case streamEndClosed:
		return "stream closed"
	case streamEndStateChanged:
		return "state changed"
	case streamEndDisconnected:
		return "disconnected"
	case streamEndTooSlow:
		return "too slow"
	default:
		return fmt.Sprintf("stream closed for unknown reason (%d)", e.code)
	}
}

func (e streamEndError) Success() bool {
	return e.code == streamEndOK
}
func (e streamEndError) Closed() bool {
	return e.code == streamEndClosed
}
func (e streamEndError) StateChanged() bool {
	return e.code == streamEndStateChanged
}
func (e streamEndError) Disconnected() bool {
	return e.code == streamEndDisconnected
}
func (e streamEndError) TooSlow() bool {
	return e.code == streamEndTooSlow
}

var (
	// ErrDispatchFail occurs when the request router fails to dispatch an operation
	ErrDispatchFail = errors.New("failed to dispatch operation")

	// ErrBadHosts occurs when the list of hosts specified cannot be contacted.
	ErrBadHosts = errors.New("failed to connect to any of the specified hosts")

	// ErrProtocol occurs when the server responds with unexpected or unparseable data.
	ErrProtocol = errors.New("failed to parse server response")

	// ErrNoReplicas occurs when no replicas respond in time
	ErrNoReplicas = errors.New("no replicas responded in time")

	// ErrNoServer occurs when no server is available to service a keys vbucket.
	ErrNoServer = errors.New("no server available for this vbucket")

	// ErrInvalidServer occurs when an explicit, but invalid server index is specified.
	ErrInvalidServer = errors.New("specific server index is invalid")

	// ErrInvalidVBucket occurs when an explicit, but invalid vbucket index is specified.
	ErrInvalidVBucket = errors.New("specific vbucket index is invalid")

	// ErrInvalidReplica occurs when an explicit, but invalid replica index is specified.
	ErrInvalidReplica = errors.New("specific server index is invalid")

	// ErrInvalidCert occurs when a certificate that is not useable is passed to an Agent.
	ErrInvalidCert = errors.New("certificate is invalid")

	// ErrShutdown occurs when operations are performed on a previously closed Agent.
	ErrShutdown = &shutdownError{}

	// ErrOverload occurs when too many operations are dispatched and all queues are full.
	ErrOverload = &overloadError{}

	// ErrNetwork occurs when network failures prevent an operation from succeeding.
	ErrNetwork = &networkError{}

	// ErrTimeout occurs when an operation does not receive a response in a timely manner.
	ErrTimeout = &timeoutError{}

	// ErrStreamClosed occurs when a DCP stream is closed gracefully.
	ErrStreamClosed = &streamEndError{streamEndClosed}

	// ErrStreamStateChanged occurs when a DCP stream is interrupted by failover.
	ErrStreamStateChanged = &streamEndError{streamEndStateChanged}

	// ErrStreamDisconnected occurs when a DCP stream is disconnected.
	ErrStreamDisconnected = &streamEndError{streamEndDisconnected}

	// ErrStreamTooSlow occurs when a DCP stream is cancelled due to the application
	// not keeping up with the rate of flow of DCP events sent by the server.
	ErrStreamTooSlow = &streamEndError{streamEndTooSlow}

	// ErrKeyNotFound occurs when an operation is performed on a key that does not exist.
	ErrKeyNotFound = &memdError{statusKeyNotFound}

	// ErrKeyExists occurs when an operation is performed on a key that could not be found.
	ErrKeyExists = &memdError{statusKeyExists}

	// ErrTooBig occurs when an operation attempts to store more data in a single document
	// than the server is capable of storing (by default, this is a 20MB limit).
	ErrTooBig = &memdError{statusTooBig}

	// ErrInvalidArgs occurs when the server receives invalid arguments for an operation.
	ErrInvalidArgs = &memdError{statusInvalidArgs}

	// ErrNotStored occurs when the server fails to store a key.
	ErrNotStored = &memdError{statusNotStored}

	// ErrBadDelta occurs when an invalid delta value is specified to a counter operation.
	ErrBadDelta = &memdError{statusBadDelta}

	// ErrNotMyVBucket occurs when an operation is dispatched to a server which is
	// non-authoritative for a specific vbucket.
	ErrNotMyVBucket = &memdError{statusNotMyVBucket}

	// ErrNoBucket occurs when no bucket was selected on a connection.
	ErrNoBucket = &memdError{statusNoBucket}

	// ErrAuthStale occurs when authentication credentials have become invalidated.
	ErrAuthStale = &memdError{statusAuthStale}

	// ErrAuthError occurs when the authentication information provided was not valid.
	ErrAuthError = &memdError{statusAuthError}

	// ErrAuthContinue occurs in multi-step authentication when more authentication
	// work needs to be performed in order to complete the authentication process.
	ErrAuthContinue = &memdError{statusAuthContinue}

	// ErrRangeError occurs when the range specified to the server is not valid.
	ErrRangeError = &memdError{statusRangeError}

	// ErrRollback occurs when a DCP stream fails to open due to a rollback having
	// previously occurred since the last time the stream was opened.
	ErrRollback = &memdError{statusRollback}

	// ErrAccessError occurs when an access error occurs.
	ErrAccessError = &memdError{statusAccessError}

	// ErrNotInitialized is sent by servers which are still initializing, and are not
	// yet ready to accept operations on behalf of a particular bucket.
	ErrNotInitialized = &memdError{statusNotInitialized}

	// ErrUnknownCommand occurs when an unknown operation is sent to a server.
	ErrUnknownCommand = &memdError{statusUnknownCommand}

	// ErrOutOfMemory occurs when the server cannot service a request due to memory
	// limitations.
	ErrOutOfMemory = &memdError{statusOutOfMemory}

	// ErrNotSupported occurs when an operation is understood by the server, but that
	// operation is not supported on this server (occurs for a variety of reasons).
	ErrNotSupported = &memdError{statusNotSupported}

	// ErrInternalError occurs when internal errors prevent the server from processing
	// your request.
	ErrInternalError = &memdError{statusInternalError}

	// ErrBusy occurs when the server is too busy to process your request right away.
	// Attempting the operation at a later time will likely succeed.
	ErrBusy = &memdError{statusBusy}

	// ErrTmpFail occurs when a temporary failure is preventing the server from
	// processing your request.
	ErrTmpFail = &memdError{statusTmpFail}

	// ErrSubDocPathNotFound occurs when a sub-document operation targets a path
	// which does not exist in the specifie document.
	ErrSubDocPathNotFound = &memdError{statusSubDocPathNotFound}

	// ErrSubDocPathMismatch occurs when a sub-document operation specifies a path
	// which does not match the document structure (field access on an array).
	ErrSubDocPathMismatch = &memdError{statusSubDocPathMismatch}

	// ErrSubDocPathInvalid occurs when a sub-document path could not be parsed.
	ErrSubDocPathInvalid = &memdError{statusSubDocPathInvalid}

	// ErrSubDocPathTooBig occurs when a sub-document path is too big.
	ErrSubDocPathTooBig = &memdError{statusSubDocPathTooBig}

	// ErrSubDocDocTooDeep occurs when an operation would cause a document to be
	// nested beyond the depth limits allowed by the sub-document specification.
	ErrSubDocDocTooDeep = &memdError{statusSubDocDocTooDeep}

	// ErrSubDocCantInsert occurs when a sub-document operation could not insert.
	ErrSubDocCantInsert = &memdError{statusSubDocCantInsert}

	// ErrSubDocNotJson occurs when a sub-document operation is performed on a
	// document which is not JSON.
	ErrSubDocNotJson = &memdError{statusSubDocNotJson}

	// ErrSubDocBadRange occurs when a sub-document operation is performed with
	// a bad range.
	ErrSubDocBadRange = &memdError{statusSubDocBadRange}

	// ErrSubDocBadDelta occurs when a sub-document counter operation is performed
	// and the specified delta is not valid.
	ErrSubDocBadDelta = &memdError{statusSubDocBadDelta}

	// ErrSubDocPathExists occurs when a sub-document operation expects a path not
	// to exists, but the path was found in the document.
	ErrSubDocPathExists = &memdError{statusSubDocPathExists}

	// ErrSubDocValueTooDeep occurs when a sub-document operation specifies a value
	// which is deeper than the depth limits of the sub-document specification.
	ErrSubDocValueTooDeep = &memdError{statusSubDocValueTooDeep}

	// ErrSubDocBadCombo occurs when a multi-operation sub-document operation is
	// performed and operations within the package of ops conflict with each other.
	ErrSubDocBadCombo = &memdError{statusSubDocBadCombo}

	// ErrSubDocBadMulti occurs when a multi-operation sub-document operation is
	// performed and operations within the package of ops conflict with each other.
	ErrSubDocBadMulti = &memdError{statusSubDocBadMulti}
)

func getStreamEndError(code streamEndStatus) error {
	switch code {
	case streamEndOK:
		return nil
	case streamEndClosed:
		return ErrStreamClosed
	case streamEndStateChanged:
		return ErrStreamStateChanged
	case streamEndDisconnected:
		return ErrStreamDisconnected
	case streamEndTooSlow:
		return ErrStreamTooSlow
	default:
		return &streamEndError{code}
	}
}

func getMemdError(code statusCode) error {
	switch code {
	case statusSuccess:
		return nil
	case statusKeyNotFound:
		return ErrKeyNotFound
	case statusKeyExists:
		return ErrKeyExists
	case statusTooBig:
		return ErrTooBig
	case statusInvalidArgs:
		return ErrInvalidArgs
	case statusNotStored:
		return ErrNotStored
	case statusBadDelta:
		return ErrBadDelta
	case statusNotMyVBucket:
		return ErrNotMyVBucket
	case statusNoBucket:
		return ErrNoBucket
	case statusAuthStale:
		return ErrAuthStale
	case statusAuthError:
		return ErrAuthError
	case statusAuthContinue:
		return ErrAuthContinue
	case statusRangeError:
		return ErrRangeError
	case statusAccessError:
		return ErrAccessError
	case statusNotInitialized:
		return ErrNotInitialized
	case statusRollback:
		return ErrRollback
	case statusUnknownCommand:
		return ErrUnknownCommand
	case statusOutOfMemory:
		return ErrOutOfMemory
	case statusNotSupported:
		return ErrNotSupported
	case statusInternalError:
		return ErrInternalError
	case statusBusy:
		return ErrBusy
	case statusTmpFail:
		return ErrTmpFail
	case statusSubDocPathNotFound:
		return ErrSubDocPathNotFound
	case statusSubDocPathMismatch:
		return ErrSubDocPathMismatch
	case statusSubDocPathInvalid:
		return ErrSubDocPathInvalid
	case statusSubDocPathTooBig:
		return ErrSubDocPathTooBig
	case statusSubDocDocTooDeep:
		return ErrSubDocDocTooDeep
	case statusSubDocCantInsert:
		return ErrSubDocCantInsert
	case statusSubDocNotJson:
		return ErrSubDocNotJson
	case statusSubDocBadRange:
		return ErrSubDocBadRange
	case statusSubDocBadDelta:
		return ErrSubDocBadDelta
	case statusSubDocPathExists:
		return ErrSubDocPathExists
	case statusSubDocValueTooDeep:
		return ErrSubDocValueTooDeep
	case statusSubDocBadCombo:
		return ErrSubDocBadCombo
	case statusSubDocBadMulti:
		return ErrSubDocBadMulti
	default:
		return &memdError{code}
	}
}
