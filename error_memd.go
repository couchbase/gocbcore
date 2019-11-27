package gocbcore

import (
	"errors"
	"log"
)

var statusCodeErrorMap = make(map[StatusCode]error)

func makeKvStatusError(code StatusCode) error {
	err := errors.New(getKvStatusCodeText((code)))
	if statusCodeErrorMap[code] != nil {
		log.Fatal("error handling setup failure")
	}
	statusCodeErrorMap[code] = err
	return err
}

func getKvStatusCodeError(code StatusCode) error {
	if err := statusCodeErrorMap[code]; err != nil {
		return err
	}
	return errors.New(getKvStatusCodeText((code)))
}

var (
	// ErrMemdKeyNotFound occurs when an operation is performed on a key that does not exist.
	ErrMemdKeyNotFound = makeKvStatusError(StatusKeyNotFound)

	// ErrMemdKeyExists occurs when an operation is performed on a key that could not be found.
	ErrMemdKeyExists = makeKvStatusError(StatusKeyExists)

	// ErrMemdTooBig occurs when an operation attempts to store more data in a single document
	// than the server is capable of storing (by default, this is a 20MB limit).
	ErrMemdTooBig = makeKvStatusError(StatusTooBig)

	// ErrMemdInvalidArgs occurs when the server receives invalid arguments for an operation.
	ErrMemdInvalidArgs = makeKvStatusError(StatusInvalidArgs)

	// ErrMemdNotStored occurs when the server fails to store a key.
	ErrMemdNotStored = makeKvStatusError(StatusNotStored)

	// ErrMemdBadDelta occurs when an invalid delta value is specified to a counter operation.
	ErrMemdBadDelta = makeKvStatusError(StatusBadDelta)

	// ErrMemdNotMyVBucket occurs when an operation is dispatched to a server which is
	// non-authoritative for a specific vbucket.
	ErrMemdNotMyVBucket = makeKvStatusError(StatusNotMyVBucket)

	// ErrMemdNoBucket occurs when no bucket was selected on a connection.
	ErrMemdNoBucket = makeKvStatusError(StatusNoBucket)

	// ErrMemdLocked occurs when a document is already locked.
	ErrMemdLocked = makeKvStatusError(StatusLocked)

	// ErrMemdAuthStale occurs when authentication credentials have become invalidated.
	ErrMemdAuthStale = makeKvStatusError(StatusAuthStale)

	// ErrMemdAuthError occurs when the authentication information provided was not valid.
	ErrMemdAuthError = makeKvStatusError(StatusAuthError)

	// ErrMemdAuthContinue occurs in multi-step authentication when more authentication
	// work needs to be performed in order to complete the authentication process.
	ErrMemdAuthContinue = makeKvStatusError(StatusAuthContinue)

	// ErrMemdRangeError occurs when the range specified to the server is not valid.
	ErrMemdRangeError = makeKvStatusError(StatusRangeError)

	// ErrMemdRollback occurs when a DCP stream fails to open due to a rollback having
	// previously occurred since the last time the stream was opened.
	ErrMemdRollback = makeKvStatusError(StatusRollback)

	// ErrMemdAccessError occurs when an access error occurs.
	ErrMemdAccessError = makeKvStatusError(StatusAccessError)

	// ErrMemdNotInitialized is sent by servers which are still initializing, and are not
	// yet ready to accept operations on behalf of a particular bucket.
	ErrMemdNotInitialized = makeKvStatusError(StatusNotInitialized)

	// ErrMemdUnknownCommand occurs when an unknown operation is sent to a server.
	ErrMemdUnknownCommand = makeKvStatusError(StatusUnknownCommand)

	// ErrMemdOutOfMemory occurs when the server cannot service a request due to memory
	// limitations.
	ErrMemdOutOfMemory = makeKvStatusError(StatusOutOfMemory)

	// ErrMemdNotSupported occurs when an operation is understood by the server, but that
	// operation is not supported on this server (occurs for a variety of reasons).
	ErrMemdNotSupported = makeKvStatusError(StatusNotSupported)

	// ErrMemdInternalError occurs when internal errors prevent the server from processing
	// your request.
	ErrMemdInternalError = makeKvStatusError(StatusInternalError)

	// ErrMemdBusy occurs when the server is too busy to process your request right away.
	// Attempting the operation at a later time will likely succeed.
	ErrMemdBusy = makeKvStatusError(StatusBusy)

	// ErrMemdTmpFail occurs when a temporary failure is preventing the server from
	// processing your request.
	ErrMemdTmpFail = makeKvStatusError(StatusTmpFail)

	// ErrMemdCollectionNotFound occurs when a Collection cannot be found.
	ErrMemdCollectionNotFound = makeKvStatusError(StatusCollectionUnknown)

	// ErrMemdScopeNotFound occurs when a Collection cannot be found.
	ErrMemdScopeNotFound = makeKvStatusError(StatusScopeUnknown)

	// ErrMemdDurabilityInvalidLevel occurs when an invalid durability level was requested.
	ErrMemdDurabilityInvalidLevel = makeKvStatusError(StatusDurabilityInvalidLevel)

	// ErrMemdDurabilityImpossible occurs when a request is performed with impossible
	//  durability level requirements.
	ErrMemdDurabilityImpossible = makeKvStatusError(StatusDurabilityImpossible)

	// ErrMemdSyncWriteInProgess occurs when an attempt is made to write to a key that has
	//  a SyncWrite pending.
	ErrMemdSyncWriteInProgess = makeKvStatusError(StatusSyncWriteInProgress)

	// ErrMemdSyncWriteAmbiguous occurs when an SyncWrite does not complete in the specified
	// time and the result is ambiguous.
	ErrMemdSyncWriteAmbiguous = makeKvStatusError(StatusSyncWriteAmbiguous)

	// ErrMemdSyncWriteReCommitInProgress occurs when an SyncWrite is being recommitted.
	ErrMemdSyncWriteReCommitInProgress = makeKvStatusError(StatusSyncWriteReCommitInProgress)

	// ErrMemdSubDocPathNotFound occurs when a sub-document operation targets a path
	// which does not exist in the specifie document.
	ErrMemdSubDocPathNotFound = makeKvStatusError(StatusSubDocPathNotFound)

	// ErrMemdSubDocPathMismatch occurs when a sub-document operation specifies a path
	// which does not match the document structure (field access on an array).
	ErrMemdSubDocPathMismatch = makeKvStatusError(StatusSubDocPathMismatch)

	// ErrMemdSubDocPathInvalid occurs when a sub-document path could not be parsed.
	ErrMemdSubDocPathInvalid = makeKvStatusError(StatusSubDocPathInvalid)

	// ErrMemdSubDocPathTooBig occurs when a sub-document path is too big.
	ErrMemdSubDocPathTooBig = makeKvStatusError(StatusSubDocPathTooBig)

	// ErrMemdSubDocDocTooDeep occurs when an operation would cause a document to be
	// nested beyond the depth limits allowed by the sub-document specification.
	ErrMemdSubDocDocTooDeep = makeKvStatusError(StatusSubDocDocTooDeep)

	// ErrMemdSubDocCantInsert occurs when a sub-document operation could not insert.
	ErrMemdSubDocCantInsert = makeKvStatusError(StatusSubDocCantInsert)

	// ErrMemdSubDocNotJSON occurs when a sub-document operation is performed on a
	// document which is not JSON.
	ErrMemdSubDocNotJSON = makeKvStatusError(StatusSubDocNotJSON)

	// ErrMemdSubDocBadRange occurs when a sub-document operation is performed with
	// a bad range.
	ErrMemdSubDocBadRange = makeKvStatusError(StatusSubDocBadRange)

	// ErrMemdSubDocBadDelta occurs when a sub-document counter operation is performed
	// and the specified delta is not valid.
	ErrMemdSubDocBadDelta = makeKvStatusError(StatusSubDocBadDelta)

	// ErrMemdSubDocPathExists occurs when a sub-document operation expects a path not
	// to exists, but the path was found in the document.
	ErrMemdSubDocPathExists = makeKvStatusError(StatusSubDocPathExists)

	// ErrMemdSubDocValueTooDeep occurs when a sub-document operation specifies a value
	// which is deeper than the depth limits of the sub-document specification.
	ErrMemdSubDocValueTooDeep = makeKvStatusError(StatusSubDocValueTooDeep)

	// ErrMemdSubDocBadCombo occurs when a multi-operation sub-document operation is
	// performed and operations within the package of ops conflict with each other.
	ErrMemdSubDocBadCombo = makeKvStatusError(StatusSubDocBadCombo)

	// ErrMemdSubDocBadMulti occurs when a multi-operation sub-document operation is
	// performed and operations within the package of ops conflict with each other.
	ErrMemdSubDocBadMulti = makeKvStatusError(StatusSubDocBadMulti)

	// ErrMemdSubDocSuccessDeleted occurs when a multi-operation sub-document operation
	// is performed on a soft-deleted document.
	ErrMemdSubDocSuccessDeleted = makeKvStatusError(StatusSubDocSuccessDeleted)

	// ErrMemdSubDocXattrInvalidFlagCombo occurs when an invalid set of
	// extended-attribute flags is passed to a sub-document operation.
	ErrMemdSubDocXattrInvalidFlagCombo = makeKvStatusError(StatusSubDocXattrInvalidFlagCombo)

	// ErrMemdSubDocXattrInvalidKeyCombo occurs when an invalid set of key operations
	// are specified for a extended-attribute sub-document operation.
	ErrMemdSubDocXattrInvalidKeyCombo = makeKvStatusError(StatusSubDocXattrInvalidKeyCombo)

	// ErrMemdSubDocXattrUnknownMacro occurs when an invalid macro value is specified.
	ErrMemdSubDocXattrUnknownMacro = makeKvStatusError(StatusSubDocXattrUnknownMacro)

	// ErrMemdSubDocXattrUnknownVAttr occurs when an invalid virtual attribute is specified.
	ErrMemdSubDocXattrUnknownVAttr = makeKvStatusError(StatusSubDocXattrUnknownVAttr)

	// ErrMemdSubDocXattrCannotModifyVAttr occurs when a mutation is attempted upon
	// a virtual attribute (which are immutable by definition).
	ErrMemdSubDocXattrCannotModifyVAttr = makeKvStatusError(StatusSubDocXattrCannotModifyVAttr)

	// ErrMemdSubDocMultiPathFailureDeleted occurs when a Multi Path Failure occurs on
	// a soft-deleted document.
	ErrMemdSubDocMultiPathFailureDeleted = makeKvStatusError(StatusSubDocMultiPathFailureDeleted)
)
