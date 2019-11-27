package gocbcore

import (
	"fmt"
)

func getKvStatusCodeText(code StatusCode) string {
	switch code {
	case StatusSuccess:
		return "success"
	case StatusKeyNotFound:
		return "key not found"
	case StatusKeyExists:
		return "key already exists, if a cas was provided the key exists with a different cas"
	case StatusTooBig:
		return "document value was too large"
	case StatusInvalidArgs:
		return "invalid arguments"
	case StatusNotStored:
		return "document could not be stored"
	case StatusBadDelta:
		return "invalid delta was passed"
	case StatusNotMyVBucket:
		return "operation sent to incorrect server"
	case StatusNoBucket:
		return "not connected to a bucket"
	case StatusAuthStale:
		return "authentication context is stale, try re-authenticating"
	case StatusAuthError:
		return "authentication error"
	case StatusAuthContinue:
		return "more authentication steps needed"
	case StatusRangeError:
		return "requested value is outside range"
	case StatusAccessError:
		return "no access"
	case StatusNotInitialized:
		return "cluster is being initialized, requests are blocked"
	case StatusRollback:
		return "rollback is required"
	case StatusUnknownCommand:
		return "unknown command was received"
	case StatusOutOfMemory:
		return "server is out of memory"
	case StatusNotSupported:
		return "server does not support this command"
	case StatusInternalError:
		return "internal server error"
	case StatusBusy:
		return "server is busy, try again later"
	case StatusTmpFail:
		return "temporary failure occurred, try again later"
	case StatusCollectionUnknown:
		return "the requested collection cannot be found"
	case StatusScopeUnknown:
		return "the requested scope cannot be found."
	case StatusDurabilityInvalidLevel:
		return "invalid request, invalid durability level specified."
	case StatusDurabilityImpossible:
		return "the requested durability requirements are impossible."
	case StatusSyncWriteInProgress:
		return "key already has syncwrite pending."
	case StatusSyncWriteAmbiguous:
		return "the syncwrite request did not complete in time."
	case StatusSubDocPathNotFound:
		return "sub-document path does not exist"
	case StatusSubDocPathMismatch:
		return "type of element in sub-document path conflicts with type in document"
	case StatusSubDocPathInvalid:
		return "malformed sub-document path"
	case StatusSubDocPathTooBig:
		return "sub-document contains too many components"
	case StatusSubDocDocTooDeep:
		return "existing document contains too many levels of nesting"
	case StatusSubDocCantInsert:
		return "subdocument operation would invalidate the JSON"
	case StatusSubDocNotJSON:
		return "existing document is not valid JSON"
	case StatusSubDocBadRange:
		return "existing numeric value is too large"
	case StatusSubDocBadDelta:
		return "numeric operation would yield a number that is too large, or " +
			"a zero delta was specified"
	case StatusSubDocPathExists:
		return "given path already exists in the document"
	case StatusSubDocValueTooDeep:
		return "value is too deep to insert"
	case StatusSubDocBadCombo:
		return "incorrectly matched subdocument operation types"
	case StatusSubDocBadMulti:
		return "could not execute one or more multi lookups or mutations"
	case StatusSubDocSuccessDeleted:
		return "document is soft-deleted"
	case StatusSubDocXattrInvalidFlagCombo:
		return "invalid xattr flag combination"
	case StatusSubDocXattrInvalidKeyCombo:
		return "invalid xattr key combination"
	case StatusSubDocXattrUnknownMacro:
		return "unknown xattr macro"
	case StatusSubDocXattrUnknownVAttr:
		return "unknown xattr virtual attribute"
	case StatusSubDocXattrCannotModifyVAttr:
		return "cannot modify virtual attributes"
	case StatusSubDocMultiPathFailureDeleted:
		return "sub-document multi-path error"
	default:
		return fmt.Sprintf("unknown kv status code (%d)", code)
	}
}

func getKvStreamEndStatusText(code StreamEndStatus) string {
	switch code {
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
	case streamEndFilterEmpty:
		return "filter empty"
	default:
		return fmt.Sprintf("unknown stream close reason (%d)", code)
	}
}
