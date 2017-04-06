package gocbcore

const (
	goCbCoreVersionStr = "v6-dev"
)

type commandMagic uint8

const (
	reqMagic = commandMagic(0x80)
	resMagic = commandMagic(0x81)
)

// commandCode for memcached packets.
type commandCode uint8

const (
	cmdGet                  = commandCode(0x00)
	cmdSet                  = commandCode(0x01)
	cmdAdd                  = commandCode(0x02)
	cmdReplace              = commandCode(0x03)
	cmdDelete               = commandCode(0x04)
	cmdIncrement            = commandCode(0x05)
	cmdDecrement            = commandCode(0x06)
	cmdAppend               = commandCode(0x0e)
	cmdPrepend              = commandCode(0x0f)
	cmdStat                 = commandCode(0x10)
	cmdTouch                = commandCode(0x1c)
	cmdGAT                  = commandCode(0x1d)
	cmdHello                = commandCode(0x1f)
	cmdSASLListMechs        = commandCode(0x20)
	cmdSASLAuth             = commandCode(0x21)
	cmdSASLStep             = commandCode(0x22)
	cmdGetAllVBSeqnos       = commandCode(0x48)
	cmdDcpOpenConnection    = commandCode(0x50)
	cmdDcpAddStream         = commandCode(0x51)
	cmdDcpCloseStream       = commandCode(0x52)
	cmdDcpStreamReq         = commandCode(0x53)
	cmdDcpGetFailoverLog    = commandCode(0x54)
	cmdDcpStreamEnd         = commandCode(0x55)
	cmdDcpSnapshotMarker    = commandCode(0x56)
	cmdDcpMutation          = commandCode(0x57)
	cmdDcpDeletion          = commandCode(0x58)
	cmdDcpExpiration        = commandCode(0x59)
	cmdDcpFlush             = commandCode(0x5a)
	cmdDcpSetVbucketState   = commandCode(0x5b)
	cmdDcpNoop              = commandCode(0x5c)
	cmdDcpBufferAck         = commandCode(0x5d)
	cmdDcpControl           = commandCode(0x5e)
	cmdGetReplica           = commandCode(0x83)
	cmdSelectBucket         = commandCode(0x89)
	cmdObserveSeqNo         = commandCode(0x91)
	cmdObserve              = commandCode(0x92)
	cmdGetLocked            = commandCode(0x94)
	cmdUnlockKey            = commandCode(0x95)
	cmdSetMeta              = commandCode(0xa2)
	cmdDelMeta              = commandCode(0xa8)
	cmdGetClusterConfig     = commandCode(0xb5)
	cmdGetRandom            = commandCode(0xb6)
	cmdSubDocGet            = commandCode(0xc5)
	cmdSubDocExists         = commandCode(0xc6)
	cmdSubDocDictAdd        = commandCode(0xc7)
	cmdSubDocDictSet        = commandCode(0xc8)
	cmdSubDocDelete         = commandCode(0xc9)
	cmdSubDocReplace        = commandCode(0xca)
	cmdSubDocArrayPushLast  = commandCode(0xcb)
	cmdSubDocArrayPushFirst = commandCode(0xcc)
	cmdSubDocArrayInsert    = commandCode(0xcd)
	cmdSubDocArrayAddUnique = commandCode(0xce)
	cmdSubDocCounter        = commandCode(0xcf)
	cmdSubDocMultiLookup    = commandCode(0xd0)
	cmdSubDocMultiMutation  = commandCode(0xd1)
	cmdGetErrorMap          = commandCode(0xfe)
)

type helloFeature uint16

const (
	featureDatatype   = helloFeature(0x01)
	featureTls        = helloFeature(0x02)
	featureTcpNoDelay = helloFeature(0x03)
	featureSeqNo      = helloFeature(0x04)
	featureTcpDelay   = helloFeature(0x05)
	featureXattr      = helloFeature(0x06)
	featureXerror     = helloFeature(0x07)
)

// Status field for memcached response.
type statusCode uint16

const (
	statusSuccess              = statusCode(0x00)
	statusKeyNotFound          = statusCode(0x01)
	statusKeyExists            = statusCode(0x02)
	statusTooBig               = statusCode(0x03)
	statusInvalidArgs          = statusCode(0x04)
	statusNotStored            = statusCode(0x05)
	statusBadDelta             = statusCode(0x06)
	statusNotMyVBucket         = statusCode(0x07)
	statusNoBucket             = statusCode(0x08)
	statusAuthStale            = statusCode(0x1f)
	statusAuthError            = statusCode(0x20)
	statusAuthContinue         = statusCode(0x21)
	statusRangeError           = statusCode(0x22)
	statusRollback             = statusCode(0x23)
	statusAccessError          = statusCode(0x24)
	statusNotInitialized       = statusCode(0x25)
	statusUnknownCommand       = statusCode(0x81)
	statusOutOfMemory          = statusCode(0x82)
	statusNotSupported         = statusCode(0x83)
	statusInternalError        = statusCode(0x84)
	statusBusy                 = statusCode(0x85)
	statusTmpFail              = statusCode(0x86)
	statusSubDocPathNotFound   = statusCode(0xc0)
	statusSubDocPathMismatch   = statusCode(0xc1)
	statusSubDocPathInvalid    = statusCode(0xc2)
	statusSubDocPathTooBig     = statusCode(0xc3)
	statusSubDocDocTooDeep     = statusCode(0xc4)
	statusSubDocCantInsert     = statusCode(0xc5)
	statusSubDocNotJson        = statusCode(0xc6)
	statusSubDocBadRange       = statusCode(0xc7)
	statusSubDocBadDelta       = statusCode(0xc8)
	statusSubDocPathExists     = statusCode(0xc9)
	statusSubDocValueTooDeep   = statusCode(0xca)
	statusSubDocBadCombo       = statusCode(0xcb)
	statusSubDocBadMulti       = statusCode(0xcc)
	statusSubDocSuccessDeleted = statusCode(0xcd)
)

type streamEndStatus uint32

const (
	streamEndOK           = streamEndStatus(0x00)
	streamEndClosed       = streamEndStatus(0x01)
	streamEndStateChanged = streamEndStatus(0x02)
	streamEndDisconnected = streamEndStatus(0x03)
	streamEndTooSlow      = streamEndStatus(0x04)
)

type bucketType int

const (
	bktTypeInvalid   bucketType = 0
	bktTypeCouchbase            = iota
	bktTypeMemcached            = iota
)

type vbucketState uint32

const (
	vbucketStateActive  = vbucketState(0x01)
	vbucketStateReplica = vbucketState(0x02)
	vbucketStatePending = vbucketState(0x03)
	vbucketStateDead    = vbucketState(0x04)
)

// SetMetaOption represents possible option values for a SetMeta operation.
type SetMetaOption uint32

const (
	// SkipConflictResolution disables conflict resolution for the document.
	SkipConflictResolution = SetMetaOption(0x01)

	// UseLwwConflictResolution switches to Last-Write-Wins conflict resolution
	// for the document.
	UseLwwConflictResolution = SetMetaOption(0x02)

	// RegenerateCas causes the server to invalidate the current CAS value for
	// a document, and to generate a new one.
	RegenerateCas = SetMetaOption(0x04)
)

// KeyState represents the various storage states of a key on the server.
type KeyState uint8

const (
	// KeyStateNotPersisted indicates the key is in memory, but not yet written to disk.
	KeyStateNotPersisted = KeyState(0x00)

	// KeyStatePersisted indicates that the key has been written to disk.
	KeyStatePersisted = KeyState(0x01)

	// KeyStateNotFound indicates that the key is not found in memory or on disk.
	KeyStateNotFound = KeyState(0x80)

	// KeyStateDeleted indicates that the key has been written to disk as deleted.
	KeyStateDeleted = KeyState(0x81)
)

// SubDocOpType specifies the type of a sub-document operation.
type SubDocOpType uint8

const (
	// SubDocOpGet indicates the operation is a sub-document `Get` operation.
	SubDocOpGet = SubDocOpType(cmdSubDocGet)

	// SubDocOpExists indicates the operation is a sub-document `Exists` operation.
	SubDocOpExists = SubDocOpType(cmdSubDocExists)

	// SubDocOpDictAdd indicates the operation is a sub-document `Add` operation.
	SubDocOpDictAdd = SubDocOpType(cmdSubDocDictAdd)

	// SubDocOpDictSet indicates the operation is a sub-document `Set` operation.
	SubDocOpDictSet = SubDocOpType(cmdSubDocDictSet)

	// SubDocOpDelete indicates the operation is a sub-document `Remove` operation.
	SubDocOpDelete = SubDocOpType(cmdSubDocDelete)

	// SubDocOpReplace indicates the operation is a sub-document `Replace` operation.
	SubDocOpReplace = SubDocOpType(cmdSubDocReplace)

	// SubDocOpArrayPushLast indicates the operation is a sub-document `ArrayPushLast` operation.
	SubDocOpArrayPushLast = SubDocOpType(cmdSubDocArrayPushLast)

	// SubDocOpArrayPushFirst indicates the operation is a sub-document `ArrayPushFirst` operation.
	SubDocOpArrayPushFirst = SubDocOpType(cmdSubDocArrayPushFirst)

	// SubDocOpArrayInsert indicates the operation is a sub-document `ArrayInsert` operation.
	SubDocOpArrayInsert = SubDocOpType(cmdSubDocArrayInsert)

	// SubDocOpArrayAddUnique indicates the operation is a sub-document `ArrayAddUnique` operation.
	SubDocOpArrayAddUnique = SubDocOpType(cmdSubDocArrayAddUnique)

	// SubDocOpCounter indicates the operation is a sub-document `Counter` operation.
	SubDocOpCounter = SubDocOpType(cmdSubDocCounter)

	// SubDocOpGetDoc represents a full document retrieval, for use with extended attribute ops.
	SubDocOpGetDoc = SubDocOpType(cmdGet)

	// SubDocOpSetDoc represents a full document set, for use with extended attribute ops.
	SubDocOpSetDoc = SubDocOpType(cmdSet)

	// SubDocOpAddDoc represents a full document add, for use with extended attribute ops.
	SubDocOpAddDoc = SubDocOpType(cmdAdd)
)

// DcpOpenFlag specifies flags for DCP streams configured when the stream is opened.
type DcpOpenFlag uint32

const (
	// DcpOpenFlagProducer indicates this stream wants the other end to be a producer.
	DcpOpenFlagProducer = DcpOpenFlag(0x01)

	// DcpOpenFlagNotifier indicates this stream wants the other end to be a notifier.
	DcpOpenFlagNotifier = DcpOpenFlag(0x02)

	// DcpOpenFlagIncludeXattrs indicates the client wishes to receive extended attributes.
	DcpOpenFlagIncludeXattrs = DcpOpenFlag(0x04)

	// DcpOpenFlagNoValue indicates the client does not wish to receive mutation values.
	DcpOpenFlagNoValue = DcpOpenFlag(0x08)
)

// DatatypeFlag specifies data flags for the value of a document.
type DatatypeFlag uint8

const (
	// DatatypeFlagJson indicates the server believes the value payload to be JSON.
	DatatypeFlagJson = DatatypeFlag(0x01)

	// DatatypeFlagCompressed indicates the value payload is compressed.
	DatatypeFlagCompressed = DatatypeFlag(0x02)

	// DatatypeFlagXattrs indicates the inclusion of xattr data in the value payload.
	DatatypeFlagXattrs = DatatypeFlag(0x04)
)

// SubdocFlag specifies flags for a sub-document operation.
type SubdocFlag uint8

const (
	// SubdocFlagNone indicates no special treatment for this operation.
	SubdocFlagNone = SubdocFlag(0x00)

	// SubdocFlagMkDirP indicates that the path should be created if it does not already exist.
	SubdocFlagMkDirP = SubdocFlag(0x01)

	// SubdocFlagMkDoc indicates that the document should be created if it does not already exist.
	SubdocFlagMkDoc = SubdocFlag(0x02)

	// SubdocFlagXattrPath indicates that the path refers to an Xattr rather than the document body.
	SubdocFlagXattrPath = SubdocFlag(0x04)

	// SubdocFlagAccessDeleted indicates that you wish to receive soft-deleted documents.
	SubdocFlagAccessDeleted = SubdocFlag(0x08)

	// SubdocFlagExpandMacros indicates that the value portion of any sub-document mutations
	// should be expanded if they contain macros such as ${Mutation.CAS}.
	SubdocFlagExpandMacros = SubdocFlag(0x10)
)
