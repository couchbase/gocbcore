package jcbmock

// BucketType represents the type of bucket to use.
type BucketType int

const (
	// BCouchbase represents to use a couchbase bucket.
	BCouchbase BucketType = 0

	// BMemcached represents to use a memcached bucket.
	BMemcached = iota
)

// CmdCode represents a command code to send to Couchbase Mock.
type CmdCode string

const (
	// CFailover represents a command code to send FAILOVER to the mock.
	CFailover CmdCode = "FAILOVER"

	// CRespawn represents a command code to send RESPAWN to the mock.
	CRespawn = "RESPAWN"

	// CHiccup represents a command code to send HICCUP to the mock.
	CHiccup = "HICCUP"

	// CTruncate represents a command code to send TRUNCATE to the mock.
	CTruncate = "TRUNCATE"

	// CMockinfo represents a command code to send MOCKINFO to the mock.
	CMockinfo = "MOCKINFO"

	// CPersist represents a command code to send PERSIST to the mock.
	CPersist = "PERSIST"

	// CCache represents a command code to send CACHE to the mock.
	CCache = "CACHE"

	// CUnpersist represents a command code to send UNPERSIST to the mock.
	CUnpersist = "UNPERSIST"

	// CUncache represents a command code to send UNCACHE to the mock.
	CUncache = "UNCACHE"

	// CEndure represents a command code to send ENDURE to the mock.
	CEndure = "ENDURE"

	// CPurge represents a command code to send PURGE to the mock.
	CPurge = "PURGE"

	// CKeyinfo represents a command code to send KEYINFO to the mock.
	CKeyinfo = "KEYINFO"

	// CTimeTravel represents a command code to send TIME_TRAVEL to the mock.
	CTimeTravel = "TIME_TRAVEL"

	// CHelp represents a command code to send HELP to the mock.
	CHelp = "HELP"

	// COpFail represents a command code to send OPFAIL to the mock.
	COpFail = "OPFAIL"

	// CSetCCCP represents a command code to SET_CCCP the mock.
	CSetCCCP = "SET_CCCP"

	// CGetMcPorts represents a command code to GET_MCPORTS the mock.
	CGetMcPorts = "GET_MCPORTS"

	// CRegenVBCoords represents a command code to REGEN_VBCOORDS the mock.
	CRegenVBCoords = "REGEN_VBCOORDS"

	// CResetQueryState represents a command code to RESET_QUERYSTATE the mock.
	CResetQueryState = "RESET_QUERYSTATE"

	// CStartCmdLog represents a command code to START_CMDLOG the mock.
	CStartCmdLog = "START_CMDLOG"

	// CStopCmdLog represents a command code to STOP_CMDLOG the mock.
	CStopCmdLog = "STOP_CMDLOG"

	// CGetCmdLog represents a command code to GET_CMDLOG the mock.
	CGetCmdLog = "GET_CMDLOG"

	// CStartRetryVerify represents a command code to START_RETRY_VERIFY the mock.
	CStartRetryVerify = "START_RETRY_VERIFY"

	// CCheckRetryVerify represents a command code to CHECK_RETRY_VERIFY the mock.
	CCheckRetryVerify = "CHECK_RETRY_VERIFY"

	// CSetEnhancedErrors represents a command code to SET_ENHANCED_ERRORS the mock.
	CSetEnhancedErrors = "SET_ENHANCED_ERRORS"

	// CSetCompression represents a command code to SET_COMPRESSION the mock.
	CSetCompression = "SET_COMPRESSION"

	// CSetSASLMechanisms represents a command code to SET_SASL_MECHANISMS the mock.
	CSetSASLMechanisms = "SET_SASL_MECHANISMS"
)
