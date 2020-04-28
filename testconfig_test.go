package gocbcore

var globalTestConfig *TestConfig

type TestConfig struct {
	ConnStr        string
	BucketName     string
	MemdBucketName string
	ScopeName      string
	CollectionName string
	Username       string
	Password       string
	ClusterVersion NodeVersion
	FeatureFlags   []TestFeatureFlag
}

var globalDCPTestConfig *DCPTestConfig

type DCPTestConfig struct {
	ConnStr        string
	BucketName     string
	Scope          uint32
	Collections    []uint32
	Username       string
	Password       string
	ClusterVersion NodeVersion
	FeatureFlags   []TestFeatureFlag
	NumMutations   int
	NumDeletions   int
	NumExpirations int
}
