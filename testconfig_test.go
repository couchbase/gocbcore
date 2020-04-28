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
