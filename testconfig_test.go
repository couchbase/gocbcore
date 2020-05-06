package gocbcore

import "crypto/x509"

var globalTestConfig *TestConfig

type TestConfig struct {
	ConnStr        string
	BucketName     string
	MemdBucketName string
	ScopeName      string
	CollectionName string
	Authenticator  AuthProvider
	CAProvider     func() *x509.CertPool
	ClusterVersion *NodeVersion
	FeatureFlags   []TestFeatureFlag
}

var globalDCPTestConfig *DCPTestConfig

type DCPTestConfig struct {
	ConnStr        string
	BucketName     string
	Scope          uint32
	Collections    []uint32
	Authenticator  AuthProvider
	CAProvider     func() *x509.CertPool
	ClusterVersion *NodeVersion
	FeatureFlags   []TestFeatureFlag
	NumMutations   int
	NumDeletions   int
	NumExpirations int
}
