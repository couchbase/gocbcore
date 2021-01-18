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
	MockPath       string
}

func (tc *TestConfig) Clone() *TestConfig {
	return &TestConfig{
		ConnStr:        tc.ConnStr,
		BucketName:     tc.BucketName,
		MemdBucketName: tc.MemdBucketName,
		ScopeName:      tc.ScopeName,
		CollectionName: tc.CollectionName,
		Authenticator:  tc.Authenticator,
		CAProvider:     tc.CAProvider,
		ClusterVersion: tc.ClusterVersion,
		FeatureFlags:   tc.FeatureFlags,
		MockPath:       tc.MockPath,
	}
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
	NumScopes      int
	NumCollections int
}
