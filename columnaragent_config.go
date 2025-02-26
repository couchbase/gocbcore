package gocbcore

import (
	"crypto/tls"
	"crypto/x509"
	"time"
)

// ColumnarAgentConfig specifies the configuration options for creation of an ColumnarAgent.
type ColumnarAgentConfig struct {
	UserAgent string

	ConnectTimeout time.Duration

	SeedConfig ColumnarSeedConfig

	SecurityConfig ColumnarSecurityConfig

	ConfigPollerConfig ColumnarConfigPollerConfig

	KVConfig ColumnarKVConfig

	HTTPConfig ColumnarHTTPConfig
}

// ColumnarSeedConfig specifies initial seed configuration options such as addresses.
type ColumnarSeedConfig struct {
	MemdAddrs []string
	SRVRecord *SRVRecord
}

// ColumnarSecurityConfig specifies options for controlling security related
// items such as TLS root certificates and verification skipping.
type ColumnarSecurityConfig struct {
	TLSRootCAProvider func() *x509.CertPool
	CipherSuite       []*tls.CipherSuite

	Auth AuthProvider
}

// ColumnarConfigPollerConfig specifies options for controlling the cluster configuration pollers.
type ColumnarConfigPollerConfig struct {
	CccpMaxWait    time.Duration
	CccpPollPeriod time.Duration
}

// ColumnarKVConfig specifies kv related configuration options.
type ColumnarKVConfig struct {
	// ConnectTimeout is the timeout value to apply when dialling tcp connections.
	ConnectTimeout time.Duration
	// ServerWaitBackoff is the period of time that the SDK will wait before reattempting connection to a node after
	// bootstrap fails against that node.
	ServerWaitBackoff time.Duration

	ConnectionBufferSize uint
}

// ColumnarHTTPConfig specifies http related configuration options.
type ColumnarHTTPConfig struct {
	// MaxIdleConns controls the maximum number of idle (keep-alive) connections across all hosts.
	MaxIdleConns int
	// MaxIdleConnsPerHost controls the maximum idle (keep-alive) connections to keep per-host.
	MaxIdleConnsPerHost int
	// MaxConnsPerHost controls the maximum number of connections to keep per-host.
	MaxConnsPerHost int
	// IdleConnTimeout is the maximum amount of time an idle (keep-alive) connection will remain idle before closing
	// itself.
	IdleConnectionTimeout time.Duration
}
