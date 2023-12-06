package gocbcore

import (
	"testing"
	"time"
)

func (suite *StandardTestSuite) TestDCPAgentConfig_FromConnStr() {
	connStr := "couchbase://10.112.192.101,10.112.192.102?bootstrap_on=cccp&network=external&kv_connect_timeout=100us"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if config.KVConfig.ConnectTimeout != 100*time.Microsecond {
		suite.T().Fatalf("Ex :%v", config.KVConfig.ConnectTimeout)
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_Couchbase1() {
	connStr := "couchbase://10.112.192.101"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.SeedConfig.MemdAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.SeedConfig.MemdAddrs)
	}

	if config.SeedConfig.MemdAddrs[0] != "10.112.192.101:11210" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:11210 but was %v", config.SeedConfig.MemdAddrs[0])
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_Couchbase2() {
	connStr := "couchbase://10.112.192.101,10.112.192.102"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.SeedConfig.MemdAddrs) != 2 {
		suite.T().Fatalf("Expected MemdAddrs to be len 2 but was %v", config.SeedConfig.MemdAddrs)
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_DefaultHTTP() {
	connStr := "http://10.112.192.101:8091"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.SeedConfig.MemdAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.SeedConfig.MemdAddrs)
	}

	if len(config.SeedConfig.HTTPAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.SeedConfig.HTTPAddrs)
	}

	if config.SeedConfig.MemdAddrs[0] != "10.112.192.101:11210" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:11210 but was %v", config.SeedConfig.MemdAddrs[0])
	}

	if config.SeedConfig.HTTPAddrs[0] != "10.112.192.101:8091" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:8091 but was %v", config.SeedConfig.HTTPAddrs[0])
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_NonDefaultHTTP() {
	connStr := "http://10.112.192.101:9000"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.SeedConfig.MemdAddrs) != 0 {
		suite.T().Fatalf("Expected MemdAddrs to be len 0 but was %v", config.SeedConfig.MemdAddrs)
	}

	if len(config.SeedConfig.HTTPAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.SeedConfig.HTTPAddrs)
	}

	if config.SeedConfig.HTTPAddrs[0] != "10.112.192.101:9000" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:9000 but was %v", config.SeedConfig.HTTPAddrs[0])
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_Network() {
	tests := []struct {
		name     string
		connStr  string
		expected string
	}{
		{
			name:     "external",
			connStr:  "couchbase://10.112.192.101?network=external",
			expected: "external",
		},
		{
			name:     "default",
			connStr:  "couchbase://10.112.192.101?network=default",
			expected: "default",
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); err != nil {
				t.Errorf("FromConnStr() error = %v", err)
			}

			if config.IoConfig.NetworkType != tt.expected {
				suite.T().Fatalf("Expected %s but was %s", tt.expected, config.IoConfig.NetworkType)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_KVConnectTimeout() {
	tests := []struct {
		name     string
		connStr  string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "duration",
			connStr:  "couchbase://10.112.192.101?kv_connect_timeout=5000us",
			expected: 5 * time.Millisecond,
		},
		{
			name:     "ms",
			connStr:  "couchbase://10.112.192.101?kv_connect_timeout=5",
			expected: 5 * time.Millisecond,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?kv_connect_timeout=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.KVConfig.ConnectTimeout != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KVConfig.ConnectTimeout)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_ConfigPollTimeout() {
	tests := []struct {
		name     string
		connStr  string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "duration",
			connStr:  "couchbase://10.112.192.101?config_poll_timeout=5000us",
			expected: 5 * time.Millisecond,
		},
		{
			name:     "ms",
			connStr:  "couchbase://10.112.192.101?config_poll_timeout=5",
			expected: 5 * time.Millisecond,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?config_poll_timeout=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.ConfigPollerConfig.CccpMaxWait != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ConfigPollerConfig.CccpMaxWait)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_ConfigPollPeriod() {
	tests := []struct {
		name     string
		connStr  string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "duration",
			connStr:  "couchbase://10.112.192.101?config_poll_interval=5000us",
			expected: 5 * time.Millisecond,
		},
		{
			name:     "ms",
			connStr:  "couchbase://10.112.192.101?config_poll_interval=5",
			expected: 5 * time.Millisecond,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?config_poll_interval=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.ConfigPollerConfig.CccpPollPeriod != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ConfigPollerConfig.CccpPollPeriod)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_Compression() {
	tests := []struct {
		name     string
		connStr  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "true",
			connStr:  "couchbase://10.112.192.101?compression=true",
			expected: true,
		},
		{
			name:     "false",
			connStr:  "couchbase://10.112.192.101?compression=false",
			expected: false,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?compression=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CompressionConfig.Enabled != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.CompressionConfig.Enabled)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_CompressionMinSize() {
	tests := []struct {
		name     string
		connStr  string
		expected int
		wantErr  bool
	}{
		{
			name:     "valid",
			connStr:  "couchbase://10.112.192.101?compression_min_size=100000",
			expected: 100000,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?compression_min_size=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CompressionConfig.MinSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.CompressionConfig.MinSize)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_CompressionMinRatio() {
	tests := []struct {
		name     string
		connStr  string
		expected float64
		wantErr  bool
	}{
		{
			name:     "valid",
			connStr:  "couchbase://10.112.192.101?compression_min_ratio=0.7",
			expected: 0.7,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?compression_min_ratio=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CompressionConfig.MinRatio != tt.expected {
				suite.T().Fatalf("Expected %f but was %f", tt.expected, config.CompressionConfig.MinRatio)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_DCPPriority() {
	tests := []struct {
		name     string
		connStr  string
		expected DcpAgentPriority
		wantErr  bool
	}{
		{
			name:     "empty",
			connStr:  "couchbase://10.112.192.101?dcp_priority=",
			expected: DcpAgentPriorityLow,
		},
		{
			name:     "low",
			connStr:  "couchbase://10.112.192.101?dcp_priority=low",
			expected: DcpAgentPriorityLow,
		},
		{
			name:     "medium",
			connStr:  "couchbase://10.112.192.101?dcp_priority=medium",
			expected: DcpAgentPriorityMed,
		},
		{
			name:     "high",
			connStr:  "couchbase://10.112.192.101?dcp_priority=high",
			expected: DcpAgentPriorityHigh,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?dcp_priority=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.DCPConfig.AgentPriority != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.DCPConfig.AgentPriority)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_EnableDCPExpiry() {
	tests := []struct {
		name     string
		connStr  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "true",
			connStr:  "couchbase://10.112.192.101?enable_dcp_expiry=true",
			expected: true,
		},
		{
			name:     "false",
			connStr:  "couchbase://10.112.192.101?enable_dcp_expiry=false",
			expected: false,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?enable_dcp_expiry=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.DCPConfig.UseExpiryOpcode != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.DCPConfig.UseExpiryOpcode)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_EnableDCPChangeStreams() {
	tests := []struct {
		name     string
		connStr  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "true",
			connStr:  "couchbase://10.112.192.101?enable_dcp_change_streams=true",
			expected: true,
		},
		{
			name:     "false",
			connStr:  "couchbase://10.112.192.101?enable_dcp_change_streams=false",
			expected: false,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?enable_dcp_change_streams=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.DCPConfig.UseChangeStreams != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.DCPConfig.UseChangeStreams)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_KVPoolSize() {
	tests := []struct {
		name     string
		connStr  string
		expected int
		wantErr  bool
	}{
		{
			name:     "valid",
			connStr:  "couchbase://10.112.192.101?kv_pool_size=2",
			expected: 2,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?kv_pool_size=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.KVConfig.PoolSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KVConfig.PoolSize)
			}
		})
	}
}

func (suite *StandardTestSuite) TestDCPAgentConfig_MaxQueueSize() {
	tests := []struct {
		name     string
		connStr  string
		expected int
		wantErr  bool
	}{
		{
			name:     "valid",
			connStr:  "couchbase://10.112.192.101?max_queue_size=2",
			expected: 2,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?max_queue_size=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.KVConfig.MaxQueueSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KVConfig.MaxQueueSize)
			}
		})
	}
}
