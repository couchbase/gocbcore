package gocbcore

import (
	"testing"
	"time"
)

func (suite *StandardTestSuite) TestAgentConfig_FromConnStr() {
	connStr := "couchbase://10.112.192.101,10.112.192.102?bootstrap_on=cccp&network=external&kv_connect_timeout=100us"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if config.KVConnectTimeout != 100*time.Microsecond {
		suite.T().Fatalf("Ex :%v", config.KVConnectTimeout)
	}
}

func (suite *StandardTestSuite) TestAgentConfig_Couchbase1() {
	connStr := "couchbase://10.112.192.101"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.HTTPAddrs)
	}

	if config.MemdAddrs[0] != "10.112.192.101:11210" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:11210 but was %v", config.MemdAddrs[0])
	}

	if config.HTTPAddrs[0] != "10.112.192.101:8091" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:8091 but was %v", config.HTTPAddrs[0])
	}
}

func (suite *StandardTestSuite) TestAgentConfig_Couchbase2() {
	connStr := "couchbase://10.112.192.101,10.112.192.102"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 2 {
		suite.T().Fatalf("Expected MemdAddrs to be len 2 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 2 {
		suite.T().Fatalf("Expected MemdAddrs to be len 2 but was %v", config.HTTPAddrs)
	}
}

func (suite *StandardTestSuite) TestAgentConfig_DefaultHTTP() {
	connStr := "http://10.112.192.101:8091"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.HTTPAddrs)
	}

	if config.MemdAddrs[0] != "10.112.192.101:11210" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:11210 but was %v", config.MemdAddrs[0])
	}

	if config.HTTPAddrs[0] != "10.112.192.101:8091" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:8091 but was %v", config.HTTPAddrs[0])
	}
}

func (suite *StandardTestSuite) TestAgentConfig_NonDefaultHTTP() {
	connStr := "http://10.112.192.101:9000"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 0 {
		suite.T().Fatalf("Expected MemdAddrs to be len 0 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 1 {
		suite.T().Fatalf("Expected MemdAddrs to be len 1 but was %v", config.HTTPAddrs)
	}

	if config.HTTPAddrs[0] != "10.112.192.101:9000" {
		suite.T().Fatalf("Expected address to be 10.112.192.101:9000 but was %v", config.HTTPAddrs[0])
	}
}

func (suite *StandardTestSuite) TestAgentConfig_BootstrapOnCCCP() {
	tests := []struct {
		name         string
		connStr      string
		lenMemdAddrs int
		lenHttpAddrs int
	}{
		{
			name:         "cccp",
			connStr:      "couchbase://10.112.192.101?bootstrap_on=cccp",
			lenMemdAddrs: 1,
			lenHttpAddrs: 0,
		},
		{
			name:         "http",
			connStr:      "couchbase://10.112.192.101?bootstrap_on=http",
			lenMemdAddrs: 0,
			lenHttpAddrs: 1,
		},
		{
			name:         "both",
			connStr:      "couchbase://10.112.192.101?bootstrap_on=both",
			lenMemdAddrs: 1,
			lenHttpAddrs: 1,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); err != nil {
				t.Errorf("FromConnStr() error = %v", err)
			}

			if len(config.MemdAddrs) != tt.lenMemdAddrs {
				suite.T().Fatalf("Expected MemdAddrs to be len %d but was %v", tt.lenMemdAddrs, config.HTTPAddrs)
			}

			if len(config.HTTPAddrs) != tt.lenHttpAddrs {
				suite.T().Fatalf("Expected HTTPAddrs to be len %d but was %v", tt.lenHttpAddrs, config.HTTPAddrs)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_Network() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); err != nil {
				t.Errorf("FromConnStr() error = %v", err)
			}

			if config.NetworkType != tt.expected {
				suite.T().Fatalf("Expected %s but was %s", tt.expected, config.NetworkType)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_KVConnectTimeout() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.KVConnectTimeout != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KVConnectTimeout)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_ConfigPollTimeout() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.CccpMaxWait != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.CccpMaxWait)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_ConfigPollPeriod() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CccpPollPeriod != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.CccpPollPeriod)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_EnableMutationTokens() {
	tests := []struct {
		name     string
		connStr  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "true",
			connStr:  "couchbase://10.112.192.101?enable_mutation_tokens=true",
			expected: true,
		},
		{
			name:     "false",
			connStr:  "couchbase://10.112.192.101?enable_mutation_tokens=false",
			expected: false,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?enable_mutation_tokens=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseMutationTokens != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.UseMutationTokens)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_Compression() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseCompression != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.UseCompression)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_CompressionMinSize() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CompressionMinSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.CompressionMinSize)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_CompressionMinRatio() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CompressionMinRatio != tt.expected {
				suite.T().Fatalf("Expected %f but was %f", tt.expected, config.CompressionMinRatio)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_ServerDurations() {
	tests := []struct {
		name     string
		connStr  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "true",
			connStr:  "couchbase://10.112.192.101?enable_server_durations=true",
			expected: true,
		},
		{
			name:     "false",
			connStr:  "couchbase://10.112.192.101?enable_server_durations=false",
			expected: false,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?enable_server_durations=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseDurations != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.UseDurations)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_MaxIdleHTTPConnections() {
	tests := []struct {
		name     string
		connStr  string
		expected int
		wantErr  bool
	}{
		{
			name:     "valid",
			connStr:  "couchbase://10.112.192.101?max_idle_http_connections=2",
			expected: 2,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?max_idle_http_connections=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPMaxIdleConns != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.HTTPMaxIdleConns)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_MaxPerHostIdleHTTPConnections() {
	tests := []struct {
		name     string
		connStr  string
		expected int
		wantErr  bool
	}{
		{
			name:     "valid",
			connStr:  "couchbase://10.112.192.101?max_perhost_idle_http_connections=2",
			expected: 2,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?max_perhost_idle_http_connections=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPMaxIdleConnsPerHost != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.HTTPMaxIdleConnsPerHost)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_IdleHTTPConnectionTimeout() {
	tests := []struct {
		name     string
		connStr  string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "duration",
			connStr:  "couchbase://10.112.192.101?idle_http_connection_timeout=5000us",
			expected: 5 * time.Millisecond,
		},
		{
			name:     "ms",
			connStr:  "couchbase://10.112.192.101?idle_http_connection_timeout=5",
			expected: 5 * time.Millisecond,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?idle_http_connection_timeout=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPIdleConnectionTimeout != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.HTTPIdleConnectionTimeout)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_OrphanResponseLogging() {
	tests := []struct {
		name     string
		connStr  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "true",
			connStr:  "couchbase://10.112.192.101?orphaned_response_logging=true",
			expected: true,
		},
		{
			name:     "false",
			connStr:  "couchbase://10.112.192.101?orphaned_response_logging=false",
			expected: false,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?orphaned_response_logging=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseZombieLogger != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.UseZombieLogger)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_OrphanResponseLoggingInterval() {
	tests := []struct {
		name     string
		connStr  string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "duration",
			connStr:  "couchbase://10.112.192.101?orphaned_response_logging_interval=5000us",
			expected: 5 * time.Millisecond,
		},
		{
			name:     "ms",
			connStr:  "couchbase://10.112.192.101?orphaned_response_logging_interval=5",
			expected: 5 * time.Millisecond,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?orphaned_response_logging_interval=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.ZombieLoggerInterval != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerInterval)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_OrphanResponseLoggerSampleSize() {
	tests := []struct {
		name     string
		connStr  string
		expected int
		wantErr  bool
	}{
		{
			name:     "valid",
			connStr:  "couchbase://10.112.192.101?orphaned_response_logging_sample_size=2",
			expected: 2,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?orphaned_response_logging_sample_size=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.ZombieLoggerSampleSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerSampleSize)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_HTTPRedialPeriod() {
	tests := []struct {
		name     string
		connStr  string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "duration",
			connStr:  "couchbase://10.112.192.101?http_redial_period=5000us",
			expected: 5 * time.Millisecond,
		},
		{
			name:     "ms",
			connStr:  "couchbase://10.112.192.101?http_redial_period=5",
			expected: 5 * time.Millisecond,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?http_redial_period=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPRedialPeriod != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerInterval)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_HTTPRetryDelay() {
	tests := []struct {
		name     string
		connStr  string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "duration",
			connStr:  "couchbase://10.112.192.101?http_retry_delay=5000us",
			expected: 5 * time.Millisecond,
		},
		{
			name:     "ms",
			connStr:  "couchbase://10.112.192.101?http_retry_delay=5",
			expected: 5 * time.Millisecond,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?http_retry_delay=squirrel",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPRetryDelay != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerInterval)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_KVPoolSize() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.KvPoolSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KvPoolSize)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_MaxQueueSize() {
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
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.MaxQueueSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.MaxQueueSize)
			}
		})
	}
}
