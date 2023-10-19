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

	if config.KVConfig.ConnectTimeout != 100*time.Microsecond {
		suite.T().Fatalf("Ex :%v", config.KVConfig.ConnectTimeout)
	}
}

func (suite *StandardTestSuite) TestAgentConfig_Couchbase1() {
	connStr := "couchbase://10.112.192.101"

	config := &AgentConfig{}
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

func (suite *StandardTestSuite) TestAgentConfig_Couchbase2() {
	connStr := "couchbase://10.112.192.101,10.112.192.102"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		suite.T().Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.SeedConfig.MemdAddrs) != 2 {
		suite.T().Fatalf("Expected MemdAddrs to be len 2 but was %v", config.SeedConfig.MemdAddrs)
	}

	if len(config.SeedConfig.HTTPAddrs) != 2 {
		suite.T().Fatalf("Expected MemdAddrs to be len 2 but was %v", config.SeedConfig.HTTPAddrs)
	}
}

func (suite *StandardTestSuite) TestAgentConfig_DefaultHTTP() {
	connStr := "http://10.112.192.101:8091"

	config := &AgentConfig{}
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

func (suite *StandardTestSuite) TestAgentConfig_NonDefaultHTTP() {
	connStr := "http://10.112.192.101:9000"

	config := &AgentConfig{}
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

			if len(config.SeedConfig.MemdAddrs) != tt.lenMemdAddrs {
				suite.T().Fatalf("Expected MemdAddrs to be len %d but was %v", tt.lenMemdAddrs, config.SeedConfig.HTTPAddrs)
			}

			if len(config.SeedConfig.HTTPAddrs) != tt.lenHttpAddrs {
				suite.T().Fatalf("Expected HTTPAddrs to be len %d but was %v", tt.lenHttpAddrs, config.SeedConfig.HTTPAddrs)
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

			if config.IoConfig.NetworkType != tt.expected {
				suite.T().Fatalf("Expected %s but was %s", tt.expected, config.IoConfig.NetworkType)
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

			if config.KVConfig.ConnectTimeout != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KVConfig.ConnectTimeout)
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

			if config.ConfigPollerConfig.CccpMaxWait != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ConfigPollerConfig.CccpMaxWait)
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

			if config.ConfigPollerConfig.CccpPollPeriod != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ConfigPollerConfig.CccpPollPeriod)
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

			if config.IoConfig.UseMutationTokens != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.IoConfig.UseMutationTokens)
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

			if config.CompressionConfig.Enabled != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.CompressionConfig.Enabled)
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

			if config.CompressionConfig.MinSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.CompressionConfig.MinSize)
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

			if config.CompressionConfig.MinRatio != tt.expected {
				suite.T().Fatalf("Expected %f but was %f", tt.expected, config.CompressionConfig.MinRatio)
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

			if config.IoConfig.UseDurations != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.IoConfig.UseDurations)
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

			if config.HTTPConfig.MaxIdleConns != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.HTTPConfig.MaxIdleConns)
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

			if config.HTTPConfig.MaxIdleConnsPerHost != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.HTTPConfig.MaxIdleConnsPerHost)
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

			if config.HTTPConfig.IdleConnectionTimeout != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.HTTPConfig.IdleConnectionTimeout)
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

			if config.OrphanReporterConfig.Enabled != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.OrphanReporterConfig.Enabled)
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

			if config.OrphanReporterConfig.ReportInterval != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.OrphanReporterConfig.ReportInterval)
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

			if config.OrphanReporterConfig.SampleSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.OrphanReporterConfig.SampleSize)
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

			if config.ConfigPollerConfig.HTTPRedialPeriod != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ConfigPollerConfig.HTTPRedialPeriod)
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

			if config.ConfigPollerConfig.HTTPRetryDelay != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.ConfigPollerConfig.HTTPRetryDelay)
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

			if config.KVConfig.PoolSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KVConfig.PoolSize)
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

			if config.KVConfig.MaxQueueSize != tt.expected {
				suite.T().Fatalf("Expected %d but was %d", tt.expected, config.KVConfig.MaxQueueSize)
			}
		})
	}
}

func (suite *StandardTestSuite) TestAgentConfig_UseClusterMapNotifications() {
	tests := []struct {
		name     string
		connStr  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "true",
			connStr:  "couchbase://10.112.192.101?enable_cluster_config_notifications=true",
			expected: true,
		},
		{
			name:     "false",
			connStr:  "couchbase://10.112.192.101?enable_cluster_config_notifications=false",
			expected: false,
		},
		{
			name:    "invalid",
			connStr: "couchbase://10.112.192.101?enable_cluster_config_notifications=squirrel",
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

			if config.IoConfig.UseClusterMapNotifications != tt.expected {
				suite.T().Fatalf("Expected %t but was %t", tt.expected, config.IoConfig.UseDurations)
			}
		})
	}
}
