package gocbcore

import (
	"testing"
	"time"
)

func TestAgentConfig_FromConnStr(t *testing.T) {
	connStr := "couchbase://10.112.192.101,10.112.192.102?bootstrap_on=cccp&network=external&kv_connect_timeout=100us"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if config.KVConnectTimeout != 100*time.Microsecond {
		t.Fatalf("Ex :%v", config.KVConnectTimeout)
	}
}

func TestAgentConfig_Couchbase1(t *testing.T) {
	connStr := "couchbase://10.112.192.101"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 1 {
		t.Fatalf("Expected MemdAddrs to be len 1 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 1 {
		t.Fatalf("Expected MemdAddrs to be len 1 but was %v", config.HTTPAddrs)
	}

	if config.MemdAddrs[0] != "10.112.192.101:11210" {
		t.Fatalf("Expected address to be 10.112.192.101:11210 but was %v", config.MemdAddrs[0])
	}

	if config.HTTPAddrs[0] != "10.112.192.101:8091" {
		t.Fatalf("Expected address to be 10.112.192.101:8091 but was %v", config.HTTPAddrs[0])
	}
}

func TestAgentConfig_Couchbase2(t *testing.T) {
	connStr := "couchbase://10.112.192.101,10.112.192.102"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 2 {
		t.Fatalf("Expected MemdAddrs to be len 2 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 2 {
		t.Fatalf("Expected MemdAddrs to be len 2 but was %v", config.HTTPAddrs)
	}
}

func TestAgentConfig_DefaultHTTP(t *testing.T) {
	connStr := "http://10.112.192.101:8091"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 1 {
		t.Fatalf("Expected MemdAddrs to be len 1 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 1 {
		t.Fatalf("Expected MemdAddrs to be len 1 but was %v", config.HTTPAddrs)
	}

	if config.MemdAddrs[0] != "10.112.192.101:11210" {
		t.Fatalf("Expected address to be 10.112.192.101:11210 but was %v", config.MemdAddrs[0])
	}

	if config.HTTPAddrs[0] != "10.112.192.101:8091" {
		t.Fatalf("Expected address to be 10.112.192.101:8091 but was %v", config.HTTPAddrs[0])
	}
}

func TestAgentConfig_NonDefaultHTTP(t *testing.T) {
	connStr := "http://10.112.192.101:9000"

	config := &AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 0 {
		t.Fatalf("Expected MemdAddrs to be len 0 but was %v", config.MemdAddrs)
	}

	if len(config.HTTPAddrs) != 1 {
		t.Fatalf("Expected MemdAddrs to be len 1 but was %v", config.HTTPAddrs)
	}

	if config.HTTPAddrs[0] != "10.112.192.101:9000" {
		t.Fatalf("Expected address to be 10.112.192.101:9000 but was %v", config.HTTPAddrs[0])
	}
}

func TestAgentConfig_BootstrapOnCCCP(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); err != nil {
				t.Errorf("FromConnStr() error = %v", err)
			}

			if len(config.MemdAddrs) != tt.lenMemdAddrs {
				t.Fatalf("Expected MemdAddrs to be len %d but was %v", tt.lenMemdAddrs, config.HTTPAddrs)
			}

			if len(config.HTTPAddrs) != tt.lenHttpAddrs {
				t.Fatalf("Expected HTTPAddrs to be len %d but was %v", tt.lenHttpAddrs, config.HTTPAddrs)
			}
		})
	}
}

func TestAgentConfig_Network(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); err != nil {
				t.Errorf("FromConnStr() error = %v", err)
			}

			if config.NetworkType != tt.expected {
				t.Fatalf("Expected %s but was %s", tt.expected, config.NetworkType)
			}
		})
	}
}

func TestAgentConfig_KVConnectTimeout(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.KVConnectTimeout != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.KVConnectTimeout)
			}
		})
	}
}

func TestAgentConfig_ConfigPollTimeout(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.CccpMaxWait != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.CccpMaxWait)
			}
		})
	}
}

func TestAgentConfig_ConfigPollPeriod(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CccpPollPeriod != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.CccpPollPeriod)
			}
		})
	}
}

func TestAgentConfig_EnableMutationTokens(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseMutationTokens != tt.expected {
				t.Fatalf("Expected %t but was %t", tt.expected, config.UseMutationTokens)
			}
		})
	}
}

func TestAgentConfig_Compression(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseCompression != tt.expected {
				t.Fatalf("Expected %t but was %t", tt.expected, config.UseCompression)
			}
		})
	}
}

func TestAgentConfig_CompressionMinSize(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CompressionMinSize != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.CompressionMinSize)
			}
		})
	}
}

func TestAgentConfig_CompressionMinRatio(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.CompressionMinRatio != tt.expected {
				t.Fatalf("Expected %f but was %f", tt.expected, config.CompressionMinRatio)
			}
		})
	}
}

func TestAgentConfig_ServerDurations(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseDurations != tt.expected {
				t.Fatalf("Expected %t but was %t", tt.expected, config.UseDurations)
			}
		})
	}
}

func TestAgentConfig_MaxIdleHTTPConnections(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPMaxIdleConns != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.HTTPMaxIdleConns)
			}
		})
	}
}

func TestAgentConfig_MaxPerHostIdleHTTPConnections(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPMaxIdleConnsPerHost != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.HTTPMaxIdleConnsPerHost)
			}
		})
	}
}

func TestAgentConfig_IdleHTTPConnectionTimeout(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPIdleConnectionTimeout != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.HTTPIdleConnectionTimeout)
			}
		})
	}
}

func TestAgentConfig_OrphanResponseLogging(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseZombieLogger != tt.expected {
				t.Fatalf("Expected %t but was %t", tt.expected, config.UseZombieLogger)
			}
		})
	}
}

func TestAgentConfig_OrphanResponseLoggingInterval(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.ZombieLoggerInterval != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerInterval)
			}
		})
	}
}

func TestAgentConfig_OrphanResponseLoggerSampleSize(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.ZombieLoggerSampleSize != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerSampleSize)
			}
		})
	}
}

func TestAgentConfig_HTTPRedialPeriod(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPRedialPeriod != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerInterval)
			}
		})
	}
}

func TestAgentConfig_HTTPRetryDelay(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.HTTPRetryDelay != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.ZombieLoggerInterval)
			}
		})
	}
}

func TestAgentConfig_KVPoolSize(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.KvPoolSize != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.KvPoolSize)
			}
		})
	}
}

func TestAgentConfig_MaxQueueSize(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &AgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.MaxQueueSize != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.MaxQueueSize)
			}
		})
	}
}
