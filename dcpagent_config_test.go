package gocbcore

import (
	"testing"
	"time"
)

func TestDcpDCPAgentConfig_FromConnStr(t *testing.T) {
	connStr := "couchbase://10.112.192.101,10.112.192.102?bootstrap_on=cccp&network=external&kv_connect_timeout=100us"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if config.KVConnectTimeout != 100*time.Microsecond {
		t.Fatalf("Ex :%v", config.KVConnectTimeout)
	}
}

func TestDcpDCPAgentConfig_Couchbase1(t *testing.T) {
	connStr := "couchbase://10.112.192.101"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 1 {
		t.Fatalf("Expected MemdAddrs to be len 1 but was %v", config.MemdAddrs)
	}

	if config.MemdAddrs[0] != "10.112.192.101:11210" {
		t.Fatalf("Expected address to be 10.112.192.101:11210 but was %v", config.MemdAddrs[0])
	}
}

func TestDcpDCPAgentConfig_Couchbase2(t *testing.T) {
	connStr := "couchbase://10.112.192.101,10.112.192.102"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to execute FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 2 {
		t.Fatalf("Expected MemdAddrs to be len 2 but was %v", config.MemdAddrs)
	}
}

func TestDcpDCPAgentConfig_DefaultHTTP(t *testing.T) {
	connStr := "http://10.112.192.101:8091"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err == nil {
		t.Fatalf("Expected to fail for http scheme FromConnStr: %v", err)
	}
}

func TestDcpDCPAgentConfig_NonDefaultHTTP(t *testing.T) {
	connStr := "http://10.112.192.101:9000"

	config := &DCPAgentConfig{}
	err := config.FromConnStr(connStr)
	if err == nil {
		t.Fatalf("Expected to fail for http scheme FromConnStr: %v", err)
	}

	if len(config.MemdAddrs) != 0 {
		t.Fatalf("Expected MemdAddrs to be len 0 but was %v", config.MemdAddrs)
	}
}

func TestDcpDCPAgentConfig_Network(t *testing.T) {
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
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); err != nil {
				t.Errorf("FromConnStr() error = %v", err)
			}

			if config.NetworkType != tt.expected {
				t.Fatalf("Expected %s but was %s", tt.expected, config.NetworkType)
			}
		})
	}
}

func TestDcpDCPAgentConfig_KVConnectTimeout(t *testing.T) {
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
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.KVConnectTimeout != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.KVConnectTimeout)
			}
		})
	}
}

func TestDcpDCPAgentConfig_ConfigPollTimeout(t *testing.T) {
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
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if config.CccpMaxWait != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.CccpMaxWait)
			}
		})
	}
}

func TestDcpDCPAgentConfig_ConfigPollPeriod(t *testing.T) {
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
			config := &DCPAgentConfig{}
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

func TestDcpDCPAgentConfig_Compression(t *testing.T) {
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
			config := &DCPAgentConfig{}
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

func TestDcpDCPAgentConfig_CompressionMinSize(t *testing.T) {
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
			config := &DCPAgentConfig{}
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

func TestDcpDCPAgentConfig_CompressionMinRatio(t *testing.T) {
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
			config := &DCPAgentConfig{}
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

func TestDcpDCPAgentConfig_DCPPriority(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.AgentPriority != tt.expected {
				t.Fatalf("Expected %d but was %d", tt.expected, config.AgentPriority)
			}
		})
	}
}

func TestDcpDCPAgentConfig_EnableDCPExpiry(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			config := &DCPAgentConfig{}
			if err := config.FromConnStr(tt.connStr); (err != nil) != tt.wantErr {
				t.Errorf("FromConnStr() error = %v, wanted error = %t", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if config.UseExpiryOpcode != tt.expected {
				t.Fatalf("Expected %t but was %t", tt.expected, config.UseExpiryOpcode)
			}
		})
	}
}

func TestDcpDCPAgentConfig_KVPoolSize(t *testing.T) {
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
			config := &DCPAgentConfig{}
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

func TestDcpDCPAgentConfig_MaxQueueSize(t *testing.T) {
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
			config := &DCPAgentConfig{}
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
