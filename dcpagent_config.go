package gocbcore

import (
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/couchbaselabs/gocbconnstr"
)

// DCPAgentConfig specifies the configuration options for creation of a DCPAgent.
type DCPAgentConfig struct {
	UserAgent   string
	MemdAddrs   []string
	UseTLS      bool
	BucketName  string
	NetworkType string
	Auth        AuthProvider

	TLSRootCAs    *x509.CertPool
	TLSSkipVerify bool

	UseCompression       bool
	DisableDecompression bool

	UseCollections bool

	CompressionMinSize  int
	CompressionMinRatio float64

	CccpMaxWait    time.Duration
	CccpPollPeriod time.Duration

	ConnectTimeout   time.Duration
	KVConnectTimeout time.Duration
	KvPoolSize       int
	MaxQueueSize     int

	DcpAgentPriority  DcpAgentPriority
	UseDCPExpiry      bool
	UseDCPStreamID    bool
	UseDCPOSOBackfill bool
}

func (config *DCPAgentConfig) redacted() interface{} {
	newConfig := DCPAgentConfig{}
	newConfig = *config
	if isLogRedactionLevelFull() {
		// The slices here are still pointing at config's underlying arrays
		// so we need to make them not do that.
		newConfig.MemdAddrs = append([]string(nil), newConfig.MemdAddrs...)
		for i, addr := range newConfig.MemdAddrs {
			newConfig.MemdAddrs[i] = redactSystemData(addr)
		}

		if newConfig.BucketName != "" {
			newConfig.BucketName = redactMetaData(newConfig.BucketName)
		}
	}

	return newConfig
}

// FromConnStr populates the AgentConfig with information from a
// Couchbase Connection String.
// Supported options are:
//   ca_cert_path (string) - Specifies the path to a CA certificate.
//   network (string) - The network type to use.
//   kv_connect_timeout (duration) - Maximum period to attempt to connect to cluster in ms.
//   config_poll_interval (duration) - Period to wait between CCCP config polling in ms.
//   config_poll_timeout (duration) - Maximum period of time to wait for a CCCP request.
//   compression (bool) - Whether to enable network-wise compression of documents.
//   compression_min_size (int) - The minimal size of the document in bytes to consider compression.
//   compression_min_ratio (float64) - The minimal compress ratio (compressed / original) for the document to be sent compressed.
//   orphaned_response_logging (bool) - Whether to enable orphaned response logging.
//   orphaned_response_logging_interval (duration) - How often to print the orphan log records.
//   orphaned_response_logging_sample_size (int) - The maximum number of orphan log records to track.
//   dcp_priority (int) - Specifies the priority to request from the Cluster when connecting for DCP.
//   enable_dcp_expiry (bool) - Whether to enable the feature to distinguish between explicit delete and expired delete on DCP.
//   kv_pool_size (int) - The number of connections to create to each kv node.
//   max_queue_size (int) - The maximum number of requests that can be queued for sending per connection.
func (config *DCPAgentConfig) FromConnStr(connStr string) error {
	baseSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return err
	}

	if baseSpec.Scheme == "http" {
		return errors.New("http scheme is not supported for dcp agent, use couchbase or couchbases instead")
	}

	spec, err := gocbconnstr.Resolve(baseSpec)
	if err != nil {
		return err
	}

	fetchOption := func(name string) (string, bool) {
		optValue := spec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}
		return optValue[len(optValue)-1], true
	}

	var memdHosts []string
	for _, specHost := range spec.MemdHosts {
		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	config.MemdAddrs = memdHosts

	if spec.UseSsl {
		var cacertpaths []string

		cacertpaths = spec.Options["ca_cert_path"]

		if len(cacertpaths) > 0 {
			roots := x509.NewCertPool()

			for _, path := range cacertpaths {
				cacert, err := ioutil.ReadFile(path)
				if err != nil {
					return err
				}

				ok := roots.AppendCertsFromPEM(cacert)
				if !ok {
					return errInvalidCertificate
				}
			}

			config.TLSRootCAs = roots
		} else {
			config.TLSSkipVerify = true
		}

		config.UseTLS = true
	}

	if spec.Bucket != "" {
		config.BucketName = spec.Bucket
	}

	if valStr, ok := fetchOption("network"); ok {
		if valStr == "default" {
			valStr = ""
		}

		config.NetworkType = valStr
	}

	if valStr, ok := fetchOption("kv_connect_timeout"); ok {
		val, err := parseDurationOrInt(valStr)
		if err != nil {
			return fmt.Errorf("kv_connect_timeout option must be a duration or a number")
		}
		config.KVConnectTimeout = val
	}

	if valStr, ok := fetchOption("config_poll_timeout"); ok {
		val, err := parseDurationOrInt(valStr)
		if err != nil {
			return fmt.Errorf("config poll timeout option must be a duration or a number")
		}
		config.CccpMaxWait = val
	}

	if valStr, ok := fetchOption("config_poll_interval"); ok {
		val, err := parseDurationOrInt(valStr)
		if err != nil {
			return fmt.Errorf("config pool interval option must be duration or a number")
		}
		config.CccpPollPeriod = val
	}

	if valStr, ok := fetchOption("compression"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return fmt.Errorf("compression option must be a boolean")
		}
		config.UseCompression = val
	}

	if valStr, ok := fetchOption("compression_min_size"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("compression_min_size option must be an int")
		}
		config.CompressionMinSize = int(val)
	}

	if valStr, ok := fetchOption("compression_min_ratio"); ok {
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			return fmt.Errorf("compression_min_size option must be an int")
		}
		config.CompressionMinRatio = val
	}

	// This option is experimental
	if valStr, ok := fetchOption("dcp_priority"); ok {
		var priority DcpAgentPriority
		switch valStr {
		case "":
			priority = DcpAgentPriorityLow
		case "low":
			priority = DcpAgentPriorityLow
		case "medium":
			priority = DcpAgentPriorityMed
		case "high":
			priority = DcpAgentPriorityHigh
		default:
			return fmt.Errorf("dcp_priority must be one of low, medium or high")
		}
		config.DcpAgentPriority = priority
	}

	// This option is experimental
	if valStr, ok := fetchOption("enable_dcp_expiry"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return fmt.Errorf("enable_dcp_expiry option must be a boolean")
		}
		config.UseDCPExpiry = val
	}

	// This option is experimental
	if valStr, ok := fetchOption("kv_pool_size"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kv pool size option must be a number")
		}
		config.KvPoolSize = int(val)
	}

	// This option is experimental
	if valStr, ok := fetchOption("max_queue_size"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("max queue size option must be a number")
		}
		config.MaxQueueSize = int(val)
	}

	return nil
}
