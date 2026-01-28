package gocbcore

type clusterAgentConfig struct {
	UserAgent string

	SeedConfig SeedConfig

	SecurityConfig SecurityConfig

	HTTPConfig HTTPConfig

	TracerConfig TracerConfig

	MeterConfig MeterConfig

	ObservabilityConfig ObservabilityConfig

	TelemetryConfig TelemetryConfig

	DefaultRetryStrategy RetryStrategy
	CircuitBreakerConfig CircuitBreakerConfig

	InternalConfig InternalConfig
}

func (config *clusterAgentConfig) redacted() interface{} {
	newConfig := *config
	if isLogRedactionLevelFull() {
		// The slices here are still pointing at config's underlying arrays
		// so we need to make them not do that.
		newConfig.SeedConfig = newConfig.SeedConfig.redacted()
	}

	return newConfig
}
