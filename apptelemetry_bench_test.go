package gocbcore

import (
	"fmt"
	"github.com/google/uuid"
	"sync/atomic"
	"testing"
)

type noopTelemetryClient struct {
}

func (c *noopTelemetryClient) connectIfNotStarted() {
	return
}

func (c *noopTelemetryClient) connectionInitiated() bool {
	return true
}

func (c *noopTelemetryClient) Close() {
	return
}

func (c *noopTelemetryClient) usesExternalEndpoint() bool {
	return true
}

func (c *noopTelemetryClient) updateEndpoints(telemetryEndpoints) {
	return
}

func BenchmarkAppTelemetrySet(b *testing.B) {
	b.ReportAllocs()

	suite := GetBenchSuite()

	agent, err := createAgentWithTelemetryReporter(&TelemetryReporter{metrics: newTelemetryMetrics(), client: &noopTelemetryClient{}})
	if err != nil {
		b.Fatalf("Failed to create agent: %s", err)
	}
	defer agent.Close()

	key := []byte(uuid.New().String())
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	var i uint32
	suite.RunParallel(b, func(cb func(error)) error {
		keyNum := atomic.AddUint32(&i, 1)
		_, err := agent.Set(SetOptions{
			Key:   []byte(fmt.Sprintf("%s-%d", key, keyNum)),
			Value: randomBytes,
		}, func(result *StoreResult, err error) {
			cb(err)
		})
		return err
	})
}

func BenchmarkAppTelemetryGet(b *testing.B) {
	b.ReportAllocs()

	suite := GetBenchSuite()

	agent, err := createAgentWithTelemetryReporter(&TelemetryReporter{metrics: newTelemetryMetrics(), client: &noopTelemetryClient{}})
	if err != nil {
		b.Fatalf("Failed to create agent: %s", err)
	}
	defer agent.Close()
	s := suite.GetHarness(b)

	key := []byte(uuid.New().String())
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	s.PushOp(agent.Set(SetOptions{
		Key:   key,
		Value: randomBytes,
	}, func(result *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Failed to set: %v", err)
			}
		})
	}))
	s.Wait(0)

	suite.RunParallel(b, func(cb func(error)) error {
		_, err := agent.Get(GetOptions{
			Key: key,
		}, func(result *GetResult, err error) {
			cb(err)
		})
		return err
	})
}
