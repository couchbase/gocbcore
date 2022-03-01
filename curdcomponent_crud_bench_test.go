package gocbcore

import (
	"fmt"
	"github.com/google/uuid"
	"sync/atomic"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	b.ReportAllocs()

	suite := GetBenchSuite()

	agent := suite.GetAgent()

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

func BenchmarkReplace(b *testing.B) {
	b.ReportAllocs()

	suite := GetBenchSuite()

	agent, s := suite.GetAgentAndHarness(b)

	key := []byte(uuid.New().String())

	s.PushOp(agent.Set(SetOptions{
		Key:   key,
		Value: []byte("{}"),
	}, func(result *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Failed to set: %v", err)
			}
		})
	}))
	s.Wait(0)

	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	suite.RunParallel(b, func(cb func(error)) error {
		_, err := agent.Replace(ReplaceOptions{
			Key:   key,
			Value: randomBytes,
		}, func(result *StoreResult, err error) {
			cb(err)
		})
		return err
	})
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()

	suite := GetBenchSuite()

	agent, s := suite.GetAgentAndHarness(b)

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

func BenchmarkSetGet(b *testing.B) {
	b.ReportAllocs()

	suite := GetBenchSuite()

	agent := suite.GetAgent()

	key := []byte(uuid.New().String())
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	runGet := func(cb func(error)) {
		_, err := agent.Get(GetOptions{
			Key: key,
		}, func(result *GetResult, err error) {
			cb(err)
		})
		if err != nil {
			cb(err)
		}
	}

	suite.RunParallel(b, func(cb func(error)) error {
		_, err := agent.Set(SetOptions{
			Key:   key,
			Value: randomBytes,
		}, func(result *StoreResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			runGet(cb)
		})
		return err
	})
}
