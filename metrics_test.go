package gocbcore

import (
	"sync"
	"sync/atomic"
)

type testCounter struct {
	count uint64
}

func (tc *testCounter) IncrementBy(val uint64) {
	atomic.AddUint64(&tc.count, val)
}

type testValueRecorder struct {
	values []uint64
	lock   sync.Mutex
}

func (tvr *testValueRecorder) RecordValue(val uint64) {
	tvr.lock.Lock()
	tvr.values = append(tvr.values, val)
	tvr.lock.Unlock()
}

type testMeter struct {
	lock      sync.Mutex
	counters  map[string]*testCounter
	recorders map[string]*testValueRecorder
}

func newTestMeter() *testMeter {
	return &testMeter{
		counters:  make(map[string]*testCounter),
		recorders: make(map[string]*testValueRecorder),
	}
}

func (tm *testMeter) Reset() {
	tm.lock.Lock()
	tm.counters = make(map[string]*testCounter)
	tm.recorders = make(map[string]*testValueRecorder)
	tm.lock.Unlock()
}

func (tm *testMeter) Counter(name string, tags map[string]string) (Counter, error) {
	key := tags["db.operation"]
	tm.lock.Lock()
	counter := tm.counters[key]
	if counter == nil {
		counter = &testCounter{}
		tm.counters[key] = counter
	}
	tm.lock.Unlock()
	return counter, nil
}

func (tm *testMeter) ValueRecorder(name string, tags map[string]string) (ValueRecorder, error) {
	key := tags["db.couchbase.service"]
	if op, ok := tags["db.operation"]; ok {
		key = key + ":" + op
	}
	tm.lock.Lock()
	recorder := tm.recorders[key]
	if recorder == nil {
		recorder = &testValueRecorder{}
		tm.recorders[key] = recorder
	}
	tm.lock.Unlock()
	return recorder, nil
}

func makeMetricsKey(service, op string) string {
	key := service
	if op != "" {
		key = key + ":" + op
	}

	return key
}
