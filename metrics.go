package gocbcore

// Meter handles metrics information for SDK operations.
type Meter interface {
	Counter(name string, tags map[string]string) (Counter, error)
	ValueRecorder(name string, tags map[string]string) (ValueRecorder, error)
}

// Counter is used for incrementing a synchronous count metric.
type Counter interface {
	IncrementBy(num uint64)
}

// ValueRecorder is used for grouping synchronous count metrics.
type ValueRecorder interface {
	RecordValue(val uint64)
}

// noopMeter is a Meter implementation which performs no metrics operations.
type noopMeter struct {
}

var (
	defaultNoopCounter       = &noopCounter{}
	defaultNoopValueRecorder = &noopValueRecorder{}
)

// Counter is used for incrementing a synchronous count metric.
func (nm noopMeter) Counter(name string, tags map[string]string) (Counter, error) {
	return defaultNoopCounter, nil
}

// ValueRecorder is used for grouping synchronous count metrics.
func (nm noopMeter) ValueRecorder(name string, tags map[string]string) (ValueRecorder, error) {
	return defaultNoopValueRecorder, nil
}

type noopCounter struct{}

func (bc *noopCounter) IncrementBy(num uint64) {
}

type noopValueRecorder struct{}

func (bc *noopValueRecorder) RecordValue(val uint64) {
}
