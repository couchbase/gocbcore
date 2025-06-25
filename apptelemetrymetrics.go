package gocbcore

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	kvNonDurableThresholds = []time.Duration{
		1 * time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second, 2500 * time.Millisecond,
	}

	kvDurableThresholds = []time.Duration{
		10 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second, 10 * time.Second,
	}

	httpThresholds = []time.Duration{
		100 * time.Millisecond, 1 * time.Second, 10 * time.Second, 30 * time.Second, 75 * time.Second,
	}
)

type telemetryStore interface {
	serialize() string
	recordOperationCompletion(telemetryOutcome, telemetryOperationAttributes)
	recordOrphanedResponse(telemetryOperationAttributes)
}

type telemetryHistogramBin struct {
	lowerBound *time.Duration
	upperBound *time.Duration

	count *uint64
}

func (b *telemetryHistogramBin) IsWithin(duration time.Duration) bool {
	if b.lowerBound == nil && b.upperBound == nil {
		return true
	}
	if b.lowerBound == nil {
		return duration < *b.upperBound
	}
	if b.upperBound == nil {
		return *b.lowerBound <= duration
	}
	return *b.lowerBound <= duration && duration < *b.upperBound
}

func (b *telemetryHistogramBin) Increment() {
	atomic.AddUint64(b.count, 1)
}

type telemetryHistogram struct {
	bins []telemetryHistogramBin

	sumMicros *uint64
}

func newTelemetryHistogram(thresholds []time.Duration) *telemetryHistogram {
	h := &telemetryHistogram{
		sumMicros: new(uint64),
	}

	h.bins = append(h.bins, telemetryHistogramBin{
		lowerBound: nil,
		upperBound: &thresholds[0],
		count:      new(uint64),
	})
	for idx := range thresholds {
		if idx == len(thresholds)-1 {
			h.bins = append(h.bins, telemetryHistogramBin{
				lowerBound: &thresholds[idx],
				upperBound: nil,
				count:      new(uint64),
			})
		} else {
			h.bins = append(h.bins, telemetryHistogramBin{
				lowerBound: &thresholds[idx],
				upperBound: &thresholds[idx+1],
				count:      new(uint64),
			})
		}
	}
	return h
}

func (h *telemetryHistogram) AddValue(value time.Duration) {
	for _, bin := range h.bins {
		if bin.IsWithin(value) {
			bin.Increment()
		}
	}
	atomic.AddUint64(h.sumMicros, uint64(value.Microseconds()))
}

func (h *telemetryHistogram) serialize(histogramName, commonTags string) []string {
	var entries []string

	var cumCount uint64
	for _, bin := range h.bins {
		cumCount += *bin.count
		var encodedUpperBound string
		if bin.upperBound != nil {
			encodedUpperBound = strconv.FormatInt(bin.upperBound.Milliseconds(), 10)
		} else {
			encodedUpperBound = "+Inf"
		}
		entries = append(entries, fmt.Sprintf("sdk_%s_duration_milliseconds_bucket{le=\"%s\",%s} %d", histogramName, encodedUpperBound, commonTags, cumCount))
	}

	entries = append(entries,
		fmt.Sprintf("sdk_%s_duration_milliseconds_count{%s} %d", histogramName, commonTags, cumCount),
		fmt.Sprintf("sdk_%s_duration_milliseconds_sum{%s} %d", histogramName, commonTags, *h.sumMicros/1000),
	)

	return entries
}

type telemetryCounters struct {
	all      *telemetryCounterMap
	timeout  *telemetryCounterMap
	canceled *telemetryCounterMap
}

func newTelemetryCounters() *telemetryCounters {
	return &telemetryCounters{
		all:      newTelemetryCounterMap(),
		timeout:  newTelemetryCounterMap(),
		canceled: newTelemetryCounterMap(),
	}
}

func (c *telemetryCounters) recordOp(outcome telemetryOutcome, attributes telemetryOperationAttributes) {
	key := telemetryCounterKey{
		agent:    attributes.agent,
		service:  attributes.service,
		nodeUUID: attributes.nodeUUID,
		node:     attributes.node,
		altNode:  attributes.altNode,
		bucket:   attributes.bucket,
	}

	switch outcome {
	case telemetryOutcomeTimedout:
		c.timeout.Increment(key)
	case telemetryOutcomeCanceled:
		c.canceled.Increment(key)
	default:
		break
	}
	c.all.Increment(key)
}

func (c *telemetryCounters) serialize() []string {
	var entries []string

	entries = append(entries, c.all.serialize("total")...)
	entries = append(entries, c.timeout.serialize("timedout")...)
	entries = append(entries, c.canceled.serialize("canceled")...)

	return entries
}

type telemetryHistograms struct {
	kvRetrieval          *telemetryHistogramMap
	kvNonDurableMutation *telemetryHistogramMap
	kvDurableMutation    *telemetryHistogramMap
	query                *telemetryHistogramMap
	search               *telemetryHistogramMap
	analytics            *telemetryHistogramMap
	eventing             *telemetryHistogramMap
	management           *telemetryHistogramMap
}

func newTelemetryHistograms() *telemetryHistograms {
	return &telemetryHistograms{
		kvRetrieval:          newTelemetryHistogramMap(kvNonDurableThresholds, "kv_retrieval"),
		kvNonDurableMutation: newTelemetryHistogramMap(kvNonDurableThresholds, "kv_mutation_nondurable"),
		kvDurableMutation:    newTelemetryHistogramMap(kvDurableThresholds, "kv_mutation_durable"),
		query:                newTelemetryHistogramMap(httpThresholds, "query"),
		search:               newTelemetryHistogramMap(httpThresholds, "search"),
		analytics:            newTelemetryHistogramMap(httpThresholds, "analytics"),
		eventing:             newTelemetryHistogramMap(httpThresholds, "eventing"),
		management:           newTelemetryHistogramMap(httpThresholds, "management"),
	}
}

func (h *telemetryHistograms) serialize() []string {
	var entries []string

	entries = append(entries, h.kvRetrieval.serialize()...)
	entries = append(entries, h.kvDurableMutation.serialize()...)
	entries = append(entries, h.kvNonDurableMutation.serialize()...)
	entries = append(entries, h.query.serialize()...)
	entries = append(entries, h.search.serialize()...)
	entries = append(entries, h.analytics.serialize()...)
	entries = append(entries, h.management.serialize()...)
	entries = append(entries, h.eventing.serialize()...)

	return entries
}

func (h *telemetryHistograms) recordOp(attributes telemetryOperationAttributes) {
	key := telemetryHistogramKey{
		agent:    attributes.agent,
		node:     attributes.node,
		altNode:  attributes.altNode,
		nodeUUID: attributes.nodeUUID,
		bucket:   attributes.bucket,
	}

	switch attributes.service {
	case MemdService:
		if attributes.mutation {
			if attributes.durable {
				h.kvDurableMutation.AddValue(key, attributes.duration)
			} else {
				h.kvNonDurableMutation.AddValue(key, attributes.duration)
			}
		} else {
			h.kvRetrieval.AddValue(key, attributes.duration)
		}
	case N1qlService:
		h.query.AddValue(key, attributes.duration)
	case FtsService:
		h.search.AddValue(key, attributes.duration)
	case CbasService:
		h.analytics.AddValue(key, attributes.duration)
	case MgmtService:
		h.management.AddValue(key, attributes.duration)
	}
}

type telemetryCounterKey struct {
	agent    string
	service  ServiceType
	node     string
	altNode  string
	nodeUUID string

	// KV-only
	bucket string
}

func (k *telemetryCounterKey) serviceAsString() string {
	switch k.service {
	case MemdService:
		return "kv"
	case N1qlService:
		return "query"
	case FtsService:
		return "search"
	case CbasService:
		return "analytics"
	case MgmtService:
		return "management"
	case EventingService:
		return "eventing"
	}
	return ""
}

type telemetryHistogramKey struct {
	agent    string
	node     string
	altNode  string
	nodeUUID string

	// KV-only
	bucket string
}

type telemetryHistogramMap struct {
	thresholds      []time.Duration
	histograms      map[telemetryHistogramKey]*telemetryHistogram
	histogramsMutex sync.Mutex
	name            string
}

func newTelemetryHistogramMap(thresholds []time.Duration, name string) *telemetryHistogramMap {
	return &telemetryHistogramMap{
		thresholds: thresholds,
		histograms: make(map[telemetryHistogramKey]*telemetryHistogram),
		name:       name,
	}
}

func (m *telemetryHistogramMap) AddValue(key telemetryHistogramKey, value time.Duration) {
	var hist *telemetryHistogram

	m.histogramsMutex.Lock()
	hist, ok := m.histograms[key]
	if !ok {
		hist = newTelemetryHistogram(m.thresholds)
		m.histograms[key] = hist
	}
	m.histogramsMutex.Unlock()

	hist.AddValue(value)
}

func (m *telemetryHistogramMap) serialize() []string {
	var entries []string

	for k, h := range m.histograms {
		commonTags := fmt.Sprintf("agent=\"%s\",node=\"%s\",node_uuid=\"%s\"", k.agent, k.node, k.nodeUUID)
		if k.bucket != "" {
			commonTags += fmt.Sprintf(",bucket=\"%s\"", k.bucket)
		}
		if k.altNode != "" {
			commonTags += fmt.Sprintf(",alt_node=\"%s\"", k.altNode)
		}
		entries = append(entries, h.serialize(m.name, commonTags)...)
	}

	return entries
}

type telemetryCounterMap struct {
	counters      map[telemetryCounterKey]*uint64
	countersMutex sync.Mutex
}

func (m *telemetryCounterMap) Increment(key telemetryCounterKey) {
	var counter *uint64

	m.countersMutex.Lock()
	counter, ok := m.counters[key]
	if !ok {
		counter = new(uint64)
		m.counters[key] = counter
	}
	m.countersMutex.Unlock()

	atomic.AddUint64(counter, 1)
}

func (m *telemetryCounterMap) serialize(counterName string) []string {
	var entries []string

	for k, v := range m.counters {
		tags := fmt.Sprintf("agent=\"%s\",node=\"%s\",node_uuid=\"%s\"", k.agent, k.node, k.nodeUUID)
		if k.service == MemdService {
			tags += fmt.Sprintf(",bucket=\"%s\"", k.bucket)
		}
		if k.altNode != "" {
			tags += fmt.Sprintf(",alt_node=\"%s\"", k.altNode)
		}
		entries = append(entries, fmt.Sprintf("sdk_%s_r_%s{%s} %d", k.serviceAsString(), counterName, tags, *v))
	}

	return entries
}

func newTelemetryCounterMap() *telemetryCounterMap {
	return &telemetryCounterMap{
		counters: make(map[telemetryCounterKey]*uint64),
	}
}

type telemetryMetrics struct {
	counters   atomic.Pointer[telemetryCounters]
	histograms atomic.Pointer[telemetryHistograms]
}

func newTelemetryMetrics() *telemetryMetrics {
	tm := &telemetryMetrics{}

	tm.counters.Store(newTelemetryCounters())
	tm.histograms.Store(newTelemetryHistograms())

	return tm
}

func (tm *telemetryMetrics) serialize() string {
	counters := tm.counters.Swap(newTelemetryCounters())
	histograms := tm.histograms.Swap(newTelemetryHistograms())

	var entries []string
	entries = append(entries, counters.serialize()...)
	entries = append(entries, histograms.serialize()...)

	return strings.Join(entries, "\n")
}

func (tm *telemetryMetrics) recordOperationCompletion(outcome telemetryOutcome, attributes telemetryOperationAttributes) {
	if !attributes.IsValid() {
		return
	}

	tm.counters.Load().recordOp(outcome, attributes)
	if outcome == telemetryOutcomeSuccess {
		tm.histograms.Load().recordOp(attributes)
	}
}

func (tm *telemetryMetrics) recordOrphanedResponse(attributes telemetryOperationAttributes) {
	if !attributes.IsValid() {
		return
	}

	// Counters have already been updated for this operation. When receiving an orphaned response, we only need to
	// record the latency.
	tm.histograms.Load().recordOp(attributes)
}

type telemetryOutcome uint8

const (
	telemetryOutcomeSuccess telemetryOutcome = iota
	telemetryOutcomeCanceled
	telemetryOutcomeTimedout
	telemetryOutcomeError
)

type telemetryOperationAttributes struct {
	duration time.Duration

	nodeUUID string
	node     string
	altNode  string

	agent   string
	service ServiceType

	// KV-only
	bucket   string
	durable  bool
	mutation bool
}

func (a *telemetryOperationAttributes) IsValid() bool {
	switch a.service {
	case MemdService, N1qlService, FtsService, CbasService, MgmtService, EventingService:
		return true
	default:
		return false
	}
}
