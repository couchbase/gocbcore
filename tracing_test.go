package gocbcore

import (
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/stretchr/testify/mock"
	"time"
)

type testSpan struct {
	Name          string
	Tags          map[string]interface{}
	Finished      bool
	ParentContext RequestSpanContext
	Spans         map[RequestSpanContext][]*testSpan
}

func (ts *testSpan) End() {
	ts.Finished = true
}

func (ts *testSpan) Context() RequestSpanContext {
	return ts.Spans
}

func newTestSpan(operationName string, parentContext RequestSpanContext) *testSpan {
	return &testSpan{
		Name:          operationName,
		Tags:          make(map[string]interface{}),
		ParentContext: parentContext,
		Spans:         make(map[RequestSpanContext][]*testSpan),
	}
}

func (ts *testSpan) SetAttribute(key string, value interface{}) {
	ts.Tags[key] = value
}

func (ts *testSpan) AddEvent(key string, timestamp time.Time) {
}

type testTracer struct {
	Spans map[RequestSpanContext][]*testSpan
}

func newTestTracer() *testTracer {
	return &testTracer{
		Spans: make(map[RequestSpanContext][]*testSpan),
	}
}

func (tt *testTracer) RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan {
	// CCCP looper will send us spans which will mess with our trace verifications.
	if operationName == memd.CmdGetClusterConfig.Name() || (operationName == spanNameDispatchToServer && parentContext == nil) {
		return &noopSpan{}
	}

	span := newTestSpan(operationName, parentContext)

	if parentContext == nil {
		tt.Spans[parentContext] = append(tt.Spans[parentContext], span)
	} else {
		ctx, ok := parentContext.(map[RequestSpanContext][]*testSpan)
		if ok {
			ctx[operationName] = append(ctx[operationName], span)
		} else {
			tt.Spans[parentContext] = append(tt.Spans[parentContext], span)
		}
	}

	return span
}

func (tt *testTracer) Reset() {
	tt.Spans = make(map[RequestSpanContext][]*testSpan)
}

func (suite *StandardTestSuite) AssertOpSpan(span *testSpan, expectedName, bucketName, cmdName string, numCmdSpans int,
	atLeastNumCmdSpans bool, docID string) {
	suite.AssertTopLevelSpan(span, expectedName, bucketName)

	if atLeastNumCmdSpans {
		suite.AssertCmdSpansGE(span.Spans, cmdName, numCmdSpans, docID)
	} else {
		suite.AssertCmdSpansEq(span.Spans, cmdName, numCmdSpans, docID)
	}
}

func (suite *StandardTestSuite) AssertTopLevelSpan(span *testSpan, expectedName, bucketName string) {
	suite.Assert().Equal(expectedName, span.Name)
	numTags := 1
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	if suite.SupportsFeature(TestFeatureClusterLabels) {
		suite.Assert().Equal("test-cluster", span.Tags["db.couchbase.cluster_name"])
		suite.Assert().NotEmpty(span.Tags["db.couchbase.cluster_uuid"])
		numTags += 2
	}
	suite.Assert().Equal(numTags, len(span.Tags))
	suite.Assert().True(span.Finished)
}

func (suite *StandardTestSuite) AssertCmdSpansEq(parents map[RequestSpanContext][]*testSpan, cmdName string,
	num int, docID string) {
	spans := parents[cmdName]
	if suite.Assert().Equal(num, len(spans)) {
		for i := 0; i < num; i++ {
			suite.AssertCmdSpan(spans[i], cmdName)
		}
	}
}

func (suite *StandardTestSuite) AssertCmdSpansGE(parents map[RequestSpanContext][]*testSpan, cmdName string,
	num int, docID string) {
	spans := parents[cmdName]
	if suite.Assert().GreaterOrEqual(num, len(spans)) {
		for i := 0; i < len(spans); i++ {
			suite.AssertCmdSpan(spans[i], cmdName)
		}
	}
}

func (suite *StandardTestSuite) AssertCmdSpan(span *testSpan, expectedName string) {
	suite.Assert().Equal(expectedName, span.Name)
	suite.Assert().True(span.Finished)
	numTags := 2
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	if suite.SupportsFeature(TestFeatureClusterLabels) {
		suite.Assert().Equal("test-cluster", span.Tags["db.couchbase.cluster_name"])
		suite.Assert().NotEmpty(span.Tags["db.couchbase.cluster_uuid"])
		numTags += 2
	}
	suite.Assert().Equal(numTags, len(span.Tags))
	suite.Assert().Contains(span.Tags, "db.couchbase.retries")
	suite.AssertNetSpansEq(span.Spans, 1)
}

func (suite *StandardTestSuite) AssertNetSpansEq(parents map[RequestSpanContext][]*testSpan, num int) {
	spans := parents[spanNameDispatchToServer]
	if suite.Assert().Equal(num, num) {
		for i := 0; i < len(spans); i++ {
			suite.AssertNetSpan(spans[i])
		}
	}
}

func (suite *StandardTestSuite) AssertNetSpan(span *testSpan) {
	suite.Assert().Equal(spanNameDispatchToServer, span.Name)
	numTags := 8
	if duration, ok := span.Tags["db.couchbase.server_duration"]; ok {
		suite.Assert().NotZero(duration)
		numTags++
	}
	suite.Assert().True(span.Finished)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	if suite.SupportsFeature(TestFeatureClusterLabels) {
		suite.Assert().Equal("test-cluster", span.Tags["db.couchbase.cluster_name"])
		suite.Assert().NotEmpty(span.Tags["db.couchbase.cluster_uuid"])
		numTags += 2
	}
	suite.Assert().Equal("IP.TCP", span.Tags["net.transport"])
	suite.Assert().NotEmpty(span.Tags["db.couchbase.operation_id"])
	suite.Assert().NotEmpty(span.Tags["db.couchbase.local_id"])
	suite.Assert().NotEmpty(span.Tags["net.host.name"])
	suite.Assert().NotEmpty(span.Tags["net.host.port"])
	suite.Assert().NotEmpty(span.Tags["net.peer.name"])
	suite.Assert().NotEmpty(span.Tags["net.peer.port"])
	suite.Assert().Equal(numTags, len(span.Tags))
}

func (suite *StandardTestSuite) AssertHTTPSpan(span *testSpan, expectedName string) {
	suite.Assert().Equal(expectedName, span.Name)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	numTags := 1
	if suite.SupportsFeature(TestFeatureClusterLabels) {
		suite.Assert().Equal("test-cluster", span.Tags["db.couchbase.cluster_name"])
		suite.Assert().NotEmpty(span.Tags["db.couchbase.cluster_uuid"])
		numTags += 2
	}
	suite.Assert().Equal(numTags, len(span.Tags))
	suite.Assert().True(span.Finished)

	childSpans := span.Spans[spanNameDispatchToServer]
	suite.Require().GreaterOrEqual(len(childSpans), 1)

	dispatchSpan := childSpans[0]
	numTagsDispatchSpan := 6
	suite.Assert().True(dispatchSpan.Finished)
	suite.Assert().Equal("couchbase", dispatchSpan.Tags["db.system"])
	if suite.SupportsFeature(TestFeatureClusterLabels) {
		suite.Assert().Equal("test-cluster", span.Tags["db.couchbase.cluster_name"])
		suite.Assert().NotEmpty(span.Tags["db.couchbase.cluster_uuid"])
		numTagsDispatchSpan += 2
	}
	suite.Assert().Equal(numTagsDispatchSpan, len(dispatchSpan.Tags))
	suite.Assert().Equal("IP.TCP", dispatchSpan.Tags["net.transport"])
	suite.Assert().NotEmpty(dispatchSpan.Tags["db.couchbase.operation_id"])
	suite.Assert().NotEmpty(dispatchSpan.Tags["net.peer.name"])
	suite.Assert().NotEmpty(dispatchSpan.Tags["net.peer.port"])
	suite.Assert().Contains(dispatchSpan.Tags, "db.couchbase.retries")
}

func (suite *StandardTestSuite) AssertHTTPSpanNoDispatch(span *testSpan, expectedName string) {
	suite.Assert().Equal(expectedName, span.Name)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	numTags := 1
	if suite.SupportsFeature(TestFeatureClusterLabels) {
		suite.Assert().Equal("test-cluster", span.Tags["db.couchbase.cluster_name"])
		suite.Assert().NotEmpty(span.Tags["db.couchbase.cluster_uuid"])
		numTags += 2
	}
	suite.Assert().Equal(numTags, len(span.Tags))
	suite.Assert().True(span.Finished)

	_, ok := span.Spans[spanNameDispatchToServer]
	suite.Assert().False(ok)
}

func (suite *StandardTestSuite) TestBasicOpsTracingParentNoRoot() {
	cfg := makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	cfg.TracerConfig.NoRootTraceSpans = true
	tracer := newTestTracer()
	cfg.TracerConfig.Tracer = tracer
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestBasicOpsTracingParentNoRoot", suite.CollectionName, suite.ScopeName)

	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testtracerparentnoroot"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		TraceContext:   "set_parent",
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(tracer.Spans, "set_parent") {
		parents := tracer.Spans["set_parent"]
		if suite.Assert().Equal(1, len(parents)) {
			suite.AssertCmdSpan(parents[0], memd.CmdSet.Name())
		}
	}
}

func (suite *StandardTestSuite) TestBasicOpsTracingParentRoot() {
	cfg := makeAgentConfig(globalTestConfig)
	cfg.BucketName = globalTestConfig.BucketName
	tracer := newTestTracer()
	cfg.TracerConfig.Tracer = tracer
	agent, err := CreateAgent(&cfg)
	suite.Require().Nil(err, err)
	defer agent.Close()
	s := suite.GetHarness()

	suite.VerifyConnectedToBucket(agent, s, "TestBasicOpsTracingParentRoot", suite.CollectionName, suite.ScopeName)

	// Set
	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("testtracerparentroot"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
		TraceContext:   "set_parent",
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	if suite.Assert().Contains(tracer.Spans, "set_parent") {
		parents := tracer.Spans["set_parent"]
		if suite.Assert().Equal(1, len(parents)) {
			suite.AssertOpSpan(parents[0], "Set", agent.BucketName(), memd.CmdSet.Name(), 1, false, "test")
		}
	}
}

func (suite *StandardTestSuite) TestTracerComponentTracksClusterLabels() {
	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.tracerComponent"))

	tc := newTracerComponent(&noopTracer{}, "", true, &noopMeter{}, cfgMgr)

	suite.Assert().Empty(tc.ClusterLabels().ClusterName)
	suite.Assert().Empty(tc.ClusterLabels().ClusterUUID)

	tc.OnNewRouteConfig(&routeConfig{
		clusterName: "test-cluster",
		clusterUUID: "48d5d855660452102a8c279dc6155e01",
	})

	suite.Assert().Equal("test-cluster", tc.ClusterLabels().ClusterName)
	suite.Assert().Equal("48d5d855660452102a8c279dc6155e01", tc.ClusterLabels().ClusterUUID)
}
