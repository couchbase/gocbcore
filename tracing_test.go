package gocbcore

import (
	"github.com/couchbase/gocbcore/v9/memd"
)

type testSpanContext struct {
	Name string
}

type testSpan struct {
	Name          string
	Tags          map[string]interface{}
	Finished      bool
	ParentContext RequestSpanContext
	Spans         map[RequestSpanContext][]*testSpan
}

func (ts *testSpan) Finish() {
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

func (ts *testSpan) SetTag(key string, value interface{}) RequestSpan {
	ts.Tags[key] = value
	return ts
}

type testTracer struct {
	Spans map[RequestSpanContext][]*testSpan
}

func newTestTracer() *testTracer {
	return &testTracer{
		Spans: make(map[RequestSpanContext][]*testSpan),
	}
}

func (tt *testTracer) StartSpan(operationName string, parentContext RequestSpanContext) RequestSpan {
	// CCCP looper will send us spans which will mess with our trace verifications.
	if operationName == memd.CmdGetClusterConfig.Name() || (operationName == "dispatch_to_server" && parentContext == nil) {
		return &noopSpan{}
	}

	span := newTestSpan(operationName, parentContext)
	if parentContext == nil {
		tt.Spans[parentContext] = append(tt.Spans[parentContext], span)
	} else {
		ctx := parentContext.(map[RequestSpanContext][]*testSpan)
		ctx[operationName] = append(ctx[operationName], span)
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
	suite.Assert().Equal(3, len(span.Tags))
	suite.Assert().True(span.Finished)
	suite.Assert().Equal("couchbase-go-sdk", span.Tags["component"])
	suite.Assert().Equal(bucketName, span.Tags["db.instance"])
	suite.Assert().Equal("client", span.Tags["span.kind"])
}

func (suite *StandardTestSuite) AssertCmdSpansEq(parents map[RequestSpanContext][]*testSpan, cmdName string,
	num int, docID string) {
	spans := parents[cmdName]
	if suite.Assert().Equal(num, len(spans)) {
		for i := 0; i < num; i++ {
			suite.AssertCmdSpan(spans[i], cmdName, uint32(i), docID)
		}
	}
}

func (suite *StandardTestSuite) AssertCmdSpansGE(parents map[RequestSpanContext][]*testSpan, cmdName string,
	num int, docID string) {
	spans := parents[cmdName]
	if suite.Assert().GreaterOrEqual(num, len(spans)) {
		for i := 0; i < len(spans); i++ {
			suite.AssertCmdSpan(spans[i], cmdName, uint32(i), docID)
		}
	}
}

func (suite *StandardTestSuite) AssertCmdSpan(span *testSpan, expectedName string, retries uint32, docID string) {
	suite.Assert().Equal(expectedName, span.Name)
	suite.Assert().Equal(1, len(span.Tags))
	suite.Assert().True(span.Finished)
	suite.Assert().Equal(retries, span.Tags["retry"])

	suite.AssertNetSpansEq(span.Spans, docID, 1)
}

func (suite *StandardTestSuite) AssertNetSpansEq(parents map[RequestSpanContext][]*testSpan, docID string, num int) {
	spans := parents["rpc"]
	if suite.Assert().Equal(num, num) {
		for i := 0; i < len(spans); i++ {
			suite.AssertNetSpan(spans[i], docID)
		}
	}
}

func (suite *StandardTestSuite) AssertNetSpan(span *testSpan, docID string) {
	suite.Assert().Equal("rpc", span.Name)
	suite.Assert().Equal(6, len(span.Tags))
	suite.Assert().True(span.Finished)
	suite.Assert().Equal("client", span.Tags["span.kind"])
	suite.Assert().NotEmpty(span.Tags["couchbase.operation_id"])
	suite.Assert().NotEmpty(span.Tags["couchbase.local_id"])
	suite.Assert().NotEmpty(span.Tags["local.address"])
	suite.Assert().NotEmpty(span.Tags["peer.address"])
	suite.Assert().Equal(docID, span.Tags["couchbase.document_key"])
}

func (suite *StandardTestSuite) AssertHTTPSpan(span *testSpan, expectedName string) {
	suite.Assert().Equal(expectedName, span.Name)
	suite.Assert().Equal(3, len(span.Tags))
	suite.Assert().True(span.Finished)
	suite.Assert().Equal("couchbase-go-sdk", span.Tags["component"])
	suite.Assert().Empty(span.Tags["db.instance"])
	suite.Assert().Equal("client", span.Tags["span.kind"])
}

func (suite *StandardTestSuite) AssertDispatchSpansEq(parents map[RequestSpanContext][]*testSpan, num int) {
	spans := parents["dispatch_to_server"]
	if suite.Assert().Equal(num, len(spans)) {
		for i := 0; i < len(spans); i++ {
			span := spans[i]
			suite.Assert().Equal("dispatch_to_server", span.Name)
			suite.Assert().Equal(1, len(span.Tags))
			suite.Assert().True(span.Finished)
			suite.Assert().Equal(uint32(i), span.Tags["retry"])
		}
	}
}
