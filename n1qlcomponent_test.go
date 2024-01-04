package gocbcore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/mock"
)

type n1qlTestHelper struct {
	TestName      string
	NumDocs       int
	QueryTestDocs *testDocs
	suite         *StandardTestSuite
}

func hlpRunQuery(t *testing.T, agent *AgentGroup, opts N1QLQueryOptions) ([][]byte, error) {
	t.Helper()

	resCh := make(chan *N1QLRowReader, 1)
	errCh := make(chan error, 1)
	_, err := agent.N1QLQuery(opts, func(reader *N1QLRowReader, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- reader
	})
	if err != nil {
		return nil, err
	}

	var rows *N1QLRowReader
	select {
	case err := <-errCh:
		return nil, err
	case res := <-resCh:
		rows = res
	}

	var rowBytes [][]byte
	for {
		row := rows.NextRow()
		if row == nil {
			break
		}

		rowBytes = append(rowBytes, row)
	}

	err = rows.Err()
	return rowBytes, err
}

func hlpEnsurePrimaryIndex(t *testing.T, agent *AgentGroup, bucketName string) {
	t.Helper()

	payloadStr := fmt.Sprintf(`{"statement":"CREATE PRIMARY INDEX ON %s"}`, bucketName)
	hlpRunQuery(t, agent, N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(5000 * time.Millisecond),
	})
}

func (nqh *n1qlTestHelper) testSetupN1ql(t *testing.T) {
	agent := nqh.suite.DefaultAgent()
	ag := nqh.suite.AgentGroup()

	nqh.QueryTestDocs = makeTestDocs(t, agent, nqh.TestName, nqh.NumDocs)

	hlpEnsurePrimaryIndex(t, ag, nqh.suite.BucketName)
}

func (nqh *n1qlTestHelper) testCleanupN1ql(t *testing.T) {
	if nqh.QueryTestDocs != nil {
		nqh.QueryTestDocs.Remove()
		nqh.QueryTestDocs = nil
	}
}

func (nqh *n1qlTestHelper) testN1QLBasic(t *testing.T) {
	ag := nqh.suite.AgentGroup()

	deadline := time.Now().Add(15000 * time.Millisecond)
	runTestQuery := func() ([]testDoc, error) {
		test := map[string]interface{}{
			"statement":         fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"%s\"", nqh.suite.BucketName, nqh.TestName),
			"client_context_id": "12345",
		}
		payload, err := json.Marshal(test)
		if err != nil {
			nqh.suite.T().Errorf("failed to marshal test payload: %s", err)
		}

		iterDeadline := time.Now().Add(5000 * time.Millisecond)
		if iterDeadline.After(deadline) {
			iterDeadline = deadline
		}

		resCh := make(chan *N1QLRowReader)
		errCh := make(chan error)
		_, err = ag.N1QLQuery(N1QLQueryOptions{
			Payload:       payload,
			RetryStrategy: nil,
			Deadline:      iterDeadline,
		}, func(reader *N1QLRowReader, err error) {
			if err != nil {
				errCh <- err
				return
			}
			resCh <- reader
		})
		if err != nil {
			return nil, err
		}
		var rows *N1QLRowReader
		select {
		case err := <-errCh:
			return nil, err
		case res := <-resCh:
			rows = res
		}

		var docs []testDoc
		for {
			row := rows.NextRow()
			if row == nil {
				break
			}

			var doc testDoc
			err := json.Unmarshal(row, &doc)
			if err != nil {
				return nil, err
			}

			docs = append(docs, doc)
		}

		err = rows.Err()
		if err != nil {
			return nil, err
		}

		return docs, nil
	}

	lastError := ""
	for {
		docs, err := runTestQuery()
		if err == nil {
			testFailed := false

			for _, doc := range docs {
				if doc.I < 1 || doc.I > nqh.NumDocs {
					lastError = fmt.Sprintf("query test read invalid row i=%d", doc.I)
					testFailed = true
				}
			}

			numDocs := len(docs)
			if numDocs != nqh.NumDocs {
				lastError = fmt.Sprintf("query test read invalid number of rows %d!=%d", numDocs, 5)
				testFailed = true
			}

			if !testFailed {
				break
			}
		}

		sleepDeadline := time.Now().Add(1000 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			nqh.suite.T().Errorf("timed out waiting for indexing: %s", lastError)
			break
		}
	}
}

func (nqh *n1qlTestHelper) testN1QLPrepared(t *testing.T) {
	ag := nqh.suite.AgentGroup()

	deadline := time.Now().Add(15000 * time.Millisecond)
	runTestQuery := func() ([]testDoc, error) {
		test := map[string]interface{}{
			"statement":         fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"%s\"", nqh.suite.BucketName, nqh.TestName),
			"client_context_id": "1234",
		}
		payload, err := json.Marshal(test)
		if err != nil {
			nqh.suite.T().Errorf("failed to marshal test payload: %s", err)
		}

		iterDeadline := time.Now().Add(5000 * time.Millisecond)
		if iterDeadline.After(deadline) {
			iterDeadline = deadline
		}

		resCh := make(chan *N1QLRowReader)
		errCh := make(chan error)
		_, err = ag.PreparedN1QLQuery(N1QLQueryOptions{
			Payload:       payload,
			RetryStrategy: nil,
			Deadline:      iterDeadline,
		}, func(reader *N1QLRowReader, err error) {
			if err != nil {
				errCh <- err
				return
			}
			resCh <- reader
		})
		if err != nil {
			return nil, err
		}

		var rows *N1QLRowReader
		select {
		case err := <-errCh:
			return nil, err
		case res := <-resCh:
			rows = res
		}

		var docs []testDoc
		for {
			row := rows.NextRow()
			if row == nil {
				break
			}

			var doc testDoc
			err := json.Unmarshal(row, &doc)
			if err != nil {
				return nil, err
			}

			docs = append(docs, doc)
		}

		err = rows.Err()
		if err != nil {
			return nil, err
		}

		return docs, nil
	}

	lastError := ""
	for {
		docs, err := runTestQuery()
		if err == nil {
			testFailed := false

			for _, doc := range docs {
				if doc.I < 1 || doc.I > nqh.NumDocs {
					lastError = fmt.Sprintf("query test read invalid row i=%d", doc.I)
					testFailed = true
				}
			}

			numDocs := len(docs)
			if numDocs != nqh.NumDocs {
				lastError = fmt.Sprintf("query test read invalid number of rows %d!=%d", numDocs, 5)
				testFailed = true
			}

			if !testFailed {
				break
			}
		}

		sleepDeadline := time.Now().Add(1000 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			nqh.suite.T().Errorf("timed out waiting for indexing: %s", lastError)
			break
		}
	}
}

func (suite *StandardTestSuite) TestN1QL() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	helper := &n1qlTestHelper{
		TestName: "testQuery",
		NumDocs:  5,
		suite:    suite,
	}

	suite.T().Run("setup", helper.testSetupN1ql)

	suite.tracer.Reset()

	suite.T().Run("Basic", helper.testN1QLBasic)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 1) {
			for i := 0; i < len(nilParents); i++ {
				suite.AssertHTTPSpan(nilParents[i], "N1QLQuery")
			}
		}
	}

	suite.VerifyMetrics(suite.meter, "n1ql:N1QLQuery", 1, true, false)

	suite.T().Run("cleanup", helper.testCleanupN1ql)
}

type roundTripper struct {
	delay  time.Duration
	tsport http.RoundTripper
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	<-time.After(rt.delay)
	return rt.tsport.RoundTrip(req)
}

func (suite *StandardTestSuite) TestN1QLCancel() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)
	agent := suite.DefaultAgent()

	rt := &roundTripper{delay: 1 * time.Second, tsport: agent.http.cli.Transport}
	httpCpt := newHTTPComponentWithClient(
		httpComponentProps{},
		&http.Client{Transport: rt},
		agent.httpMux,
		agent.tracer,
	)
	n1qlCpt := newN1QLQueryComponent(httpCpt, &configManagementComponent{}, &tracerComponent{tracer: suite.tracer, metrics: suite.meter})

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := `{"statement":"SELECT * FROM test","client_context_id":"12345"}`
	op, err := n1qlCpt.N1QLQuery(N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(5 * time.Second),
	}, func(reader *N1QLRowReader, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- reader
	})
	if err != nil {
		suite.T().Fatalf("Failed to execute query %s", err)
	}
	op.Cancel()

	var rows *N1QLRowReader
	var resErr error
	select {
	case err := <-errCh:
		resErr = err
	case res := <-resCh:
		rows = res
	}

	if rows != nil {
		suite.T().Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrRequestCanceled) {
		suite.T().Fatalf("Error should have been request canceled but was %s", resErr)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 1) {
			for i := 0; i < len(nilParents); i++ {
				suite.AssertHTTPSpan(nilParents[i], "N1QLQuery")
			}
		}
	}

	suite.VerifyMetrics(suite.meter, "n1ql:N1QLQuery", 1, true, false)
}

func (suite *StandardTestSuite) TestN1QLTimeout() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	ag := suite.AgentGroup()

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1","client_context_id":"12345"}`, suite.BucketName)
	_, err := ag.N1QLQuery(N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(1 * time.Microsecond),
	}, func(reader *N1QLRowReader, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- reader
	})
	if err != nil {
		suite.T().Fatalf("Failed to execute query %s", err)
	}

	var rows *N1QLRowReader
	var resErr error
	select {
	case err := <-errCh:
		resErr = err
	case res := <-resCh:
		rows = res
	}

	if rows != nil {
		suite.T().Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrTimeout) {
		suite.T().Fatalf("Error should have been request canceled but was %s", resErr)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(len(nilParents), 1) {
			span := nilParents[0]
			suite.Assert().Equal("N1QLQuery", span.Name)
			suite.Assert().Equal(1, len(span.Tags))
			suite.Assert().Equal("couchbase", span.Tags["db.system"])
			suite.Assert().True(span.Finished)

			_, ok := span.Spans[spanNameDispatchToServer]
			suite.Assert().False(ok)
		}
	}

	suite.VerifyMetrics(suite.meter, "n1ql:N1QLQuery", 1, true, false)
}

func (suite *StandardTestSuite) TestN1QLPrepared() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	helper := &n1qlTestHelper{
		TestName: "testPreparedQuery",
		NumDocs:  5,
		suite:    suite,
	}

	suite.T().Run("setup", helper.testSetupN1ql)

	suite.tracer.Reset()

	suite.T().Run("Basic", helper.testN1QLPrepared)

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 1) {
			for i := 0; i < len(nilParents); i++ {
				suite.AssertHTTPSpan(nilParents[i], "PreparedN1QLQuery")
			}
		}
	}

	suite.T().Run("cleanup", helper.testCleanupN1ql)
}

func (suite *StandardTestSuite) TestN1QLPreparedCancel() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	agent := suite.DefaultAgent()

	rt := &roundTripper{delay: 1 * time.Second, tsport: agent.http.cli.Transport}
	httpCpt := newHTTPComponentWithClient(
		httpComponentProps{},
		&http.Client{Transport: rt},
		agent.httpMux,
		agent.tracer,
	)
	n1qlCpt := newN1QLQueryComponent(httpCpt, &configManagementComponent{}, &tracerComponent{tracer: suite.tracer, metrics: suite.meter})

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := `{"statement":"SELECT * FROM test","client_context_id":"12345"}`
	op, err := n1qlCpt.PreparedN1QLQuery(N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(5 * time.Second),
	}, func(reader *N1QLRowReader, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- reader
	})
	if err != nil {
		suite.T().Fatalf("Failed to execute query %s", err)
	}
	op.Cancel()

	var rows *N1QLRowReader
	var resErr error
	select {
	case err := <-errCh:
		resErr = err
	case res := <-resCh:
		rows = res
	}

	if rows != nil {
		suite.T().Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrRequestCanceled) {
		suite.T().Fatalf("Error should have been request canceled but was %s", resErr)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().GreaterOrEqual(len(nilParents), 1) {
			for i := 0; i < len(nilParents); i++ {
				suite.AssertHTTPSpan(nilParents[i], "PreparedN1QLQuery")
			}
		}
	}

	suite.VerifyMetrics(suite.meter, "n1ql:PreparedN1QLQuery", 1, true, false)
}

func (suite *StandardTestSuite) TestN1QLPreparedTimeout() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	ag := suite.AgentGroup()

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1","client_context_id":"12345"}`, suite.BucketName)
	_, err := ag.PreparedN1QLQuery(N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(1 * time.Microsecond),
	}, func(reader *N1QLRowReader, err error) {
		if err != nil {
			errCh <- err
			return
		}
		resCh <- reader
	})
	if err != nil {
		suite.T().Fatalf("Failed to execute query %s", err)
	}

	var rows *N1QLRowReader
	var resErr error
	select {
	case err := <-errCh:
		resErr = err
	case res := <-resCh:
		rows = res
	}

	if rows != nil {
		suite.T().Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrTimeout) {
		suite.T().Fatalf("Error should have been request canceled but was %s", resErr)
	}

	if suite.Assert().Contains(suite.tracer.Spans, nil) {
		nilParents := suite.tracer.Spans[nil]
		if suite.Assert().Equal(len(nilParents), 1) {
			span := nilParents[0]
			suite.Assert().Equal("PreparedN1QLQuery", span.Name)
			suite.Assert().Equal(1, len(span.Tags))
			suite.Assert().Equal("couchbase", span.Tags["db.system"])
			suite.Assert().True(span.Finished)

			_, ok := span.Spans[spanNameDispatchToServer]
			suite.Assert().False(ok)
		}
	}

	suite.VerifyMetrics(suite.meter, "n1ql:PreparedN1QLQuery", 1, true, false)
}

func (suite *StandardTestSuite) TestN1QLErrorReasonDocumentExists() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)
	suite.EnsureSupportsFeature(TestFeatureN1qlReasons)

	agent, s := suite.GetAgentAndHarness()

	collection := suite.CollectionName
	if collection == "" {
		collection = "_default"
	}
	scope := suite.ScopeName
	if scope == "" {
		scope = "_default"
	}

	s.PushOp(agent.Set(SetOptions{
		Key:            []byte("n1qldocumentexists"),
		Value:          []byte("{}"),
		CollectionName: suite.CollectionName,
		ScopeName:      suite.ScopeName,
	}, func(res *StoreResult, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set returned error %v", err)
			}
			if res.Cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	payloadStr := fmt.Sprintf(
		`{"statement":"INSERT INTO %s.%s.%s (KEY, VALUE) VALUES (\"n1qldocumentexists\", {\"type\": \"hotel\"})"}`,
		suite.BucketName,
		scope,
		collection,
	)
	s.PushOp(agent.N1QLQuery(N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(10 * time.Second),
	}, func(reader *N1QLRowReader, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("N1QLQuery operation failed: %v", err)
			}

			for {
				row := reader.NextRow()
				if row == nil {
					break
				}
			}

			err = reader.Err()
			if !errors.Is(err, ErrDocumentExists) {
				s.Fatalf("N1QLQuery should failed with document exists, was: %v", err)
			}
		})
	}))
	s.Wait(0)
}

// TestN1QLErrorsAndResults tests the case where we receive both errors and results from the server meaning
// that we cannot immediately return an error and must surface it through Err instead.
func (suite *UnitTestSuite) TestN1QLErrorsAndResults() {
	d, err := suite.LoadRawTestDataset("query_rows_errors")
	suite.Require().Nil(err)

	r := ioutil.NopCloser(bytes.NewReader(d))
	resp := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    200,
		ContentLength: int64(len(d)),
		Body:          r,
	}

	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := new(mockHttpComponentInterface)
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp, nil)

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan *N1QLRowReader)
	_, err = n1qlC.N1QLQuery(N1QLQueryOptions{
		Payload: payload,
	}, func(reader *N1QLRowReader, err error) {
		suite.Require().Nil(err, err)
		waitCh <- reader
	})
	suite.Require().Nil(err, err)

	reader := <-waitCh

	numRows := 0
	for reader.NextRow() != nil {
		numRows++
	}
	suite.Assert().Zero(numRows)

	err = reader.Err()
	suite.Require().NotNil(err)

	suite.Assert().True(errors.Is(err, ErrCasMismatch))

	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(12009), firstErr.Code)
	suite.Assert().NotEmpty(firstErr.Message)
}

func (suite *UnitTestSuite) TestN1QLOldPreparedErrorsAndResults() {
	d, err := suite.LoadRawTestDataset("query_rows_errors")
	suite.Require().Nil(err)

	r := ioutil.NopCloser(bytes.NewReader(d))
	resp := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    200,
		ContentLength: int64(len(d)),
		Body:          r,
	}

	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := new(mockHttpComponentInterface)
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp, nil)

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan error)
	_, err = n1qlC.PreparedN1QLQuery(N1QLQueryOptions{
		Payload: payload,
	}, func(reader *N1QLRowReader, err error) {
		waitCh <- err
	})
	suite.Require().Nil(err, err)

	err = <-waitCh
	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(12009), firstErr.Code)
	suite.Assert().NotEmpty(firstErr.Message)
}

func (suite *UnitTestSuite) TestN1QLOldPreparedUnknownErrorsAndResults() {
	d, err := suite.LoadRawTestDataset("query_rows_unknown_errors")
	suite.Require().Nil(err)

	r := ioutil.NopCloser(bytes.NewReader(d))
	resp := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    200,
		ContentLength: int64(len(d)),
		Body:          r,
	}

	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := new(mockHttpComponentInterface)
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp, nil)

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan error)
	_, err = n1qlC.PreparedN1QLQuery(N1QLQueryOptions{
		Payload: payload,
	}, func(reader *N1QLRowReader, err error) {
		waitCh <- err
	})
	suite.Require().Nil(err, err)

	err = <-waitCh
	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(13014), firstErr.Code)
	suite.Assert().NotEmpty(firstErr.Message)
}

func (suite *UnitTestSuite) TestN1QLErrUnknownErrorsAndResults() {
	d, err := suite.LoadRawTestDataset("query_rows_unknown_errors")
	suite.Require().Nil(err)

	r := ioutil.NopCloser(bytes.NewReader(d))
	resp := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    200,
		ContentLength: int64(len(d)),
		Body:          r,
	}

	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := new(mockHttpComponentInterface)
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp, nil)

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan *N1QLRowReader)
	_, err = n1qlC.N1QLQuery(N1QLQueryOptions{
		Payload: payload,
	}, func(reader *N1QLRowReader, err error) {
		suite.Require().Nil(err, err)
		waitCh <- reader
	})
	suite.Require().Nil(err, err)

	reader := <-waitCh

	numRows := 0
	for reader.NextRow() != nil {
		numRows++
	}
	suite.Assert().Zero(numRows)

	err = reader.Err()
	suite.Require().NotNil(err)

	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(13014), firstErr.Code)
	suite.Assert().NotEmpty(firstErr.Message)
}

type readerAndError struct {
	reader *N1QLRowReader
	err    error
}

type n1qlHTTPComponent struct {
	Endpoint   string
	StatusCode int
	Body       []byte
}

func (nhc *n1qlHTTPComponent) DoInternalHTTPRequest(req *httpRequest, skipConfigCheck bool) (*HTTPResponse, error) {
	body := ioutil.NopCloser(bytes.NewReader(nhc.Body))
	return &HTTPResponse{
		Endpoint:      nhc.Endpoint,
		StatusCode:    nhc.StatusCode,
		Body:          body,
		ContentLength: int64(len(nhc.Body)),
	}, nil
}

func (suite *UnitTestSuite) doN1QLRequest(respData []byte, statusCode int, retryStrat RetryStrategy) readerAndError {
	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := &n1qlHTTPComponent{
		Endpoint:   "whatever",
		StatusCode: statusCode,
		Body:       respData,
	}

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan readerAndError)
	_, err = n1qlC.N1QLQuery(N1QLQueryOptions{
		Payload:       payload,
		RetryStrategy: retryStrat,
		Deadline:      time.Now().Add(1 * time.Second),
	}, func(reader *N1QLRowReader, err error) {
		waitCh <- readerAndError{reader: reader, err: err}
	})
	suite.Require().Nil(err, err)

	return <-waitCh
}

func (suite *UnitTestSuite) TestN1QLEnhPreparedKnownQueryRetryPrepare4050() {

	body := []byte(`{"errors":[{"code":4050,"msg":"Unrecognizable prepared statement"}]}`)

	r := ioutil.NopCloser(bytes.NewReader(body))
	resp := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    404,
		ContentLength: int64(len(body)),
		Body:          r,
	}

	body2 := []byte(`{"prepared":"somename","results":[]}`)

	r2 := ioutil.NopCloser(bytes.NewReader(body2))
	resp2 := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    200,
		ContentLength: int64(len(body2)),
		Body:          r2,
	}

	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := new(mockHttpComponentInterface)
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(0).(*httpRequest)
		var body map[string]interface{}
		suite.Require().NoError(json.Unmarshal(req.Body, &body))

		_, ok := body["statement"]
		suite.Assert().False(ok)

		prepared := body["prepared"]
		suite.Assert().Equal("somename", prepared)
	})
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp2, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(0).(*httpRequest)
		var body map[string]interface{}
		suite.Require().NoError(json.Unmarshal(req.Body, &body))

		statement := body["statement"]
		suite.Assert().Equal("PREPARE SELECT 1=1", statement)

		autoExec := body["auto_execute"]
		suite.Assert().True(autoExec.(bool))
	})

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	n1qlC.enhancedPreparedSupported = 1
	n1qlC.queryCache.Put(n1qlQueryCacheStatementContext{Statement: "SELECT 1=1"}, &n1qlQueryCacheEntry{
		name: "somename",
	})
	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan error, 1)
	_, err = n1qlC.PreparedN1QLQuery(N1QLQueryOptions{
		Payload:       payload,
		RetryStrategy: NewBestEffortRetryStrategy(nil),
		Deadline:      time.Now().Add(1 * time.Second),
	}, func(reader *N1QLRowReader, err error) {
		waitCh <- err
	})
	suite.Require().NoError(err, err)
	suite.Require().NoError(<-waitCh)
}

func (suite *UnitTestSuite) TestN1QLEnhPreparedKnownQueryFailReprepare() {
	body := []byte(`{"errors":[{"code":4050,"msg":"Unrecognizable prepared statement"}]}`)
	r := ioutil.NopCloser(bytes.NewReader(body))
	resp := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    404,
		ContentLength: int64(len(body)),
		Body:          r,
	}

	body2 := []byte(`{"errors":[{"code":9999,"msg":"A made up error"}]}`)
	r2 := ioutil.NopCloser(bytes.NewReader(body2))
	resp2 := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    404,
		ContentLength: int64(len(body2)),
		Body:          r2,
	}

	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := new(mockHttpComponentInterface)
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp, nil).Once()
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp2, nil).Once()

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	n1qlC.enhancedPreparedSupported = 1
	n1qlC.queryCache.Put(n1qlQueryCacheStatementContext{Statement: "SELECT 1=1"}, &n1qlQueryCacheEntry{
		name: "somename",
	})
	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan error, 1)
	_, err = n1qlC.PreparedN1QLQuery(N1QLQueryOptions{
		Payload:       payload,
		RetryStrategy: NewBestEffortRetryStrategy(nil),
		Deadline:      time.Now().Add(100 * time.Millisecond),
	}, func(reader *N1QLRowReader, err error) {
		waitCh <- err
	})
	suite.Require().NoError(err, err)

	var n1qlErr *N1QLError
	suite.Require().ErrorAs(<-waitCh, &n1qlErr)

	suite.Assert().Equal(uint32(1), n1qlErr.RetryAttempts)
	suite.Assert().Contains(n1qlErr.RetryReasons, QueryPreparedStatementFailureRetryReason)
}

type n1qlRetryStrategy struct {
	maxAttempts uint32
	retries     int
}

func (mrs *n1qlRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	if req.RetryAttempts() >= mrs.maxAttempts {
		return &NoRetryRetryAction{}
	}
	mrs.retries++

	return &WithDurationRetryAction{WithDuration: 1 * time.Millisecond}
}

func (suite *UnitTestSuite) TestN1QLRetryTrueErrorReadOnly() {
	d, err := suite.LoadRawTestDataset("query_failure_retry_true")
	suite.Require().Nil(err)

	mrs := &n1qlRetryStrategy{maxAttempts: 3}
	reader := suite.doN1QLRequest(d, 500, mrs)
	suite.Assert().Nil(reader.reader)

	err = reader.err
	suite.Require().NotNil(err)

	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(99999), firstErr.Code)
	suite.Assert().Equal("some nonsense", firstErr.Message)
	suite.Assert().True(firstErr.Retry)
	suite.Assert().NotNil(firstErr.Reason)

	suite.Assert().Equal(3, mrs.retries)
}

func (suite *UnitTestSuite) TestN1QLCasMismatch() {
	d, err := suite.LoadRawTestDataset("query_failure_cas_mismatch_71")
	suite.Require().Nil(err)

	result := suite.doN1QLRequest(d, 200, nil)
	suite.Require().Nil(result.err, result.err)

	reader := result.reader

	numRows := 0
	for reader.NextRow() != nil {
		numRows++
	}
	suite.Assert().Zero(numRows)

	err = reader.Err()
	suite.Require().NotNil(err)

	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(12009), firstErr.Code)
	suite.Assert().Equal("some other message not matching on cas...", firstErr.Message)
	suite.Assert().False(firstErr.Retry)
	suite.Assert().NotNil(firstErr.Reason)

	suite.Assert().True(errors.Is(err, ErrCasMismatch), "Expected doc not found but was %s", err)
}

func (suite *UnitTestSuite) TestN1QLDocExists() {
	d, err := suite.LoadRawTestDataset("query_failure_doc_exists_71")
	suite.Require().Nil(err)

	result := suite.doN1QLRequest(d, 200, nil)
	suite.Require().Nil(result.err, result.err)

	reader := result.reader

	numRows := 0
	for reader.NextRow() != nil {
		numRows++
	}
	suite.Assert().Zero(numRows)

	err = reader.Err()
	suite.Require().NotNil(err)

	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(12009), firstErr.Code)
	suite.Assert().Equal("some message", firstErr.Message)
	suite.Assert().False(firstErr.Retry)
	suite.Assert().NotNil(firstErr.Reason)

	suite.Assert().True(errors.Is(err, ErrDocumentExists), "Expected doc not found but was %s", err)
}

func (suite *UnitTestSuite) TestN1QLDocNotFound() {
	d, err := suite.LoadRawTestDataset("query_failure_doc_not_found_71")
	suite.Require().Nil(err)

	result := suite.doN1QLRequest(d, 200, nil)
	suite.Require().Nil(result.err, result.err)

	reader := result.reader

	numRows := 0
	for reader.NextRow() != nil {
		numRows++
	}
	suite.Assert().Zero(numRows)

	err = reader.Err()
	suite.Require().NotNil(err)

	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(12009), firstErr.Code)
	suite.Assert().Equal("some message", firstErr.Message)
	suite.Assert().False(firstErr.Retry)
	suite.Assert().NotNil(firstErr.Reason)

	suite.Assert().True(errors.Is(err, ErrDocumentNotFound), "Expected doc not found but was %s", err)
}

type n1qlBodyErrDesc struct {
	Code    uint32
	Message string `json:"msg"`
	Retry   bool
	Reason  map[string]interface{}
}

type n1qlBody struct {
	RequestID       string            `json:"requestID"`
	ClientContextID string            `json:"clientContextID"`
	Signature       map[string]string `json:"signature"`
	Results         []interface{}     `json:"results"`
	Errors          []n1qlBodyErrDesc `json:"errors"`
	Status          string            `json:"status"`
	Metrics         struct {
		ElapsedTime   int `json:"elapsedTime"`
		ExecutionTime int `json:"executionTime"`
		ResultCount   int `json:"resultCount"`
		ResultSize    int `json:"resultSize"`
		ErrorCount    int `json:"errorCount"`
	} `json:"metrics"`
}

func (suite *UnitTestSuite) TestN1QLMB50643() {
	body := n1qlBody{
		RequestID:       "1234",
		ClientContextID: "12345",
		Signature:       map[string]string{"*": "*"},
		Results:         []interface{}{},
		Errors: []n1qlBodyErrDesc{
			{
				Code:    12016,
				Message: "MB50643",
				Retry:   true,
				Reason: map[string]interface{}{
					"name": "#primary",
				},
			},
		},
		Status: "errors",
		Metrics: struct {
			ElapsedTime   int `json:"elapsedTime"`
			ExecutionTime int `json:"executionTime"`
			ResultCount   int `json:"resultCount"`
			ResultSize    int `json:"resultSize"`
			ErrorCount    int `json:"errorCount"`
		}{
			ErrorCount: 1,
		},
	}

	d, err := json.Marshal(body)
	suite.Require().Nil(err)

	mrs := &n1qlRetryStrategy{maxAttempts: 3}
	reader := suite.doN1QLRequest(d, 500, mrs)
	suite.Assert().Nil(reader.reader)

	err = reader.err
	suite.Require().NotNil(err)

	var nErr *N1QLError
	suite.Require().True(errors.As(err, &nErr))

	suite.Require().Len(nErr.Errors, 1)
	firstErr := nErr.Errors[0]
	suite.Assert().Equal(uint32(12016), firstErr.Code)
	suite.Assert().Equal("MB50643", firstErr.Message)
	suite.Assert().True(firstErr.Retry)
	suite.Assert().NotNil(firstErr.Reason)

	suite.Assert().Equal(0, mrs.retries)

	suite.Assert().True(errors.Is(err, ErrIndexNotFound), "Expected index not found but was %s", err)
}

func (suite *UnitTestSuite) TestN1QLErrorCodes() {
	type test struct {
		code        uint32
		msg         string
		reason      map[string]interface{}
		expectedErr error
		raw         string
	}

	type jsonN1QLError struct {
		Code    uint32                 `json:"code"`
		Message string                 `json:"msg,omitempty"`
		Reason  map[string]interface{} `json:"reason,omitempty"`
	}

	makeRaw := func(code uint32, msg string) string {
		n1qlErr := []jsonN1QLError{
			{
				Code:    code,
				Message: msg,
			},
		}

		b, err := json.Marshal(n1qlErr)
		suite.Require().NoError(err)

		return string(b)
	}

	tests := []test{
		{
			code:        1000,
			expectedErr: nil,
		},
		{
			code:        3000,
			expectedErr: errParsingFailure,
		},
		{
			code:        3001,
			expectedErr: nil,
		},
		{
			code:        4000,
			expectedErr: errPlanningFailure,
		},
		{
			code:        5000,
			expectedErr: errInternalServerFailure,
		},
		{
			code:        5000,
			expectedErr: errIndexNotFound,
			msg:         "Index test not found",
		},
		{
			code:        5000,
			expectedErr: errIndexNotFound,
			msg:         "Index does not exist",
		},
		{
			code:        5000,
			expectedErr: errIndexExists,
			msg:         "Index test already exist",
		},
		{
			code:        5000,
			expectedErr: errQuotaLimitedFailure,
			msg:         "limit for number of indexes that can be created per scope has been reached",
		},
		{
			code:        10000,
			expectedErr: errAuthenticationFailure,
		},
		{
			code:        12000,
			expectedErr: errIndexFailure,
		},
		{
			code:        14000,
			expectedErr: errIndexFailure,
		},
		{
			code:        1065,
			expectedErr: errFeatureNotAvailable,
			msg:         "query_context",
		},
		{
			code:        1065,
			expectedErr: errFeatureNotAvailable,
			msg:         "preserve_expiry",
		},
		{
			code:        1065,
			expectedErr: nil,
			msg:         "something_else",
		},
		{
			code:        1065,
			expectedErr: errFeatureNotAvailable,
			msg:         "use_replica",
		},
		{
			code:        1080,
			expectedErr: errUnambiguousTimeout,
		},
		{
			code:        1191,
			expectedErr: errRateLimitedFailure,
		},
		{
			code:        1192,
			expectedErr: errRateLimitedFailure,
		},
		{
			code:        1193,
			expectedErr: errRateLimitedFailure,
		},
		{
			code:        1194,
			expectedErr: errRateLimitedFailure,
		},
		{
			code:        1197,
			expectedErr: errFeatureNotAvailable,
		},
		{
			code:        3230,
			expectedErr: errFeatureNotAvailable,
			msg:         "advisor",
		},
		{
			code:        3230,
			expectedErr: errFeatureNotAvailable,
			msg:         "advise",
		},
		{
			code:        3230,
			expectedErr: errFeatureNotAvailable,
			msg:         "query window functions",
		},
		{
			code:        4040,
			expectedErr: errPreparedStatementFailure,
		},
		{
			code:        4050,
			expectedErr: errPreparedStatementFailure,
		},
		{
			code:        4060,
			expectedErr: errPreparedStatementFailure,
		},
		{
			code:        4070,
			expectedErr: errPreparedStatementFailure,
		},
		{
			code:        4080,
			expectedErr: errPreparedStatementFailure,
		},
		{
			code:        4090,
			expectedErr: errPreparedStatementFailure,
		},
		{
			code:        4300,
			expectedErr: errIndexExists,
		},
		{
			code:        12004,
			expectedErr: errIndexNotFound,
		},
		{
			code:        12016,
			expectedErr: errIndexNotFound,
		},
		{
			code: 12009,
			reason: map[string]interface{}{
				"code": float64(12033),
			},
			expectedErr: errCasMismatch,
		},
		{
			code: 12009,
			reason: map[string]interface{}{
				"code": float64(17012),
			},
			expectedErr: errDocumentExists,
		},
		{
			code: 12009,
			reason: map[string]interface{}{
				"code": float64(17014),
			},
			expectedErr: errDocumentNotFound,
		},
		{
			code: 12009,
			reason: map[string]interface{}{
				"code": float64(11111),
			},
			expectedErr: errDMLFailure,
		},
		{
			code:        13014,
			expectedErr: errAuthenticationFailure,
		},
	}

	for _, tt := range tests {
		suite.T().Run(fmt.Sprintf("Error code %d", tt.code), func(t *testing.T) {
			type respBody struct {
				Errors []jsonN1QLError
			}

			body := respBody{
				Errors: []jsonN1QLError{
					{
						Code:    tt.code,
						Message: tt.msg,
						Reason:  tt.reason,
					},
				},
			}

			b, err := json.Marshal(body)
			assert.NoError(t, err)

			raw, descs, err := parseN1QLError(b)
			assert.ErrorIs(t, err, tt.expectedErr)
			if tt.expectedErr == nil {
				assert.Equal(t, makeRaw(tt.code, tt.msg), raw)
			}
			require.Equal(t, 1, len(descs))
			assert.Equal(t, tt.code, descs[0].Code)
			assert.Equal(t, tt.reason, descs[0].Reason)
		})
	}
}

func (suite *UnitTestSuite) TestN1QLEnhPreparedDifferentiatesQueryContext() {
	body := []byte(`{"results":[]}`)
	r := ioutil.NopCloser(bytes.NewReader(body))
	resp := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    200,
		ContentLength: int64(len(body)),
		Body:          r,
	}
	r2 := ioutil.NopCloser(bytes.NewReader(body))
	resp2 := &HTTPResponse{
		Endpoint:      "whatever",
		StatusCode:    200,
		ContentLength: int64(len(body)),
		Body:          r2,
	}

	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.Anything)

	httpC := new(mockHttpComponentInterface)
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(0).(*httpRequest)
		var body map[string]interface{}
		suite.Require().NoError(json.Unmarshal(req.Body, &body))

		statement := body["prepared"]
		suite.Assert().Equal("cluster_level_plan", statement)

		suite.Assert().NotContains(body, "auto_execute")
	})
	httpC.On("DoInternalHTTPRequest", mock.AnythingOfType("*gocbcore.httpRequest"), false).
		Return(resp2, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(0).(*httpRequest)
		var body map[string]interface{}
		suite.Require().NoError(json.Unmarshal(req.Body, &body))

		statement := body["prepared"]
		suite.Assert().Equal("scope_level_plan", statement)

		suite.Assert().NotContains(body, "auto_execute")
	})

	n1qlC := newN1QLQueryComponent(httpC, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))

	n1qlC.enhancedPreparedSupported = 1
	n1qlC.queryCache.Put(n1qlQueryCacheStatementContext{Statement: "SELECT 1=1"}, &n1qlQueryCacheEntry{
		name: "cluster_level_plan",
	})
	n1qlC.queryCache.Put(n1qlQueryCacheStatementContext{Statement: "SELECT 1=1", Context: "default.test"}, &n1qlQueryCacheEntry{
		name: "scope_level_plan",
	})
	test := map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
	}
	payload, err := json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh := make(chan error, 1)
	_, err = n1qlC.PreparedN1QLQuery(N1QLQueryOptions{
		Payload:       payload,
		RetryStrategy: NewBestEffortRetryStrategy(nil),
		Deadline:      time.Now().Add(100 * time.Millisecond),
	}, func(reader *N1QLRowReader, err error) {
		waitCh <- err
	})
	suite.Require().NoError(err, err)
	suite.Require().NoError(<-waitCh)

	test = map[string]interface{}{
		"statement":         "SELECT 1=1",
		"client_context_id": "1234",
		"query_context":     "default.test",
	}
	payload, err = json.Marshal(test)
	suite.Require().Nil(err, err)

	waitCh = make(chan error, 1)
	_, err = n1qlC.PreparedN1QLQuery(N1QLQueryOptions{
		Payload:       payload,
		RetryStrategy: NewBestEffortRetryStrategy(nil),
		Deadline:      time.Now().Add(100 * time.Millisecond),
	}, func(reader *N1QLRowReader, err error) {
		waitCh <- err
	})
	suite.Require().NoError(err, err)
	suite.Require().NoError(<-waitCh)
}
