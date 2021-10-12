package gocbcore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
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
	suite.VerifyMetrics("n1ql:N1QLQuery", 1, true)

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
	suite.VerifyMetrics("n1ql:N1QLQuery", 1, true)
}

func (suite *StandardTestSuite) TestN1QLTimeout() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	ag := suite.AgentGroup()

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1","client_context_id":"12345"}`, suite.BucketName)
	_, err := ag.N1QLQuery(N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(100 * time.Microsecond),
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
		if suite.Assert().GreaterOrEqual(len(nilParents), 1) {
			for i := 0; i < len(nilParents); i++ {
				suite.AssertHTTPSpan(nilParents[i], "N1QLQuery")
			}
		}
	}
	suite.VerifyMetrics("n1ql:N1QLQuery", 1, true)
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
	suite.VerifyMetrics("n1ql:PreparedN1QLQuery", 1, true)
}

func (suite *StandardTestSuite) TestN1QLPreparedTimeout() {
	suite.EnsureSupportsFeature(TestFeatureN1ql)

	ag := suite.AgentGroup()

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1","client_context_id":"12345"}`, suite.BucketName)
	_, err := ag.PreparedN1QLQuery(N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(100 * time.Microsecond),
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
		if suite.Assert().GreaterOrEqual(len(nilParents), 1) {
			for i := 0; i < len(nilParents); i++ {
				suite.AssertHTTPSpan(nilParents[i], "PreparedN1QLQuery")
			}
		}
	}
	suite.VerifyMetrics("n1ql:PreparedN1QLQuery", 1, true)
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
