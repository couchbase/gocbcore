package gocbcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

type n1qlTestHelper struct {
	TestName      string
	NumDocs       int
	QueryTestDocs *testDocs
}

func hlpRunQuery(t *testing.T, agent *Agent, opts N1QLQueryOptions) ([][]byte, error) {
	t.Helper()

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
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

func hlpEnsurePrimaryIndex(t *testing.T, agent *Agent, bucketName string) {
	t.Helper()

	payloadStr := fmt.Sprintf(`{"statement":"CREATE PRIMARY INDEX ON %s"}`, bucketName)
	hlpRunQuery(t, agent, N1QLQueryOptions{
		Payload:  []byte(payloadStr),
		Deadline: time.Now().Add(5000 * time.Millisecond),
	})
}

func (nqh *n1qlTestHelper) testSetupN1ql(t *testing.T) {
	agent, h := testGetAgentAndHarness(t)

	nqh.QueryTestDocs = makeTestDocs(t, agent, nqh.TestName, nqh.NumDocs)

	hlpEnsurePrimaryIndex(t, agent, h.BucketName)
}

func (nqh *n1qlTestHelper) testCleanupN1ql(t *testing.T) {
	if nqh.QueryTestDocs != nil {
		nqh.QueryTestDocs.Remove()
		nqh.QueryTestDocs = nil
	}
}

func (nqh *n1qlTestHelper) testN1QLBasic(t *testing.T) {
	agent, h := testGetAgentAndHarness(t)

	deadline := time.Now().Add(15000 * time.Millisecond)
	runTestQuery := func() ([]testDoc, error) {
		test := map[string]interface{}{
			"statement": fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"%s\"", h.BucketName, nqh.TestName),
		}
		payload, err := json.Marshal(test)
		if err != nil {
			t.Errorf("failed to marshal test payload: %s", err)
		}

		iterDeadline := time.Now().Add(5000 * time.Millisecond)
		if iterDeadline.After(deadline) {
			iterDeadline = deadline
		}

		resCh := make(chan *N1QLRowReader)
		errCh := make(chan error)
		_, err = agent.N1QLQuery(N1QLQueryOptions{
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
			t.Errorf("timed out waiting for indexing: %s", lastError)
			break
		}
	}
}

func (nqh *n1qlTestHelper) testN1QLPrepared(t *testing.T) {
	agent, h := testGetAgentAndHarness(t)

	deadline := time.Now().Add(15000 * time.Millisecond)
	runTestQuery := func() ([]testDoc, error) {
		test := map[string]interface{}{
			"statement": fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"%s\"", h.BucketName, nqh.TestName),
		}
		payload, err := json.Marshal(test)
		if err != nil {
			t.Errorf("failed to marshal test payload: %s", err)
		}

		iterDeadline := time.Now().Add(5000 * time.Millisecond)
		if iterDeadline.After(deadline) {
			iterDeadline = deadline
		}

		resCh := make(chan *N1QLRowReader)
		errCh := make(chan error)
		_, err = agent.PreparedN1QLQuery(N1QLQueryOptions{
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
			t.Errorf("timed out waiting for indexing: %s", lastError)
			break
		}
	}
}

func TestN1QL(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureN1ql)

	helper := &n1qlTestHelper{
		TestName: "testQuery",
		NumDocs:  5,
	}

	t.Run("setup", helper.testSetupN1ql)

	t.Run("Basic", helper.testN1QLBasic)

	t.Run("cleanup", helper.testCleanupN1ql)
}

func TestN1QLCancel(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureN1ql)

	agent, h := testGetAgentAndHarness(t)

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1"}`, h.BucketName)
	op, err := agent.N1QLQuery(N1QLQueryOptions{
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
		t.Fatalf("Failed to execute query %s", err)
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
		t.Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrRequestCanceled) {
		t.Fatalf("Error should have been request canceled but was %s", resErr)
	}
}

func TestN1QLTimeout(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureN1ql)

	agent, h := testGetAgentAndHarness(t)

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1"}`, h.BucketName)
	_, err := agent.N1QLQuery(N1QLQueryOptions{
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
		t.Fatalf("Failed to execute query %s", err)
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
		t.Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrTimeout) {
		t.Fatalf("Error should have been request canceled but was %s", resErr)
	}
}

func TestN1QLPrepared(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureN1ql)

	helper := &n1qlTestHelper{
		TestName: "testPreparedQuery",
		NumDocs:  5,
	}

	t.Run("setup", helper.testSetupN1ql)

	t.Run("Basic", helper.testN1QLPrepared)

	t.Run("cleanup", helper.testCleanupN1ql)
}

func TestN1QLPreparedCancel(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureN1ql)

	agent, h := testGetAgentAndHarness(t)

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1"}`, h.BucketName)
	op, err := agent.PreparedN1QLQuery(N1QLQueryOptions{
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
		t.Fatalf("Failed to execute query %s", err)
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
		t.Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrRequestCanceled) {
		t.Fatalf("Error should have been request canceled but was %s", resErr)
	}
}

func TestN1QLPreparedTimeout(t *testing.T) {
	testEnsureSupportsFeature(t, TestFeatureN1ql)

	agent, h := testGetAgentAndHarness(t)

	resCh := make(chan *N1QLRowReader)
	errCh := make(chan error)
	payloadStr := fmt.Sprintf(`{"statement":"SELECT * FROM %s LIMIT 1"}`, h.BucketName)
	_, err := agent.PreparedN1QLQuery(N1QLQueryOptions{
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
		t.Fatalf("Failed to execute query %s", err)
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
		t.Fatal("Received rows but should not have")
	}

	if !errors.Is(resErr, ErrTimeout) {
		t.Fatalf("Error should have been request canceled but was %s", resErr)
	}
}
