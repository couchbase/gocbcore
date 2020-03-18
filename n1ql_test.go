package gocbcore

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func hlpRunQuery(t *testing.T, agent *Agent, opts N1QLQueryOptions) ([][]byte, error) {
	t.Helper()

	var rowBytes [][]byte

	rows, err := agent.N1QLQuery(opts)
	if err != nil {
		return rowBytes, err
	}

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

var queryTestDocs *testDocs

func testSetupN1ql(t *testing.T) {
	agent, h := testGetAgentAndHarness(t)

	queryTestDocs = makeTestDocs(t, agent, "testQuery", 5)

	hlpEnsurePrimaryIndex(t, agent, h.BucketName)
}

func testCleanupN1ql(t *testing.T) {
	if queryTestDocs != nil {
		queryTestDocs.Remove()
		queryTestDocs = nil
	}
}

func testN1QLBasic(t *testing.T) {
	agent, h := testGetAgentAndHarness(t)

	deadline := time.Now().Add(15000 * time.Millisecond)
	runTestQuery := func() ([]testDoc, error) {
		test := map[string]interface{}{
			"statement": fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"testQuery\"", h.BucketName),
		}
		payload, err := json.Marshal(test)
		if err != nil {
			t.Errorf("failed to marshal test payload: %s", err)
		}

		iterDeadline := time.Now().Add(5000 * time.Millisecond)
		if iterDeadline.After(deadline) {
			iterDeadline = deadline
		}

		rows, err := agent.N1QLQuery(N1QLQueryOptions{
			Payload:       payload,
			RetryStrategy: nil,
			Deadline:      iterDeadline,
		})
		if err != nil {
			return nil, err
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
				if doc.I < 1 || doc.I > 5 {
					lastError = fmt.Sprintf("query test read invalid row i=%d", doc.I)
					testFailed = true
				}
			}

			numDocs := len(docs)
			if numDocs != 5 {
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

	t.Run("setup", testSetupN1ql)

	t.Run("Basic", testN1QLBasic)

	t.Run("cleanup", testCleanupN1ql)
}
