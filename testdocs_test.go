package gocbcore

import (
	"encoding/json"
	"fmt"
	"testing"
)

const TestNumDocs = 5

type testDoc struct {
	TestName string `json:"testName"`
	I        int    `json:"i"`
}

type testDocs struct {
	t        *testing.T
	agent    *Agent
	testName string
	numDocs  int
}

func (td *testDocs) upsert() {
	waitCh := make(chan error, td.numDocs)

	for i := 1; i <= td.numDocs; i++ {
		testDocName := fmt.Sprintf("%s-%d", td.testName, i)

		bytes, err := json.Marshal(testDoc{
			TestName: td.testName,
			I:        i,
		})
		if err != nil {
			td.t.Errorf("failed to marshal test doc: %v", err)
			return
		}

		td.agent.Set(SetOptions{
			Key:   []byte(testDocName),
			Value: bytes,
		}, func(res *StoreResult, err error) {
			waitCh <- err
		})
	}

	for i := 1; i <= td.numDocs; i++ {
		err := <-waitCh
		if err != nil {
			td.t.Errorf("failed to remove test doc: %v", err)
			return
		}
	}
}

func (td *testDocs) Remove() {
	waitCh := make(chan error, td.numDocs)

	for i := 1; i <= td.numDocs; i++ {
		testDocName := fmt.Sprintf("%s-%d", td.testName, i)

		td.agent.Delete(DeleteOptions{
			Key: []byte(testDocName),
		}, func(res *DeleteResult, err error) {
			waitCh <- err
		})
	}

	for i := 1; i <= td.numDocs; i++ {
		err := <-waitCh
		if err != nil {
			td.t.Errorf("failed to remove test doc: %v", err)
			return
		}
	}
}

func makeTestDocs(t *testing.T, agent *Agent, testName string, numDocs int) *testDocs {
	td := &testDocs{
		t:        t,
		agent:    agent,
		testName: testName,
		numDocs:  numDocs,
	}
	td.upsert()
	return td
}
