package gocbcore

import (
	"errors"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *StandardTestSuite) TestEnhancedErrors() {
	agent, s := suite.GetAgentAndHarness()

	var err1, err2 error

	s.PushOp(agent.Get(GetOptions{
		Key: []byte("keyThatWontExist"),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			err1 = err
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get(GetOptions{
		Key: []byte("keyThatWontExist"),
	}, func(res *GetResult, err error) {
		s.Wrap(func() {
			err2 = err
		})
	}))
	s.Wait(0)

	if err1 == err2 {
		suite.T().Fatalf("Operation error results should never return equivalent values")
	}
}

func (suite *StandardTestSuite) TestEnhancedErrorOp() {
	suite.EnsureSupportsFeature(TestFeatureErrMap)

	if !suite.IsMockServer() {
		suite.T().Skipf("only supported when testing against mock server")
	}

	spec := suite.StartTest(TestNameExtendedError)
	h := suite.GetHarness()
	agent := spec.Agent

	h.PushOp(agent.GetAndLock(GetAndLockOptions{
		Key:            []byte("testEnhancedErrs"),
		LockTime:       10,
		CollectionName: spec.Collection,
		ScopeName:      spec.Scope,
	}, func(res *GetAndLockResult, err error) {
		h.Wrap(func() {
			typedErr, ok := err.(*KeyValueError)
			if !ok {
				h.Fatalf("error should be a KeyValueError: %v", err)
			}

			if typedErr.Context == "" {
				h.Fatalf("error should have a context")
			}

			if typedErr.ErrorName == "" {
				h.Fatalf("error should have a name")
			}

			if typedErr.ErrorDescription == "" {
				h.Fatalf("error should have a description")
			}

			if typedErr.StatusCode != memd.StatusKeyNotFound {
				h.Fatalf("status code should have been StatusKeyNotFound")
			}

			if !errors.Is(err, ErrDocumentNotFound) {
				h.Fatalf("error cause should have been ErrDocumentNotFound")
			}
		})
	}))
	h.Wait(0)

	suite.EndTest(spec)
}

func (suite *UnitTestSuite) TestEnhanceKvErrorUnknownStatusCodeError() {
	errMapBytes, err := loadRawTestDataset("err_map71_v2")
	suite.Require().NoError(err)

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:        memd.CmdMagicReq,
			Command:      memd.CmdGet,
			Datatype:     0,
			Vbucket:      0,
			Opaque:       0x21,
			Cas:          0,
			CollectionID: 0x9,
			Key:          []byte("test"),
		},
		CollectionName: "testcol",
		ScopeName:      "testscop",
	}
	connInfo := memdQRequestConnInfo{
		lastDispatchedTo:   "127.0.0.1:11210",
		lastDispatchedFrom: "127.0.0.1:2323",
		lastConnectionID:   "abcdefg",
	}
	req.connInfo.Store(connInfo)

	resp := &memdQResponse{
		Packet: &memd.Packet{
			Status: memd.StatusKeyNotFound,
			Opaque: 0x21,
		},
	}

	bucket := "testbucket"

	errMapCmpt := newErrMapManager(bucket)
	errMapCmpt.StoreErrorMap(errMapBytes)

	srcErr := &unknownKvStatusCodeError{
		code: memd.StatusKeyNotFound,
	}

	resErr := errMapCmpt.EnhanceKvError(srcErr, resp, req)
	suite.Require().NotNil(resErr)

	var kvErr *KeyValueError
	suite.Require().ErrorAs(resErr, &kvErr)
	suite.Assert().Equal("Not Found (0x01)", kvErr.InnerError.Error())
	suite.Assert().Equal(string(req.Key), kvErr.DocumentKey)
	suite.Assert().Equal(bucket, kvErr.BucketName)
	suite.Assert().Equal(req.ScopeName, kvErr.ScopeName)
	suite.Assert().Equal(req.CollectionName, kvErr.CollectionName)
	suite.Assert().Equal(req.CollectionID, kvErr.CollectionID)
	suite.Assert().Equal(connInfo.lastDispatchedTo, kvErr.LastDispatchedTo)
	suite.Assert().Equal(connInfo.lastDispatchedFrom, kvErr.LastDispatchedFrom)
	suite.Assert().Equal(connInfo.lastConnectionID, kvErr.LastConnectionID)
	suite.Assert().Equal(resp.Status, kvErr.StatusCode)
	suite.Assert().Equal(resp.Opaque, kvErr.Opaque)
}

func (suite *UnitTestSuite) TestGetKvStatusCodeErrorUnknown() {
	code := memd.StatusCode(0xfa)

	err := getKvStatusCodeError(code)
	suite.Require().NotNil(err)

	var unknownErr *unknownKvStatusCodeError
	suite.Require().ErrorAs(err, &unknownErr)

	suite.Assert().Equal(code, unknownErr.code)
}
