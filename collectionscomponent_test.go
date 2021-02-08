package gocbcore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"

	"github.com/stretchr/testify/mock"
)

// When the SDK starts up collections support is unknown.
// This test is for the scenario when a request is made whilst collections support is unknown
// but collections are enabled and the server does support them.
// We should see the SDK queue the request until collections support is known, a request should
// be made to get the collection ID for the collection name and then the user's request sent with
// the collection ID on it.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsStateUnknownSupported() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()
	cfgMgr.On("RemoveConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Once()
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			extras := make([]byte, 12)
			binary.BigEndian.PutUint64(extras[0:], 1)
			binary.BigEndian.PutUint32(extras[8:], 8)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Extras: extras}}, req, nil)
			})
		})
	dispatcher.On("RequeueDirect", mock.AnythingOfType("*gocbcore.memdQRequest"), false).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdGet, req.Command)
			suite.Assert().Equal([]byte("test-key"), req.Key)
			suite.Assert().Equal(cName, req.CollectionName)
			suite.Assert().Equal(sName, req.ScopeName)
			suite.Assert().Equal(uint32(8), req.CollectionID)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Value: []byte("test")}}, req, nil)
			})
		})

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)

	waitCh := make(chan error, 1)
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		waitCh <- err
	}

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName:   cName,
		ScopeName:        sName,
		Callback:         handler,
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	// Update the cidMgr with a config to dequeue the request.
	cidMgr.OnNewRouteConfig(&routeConfig{
		bucketCapabilities: []string{"collections"},
	})

	// Requeueing on cid unknown is done in a go routine
	select {
	case <-time.After(1 * time.Second):
		suite.T().Fatalf("Timed out waiting for callback to be called")
	case err := <-waitCh:
		suite.Assert().Nil(err, err)
	}

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
}

// When the SDK starts up collections support is unknown.
// This test is for the scenario when a request is made whilst collections support is unknown
// but collections are enabled and the server does support them but the collection is initially unknown.
// We should see the SDK queue the request until collections support is known. A request should
// be made to get the collection ID for the collection name twice and then the user's request sent with
// the collection ID on it.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsStateUnknownCollectionUnknown() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()
	cfgMgr.On("RemoveConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Once()
	// First request we reply collection unknown.
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{}}, req, errCollectionNotFound)
			})
		}).Once()
	// Second request we simulate the collection coming online.
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			extras := make([]byte, 12)
			binary.BigEndian.PutUint64(extras[0:], 1)
			binary.BigEndian.PutUint32(extras[8:], 8)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Extras: extras}}, req, nil)
			})
		}).Once()
	dispatcher.On("RequeueDirect", mock.AnythingOfType("*gocbcore.memdQRequest"), false).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdGet, req.Command)
			suite.Assert().Equal([]byte("test-key"), req.Key)
			suite.Assert().Equal(cName, req.CollectionName)
			suite.Assert().Equal(sName, req.ScopeName)
			suite.Assert().Equal(uint32(8), req.CollectionID)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Value: []byte("test")}}, req, nil)
			})
		}).Once()

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)

	waitCh := make(chan error, 1)
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		waitCh <- err
	}

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName:   cName,
		ScopeName:        sName,
		Callback:         handler,
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	// Update the cidMgr with a config to dequeue the request.
	cidMgr.OnNewRouteConfig(&routeConfig{
		bucketCapabilities: []string{"collections"},
	})

	// Requeueing on cid unknown is done in a go routine
	select {
	case <-time.After(1 * time.Second):
		suite.T().Fatalf("Timed out waiting for callback to be called")
	case err := <-waitCh:
		suite.Assert().Nil(err, err)
	}

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
}

// When the SDK starts up collections support is unknown.
// This test is for the scenario when a request is made whilst collections support is unknown
// but collections are enabled and the server does support them but the cid request is met with a server error.
// We should see the SDK queue the request until the get cid request fails and then the callback should be hit.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsStateUnknownGenericError() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()
	cfgMgr.On("RemoveConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Once()
	// First request we reply collection unknown.
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{}}, req, errInternalServerFailure)
			})
		}).Once()

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)

	waitCh := make(chan error, 1)
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		waitCh <- err
	}

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName:   cName,
		ScopeName:        sName,
		Callback:         handler,
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	// Update the cidMgr with a config to dequeue the request.
	cidMgr.OnNewRouteConfig(&routeConfig{
		bucketCapabilities: []string{"collections"},
	})

	select {
	case <-time.After(1 * time.Second):
		suite.T().Fatalf("Timed out waiting for callback to be called")
	case err := <-waitCh:
		suite.Assert().NotNil(err, err)
	}

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
}

// When the SDK starts up collections support is unknown.
// This test is for the scenario when a request is made whilst collections support is unknown
// but collections are enabled and the server does not support them.
// We should see the SDK queue the request until collections support is known.
// The SDK should then fire the request callback with an error.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsStateUnknownUnsupported() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()
	cfgMgr.On("RemoveConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Once()

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)

	var called bool
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		called = true
		if !errors.Is(err, ErrCollectionsUnsupported) {
			suite.T().Errorf("Error should have been collections unsupported but was: %v", err)
		}
	}

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName:   cName,
		ScopeName:        sName,
		Callback:         handler,
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	// Update the cidMgr with a config to dequeue the request.
	cidMgr.OnNewRouteConfig(&routeConfig{
		bucketCapabilities: []string{},
	})

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
	suite.Assert().True(called)
}

// This tests that when the SDK knows the server collections state is unsupported then
// we receive an error.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsUnsupported() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Once()
	dispatcher.On("SupportsCollections").Return(false).Once()

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)
	cidMgr.configSeen = 1

	var called bool
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		called = true
	}

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName:   cName,
		ScopeName:        sName,
		Callback:         handler,
		RootTraceContext: noopSpanContext{},
	})
	if !errors.Is(err, ErrCollectionsUnsupported) {
		suite.T().Errorf("Error should have been collections unsupported but was: %v", err)
	}
	suite.Assert().Nil(op)

	// Update the cidMgr with a config to dequeue the request.
	cidMgr.OnNewRouteConfig(&routeConfig{
		bucketCapabilities: []string{},
	})

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
	suite.Assert().False(called)
}

// This test is for the scenario when a request is made whilst collections support is known
// amd collections are enabled, the server does support them, and the collection exists.
// We should see the SDK send a request to get the collection ID for the collection name and
// then the user's request sent with the collection ID on it.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsSupportedCollectionExists() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Once()
	dispatcher.On("SupportsCollections").Return(true).Once()
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			extras := make([]byte, 12)
			binary.BigEndian.PutUint64(extras[0:], 1)
			binary.BigEndian.PutUint32(extras[8:], 8)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Extras: extras}}, req, nil)
			})
		})
	dispatcher.On("RequeueDirect", mock.AnythingOfType("*gocbcore.memdQRequest"), false).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdGet, req.Command)
			suite.Assert().Equal([]byte("test-key"), req.Key)
			suite.Assert().Equal(cName, req.CollectionName)
			suite.Assert().Equal(sName, req.ScopeName)
			suite.Assert().Equal(uint32(8), req.CollectionID)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Value: []byte("test")}}, req, nil)
			})
		})

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)
	cidMgr.configSeen = 1

	waitCh := make(chan error, 1)
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		waitCh <- err
	}

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName:   cName,
		ScopeName:        sName,
		Callback:         handler,
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	select {
	case <-time.After(1 * time.Second):
		suite.T().Fatalf("Timed out waiting for callback to be called")
	case err := <-waitCh:
		suite.Assert().Nil(err, err)
	}

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
}

// This test is for the scenario when a request is made whilst collections support is known
// and collections are enabled, the server does support them, and the collection doesn't exist initially but then
// comes online.
// We should see the SDK send a request to get the collection ID for the collection name and
// then the user's request be failed.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsSupportedCollectionComesOnline() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Once()
	dispatcher.On("SupportsCollections").Return(true).Once()
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{}}, req, errCollectionNotFound)
			})
		}).Once()
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			extras := make([]byte, 12)
			binary.BigEndian.PutUint64(extras[0:], 1)
			binary.BigEndian.PutUint32(extras[8:], 8)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Extras: extras}}, req, nil)
			})
		}).Once()

	dispatcher.On("RequeueDirect", mock.AnythingOfType("*gocbcore.memdQRequest"), false).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdGet, req.Command)
			suite.Assert().Equal([]byte("test-key"), req.Key)
			suite.Assert().Equal(cName, req.CollectionName)
			suite.Assert().Equal(sName, req.ScopeName)
			suite.Assert().Equal(uint32(8), req.CollectionID)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Value: []byte("test")}}, req, nil)
			})
		}).Once()

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)
	cidMgr.configSeen = 1

	waitCh := make(chan error, 1)
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		waitCh <- err
	}

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName:   cName,
		ScopeName:        sName,
		Callback:         handler,
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	// Requeueing on cid unknown is done in a go routine
	select {
	case <-time.After(1 * time.Second):
		suite.T().Fatalf("Timed out waiting for callback to be called")
	case err := <-waitCh:
		suite.Assert().Nil(err, err)
	}

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
}

// This test extends TestCollectionsComponentCollectionsSupportedCollectionExists to add a second
// request which should be dispatched with no extra calls.
func (suite *UnitTestSuite) TestCollectionsComponentCollectionsSupportedCollectionUpdate() {
	cName := "test"
	sName := "_default"

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.collectionsComponent")).Return()

	dispatcher := new(mockDispatcher)
	dispatcher.On("SetPostCompleteErrorHandler", mock.AnythingOfType("gocbcore.postCompleteErrorHandler")).Return()
	dispatcher.On("CollectionsEnabled").Return(true).Times(3)
	dispatcher.On("SupportsCollections").Return(true).Times(3)
	// The first request to dispatch getting the cid.
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdCollectionsGetID, req.Command)
			suite.Assert().Equal([]byte(fmt.Sprintf("%s.%s", sName, cName)), req.Key)
			suite.Assert().Empty(req.CollectionName)
			suite.Assert().Empty(req.ScopeName)
			suite.Assert().Equal(-1, req.ReplicaIdx)

			extras := make([]byte, 12)
			binary.BigEndian.PutUint64(extras[0:], 1)
			binary.BigEndian.PutUint32(extras[8:], 8)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Extras: extras}}, req, nil)
			})
		}).Once()
	// The second request should be queued due to cid being pending so it should get requeued.
	dispatcher.On("RequeueDirect", mock.AnythingOfType("*gocbcore.memdQRequest"), false).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdGet, req.Command)
			suite.Assert().Equal([]byte("test-key"), req.Key)
			suite.Assert().Equal(cName, req.CollectionName)
			suite.Assert().Equal(sName, req.ScopeName)
			suite.Assert().Equal(uint32(8), req.CollectionID)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Value: []byte("test")}}, req, nil)
			})
		}).Twice()
	// The third request should go straight through to Dispatch.
	dispatcher.On("DispatchDirect", mock.AnythingOfType("*gocbcore.memdQRequest")).Return(&memdQRequest{}, nil).
		Run(func(args mock.Arguments) {
			req := args[0].(*memdQRequest)

			suite.Assert().Equal(memd.CmdMagicReq, req.Magic)
			suite.Assert().Equal(memd.CmdGet, req.Command)
			suite.Assert().Equal([]byte("test-key"), req.Key)
			suite.Assert().Equal(cName, req.CollectionName)
			suite.Assert().Equal(sName, req.ScopeName)
			suite.Assert().Equal(uint32(8), req.CollectionID)

			time.AfterFunc(time.Millisecond, func() {
				req.Callback(&memdQResponse{Packet: &memd.Packet{Value: []byte("test")}}, req, nil)
			})
		}).Once()

	cidMgr := newCollectionIDManager(collectionIDProps{
		DefaultRetryStrategy: &failFastRetryStrategy{},
		MaxQueueSize:         100},
		dispatcher,
		newTracerComponent(&noopTracer{}, "", true),
		cfgMgr,
	)
	cidMgr.configSeen = 1

	// This request should get queued as the manager hasn't seen a config.
	op, err := cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName: cName,
		ScopeName:      sName,
		Callback: func(resp *memdQResponse, req *memdQRequest, err error) {
		},
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	// This request should get queued because the cid is pending, it will then be requeued.
	waitCh := make(chan error)
	op, err = cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName: cName,
		ScopeName:      sName,
		Callback: func(resp *memdQResponse, req *memdQRequest, err error) {
			waitCh <- err
		},
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	select {
	case <-time.After(1 * time.Second):
		suite.T().Fatalf("Timed out waiting for callback to be called")
	case err := <-waitCh:
		suite.Assert().Nil(err, err)
	}

	waitCh = make(chan error)
	op, err = cidMgr.Dispatch(&memdQRequest{
		Packet: memd.Packet{
			Magic:    memd.CmdMagicReq,
			Command:  memd.CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte("test-key"),
			Value:    nil,
		},
		CollectionName: cName,
		ScopeName:      sName,
		Callback: func(resp *memdQResponse, req *memdQRequest, err error) {
			waitCh <- err
		},
		RootTraceContext: noopSpanContext{},
	})
	suite.Require().Nil(err, err)
	suite.Assert().NotNil(op)

	select {
	case <-time.After(1 * time.Second):
		suite.T().Fatalf("Timed out waiting for callback to be called")
	case err := <-waitCh:
		suite.Assert().Nil(err, err)
	}

	cfgMgr.AssertExpectations(suite.T())
	dispatcher.AssertExpectations(suite.T())
}
