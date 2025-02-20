// Code generated by mockery v2.52.2. DO NOT EDIT.

package gocbcore

import mock "github.com/stretchr/testify/mock"

// mockDispatcher is an autogenerated mock type for the dispatcher type
type mockDispatcher struct {
	mock.Mock
}

// CollectionsEnabled provides a mock function with no fields
func (_m *mockDispatcher) CollectionsEnabled() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for CollectionsEnabled")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// DispatchDirect provides a mock function with given fields: req
func (_m *mockDispatcher) DispatchDirect(req *memdQRequest) (PendingOp, error) {
	ret := _m.Called(req)

	if len(ret) == 0 {
		panic("no return value specified for DispatchDirect")
	}

	var r0 PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(*memdQRequest) (PendingOp, error)); ok {
		return rf(req)
	}
	if rf, ok := ret.Get(0).(func(*memdQRequest) PendingOp); ok {
		r0 = rf(req)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(*memdQRequest) error); ok {
		r1 = rf(req)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DispatchDirectToAddress provides a mock function with given fields: req, address
func (_m *mockDispatcher) DispatchDirectToAddress(req *memdQRequest, address string) (PendingOp, error) {
	ret := _m.Called(req, address)

	if len(ret) == 0 {
		panic("no return value specified for DispatchDirectToAddress")
	}

	var r0 PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(*memdQRequest, string) (PendingOp, error)); ok {
		return rf(req, address)
	}
	if rf, ok := ret.Get(0).(func(*memdQRequest, string) PendingOp); ok {
		r0 = rf(req, address)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(*memdQRequest, string) error); ok {
		r1 = rf(req, address)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PipelineSnapshot provides a mock function with no fields
func (_m *mockDispatcher) PipelineSnapshot() (*pipelineSnapshot, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for PipelineSnapshot")
	}

	var r0 *pipelineSnapshot
	var r1 error
	if rf, ok := ret.Get(0).(func() (*pipelineSnapshot, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *pipelineSnapshot); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pipelineSnapshot)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RequeueDirect provides a mock function with given fields: req, isRetry
func (_m *mockDispatcher) RequeueDirect(req *memdQRequest, isRetry bool) {
	_m.Called(req, isRetry)
}

// SetPostCompleteErrorHandler provides a mock function with given fields: handler
func (_m *mockDispatcher) SetPostCompleteErrorHandler(handler postCompleteErrorHandler) {
	_m.Called(handler)
}

// SupportsCollections provides a mock function with no fields
func (_m *mockDispatcher) SupportsCollections() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for SupportsCollections")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// newMockDispatcher creates a new instance of mockDispatcher. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDispatcher(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDispatcher {
	mock := &mockDispatcher{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
