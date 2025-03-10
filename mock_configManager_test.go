// Code generated by mockery v2.52.2. DO NOT EDIT.

package gocbcore

import mock "github.com/stretchr/testify/mock"

// mockConfigManager is an autogenerated mock type for the configManager type
type mockConfigManager struct {
	mock.Mock
}

// AddConfigWatcher provides a mock function with given fields: watcher
func (_m *mockConfigManager) AddConfigWatcher(watcher routeConfigWatcher) {
	_m.Called(watcher)
}

// RemoveConfigWatcher provides a mock function with given fields: watcher
func (_m *mockConfigManager) RemoveConfigWatcher(watcher routeConfigWatcher) {
	_m.Called(watcher)
}

// newMockConfigManager creates a new instance of mockConfigManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockConfigManager(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockConfigManager {
	mock := &mockConfigManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
