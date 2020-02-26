package gocbcore

import (
	"sync/atomic"
	"unsafe"
)

type httpMux struct {
	muxPtr     unsafe.Pointer
	breakerCfg CircuitBreakerConfig
}

func newHTTPMux(breakerCfg CircuitBreakerConfig) *httpMux {
	return &httpMux{
		breakerCfg: breakerCfg,
	}
}

func (mux *httpMux) Get() *httpClientMux {
	return (*httpClientMux)(atomic.LoadPointer(&mux.muxPtr))
}

func (mux *httpMux) Update(old, new *httpClientMux) bool {
	if new == nil {
		logErrorf("Attempted to update to nil httpClientMux")
		return false
	}

	if old != nil {
		return atomic.CompareAndSwapPointer(&mux.muxPtr, unsafe.Pointer(old), unsafe.Pointer(new))
	}

	if atomic.SwapPointer(&mux.muxPtr, unsafe.Pointer(new)) != nil {
		logErrorf("Updated from nil attempted on initialized httpClientMux")
		return false
	}

	return true
}

func (mux *httpMux) Clear() *httpClientMux {
	val := atomic.SwapPointer(&mux.muxPtr, nil)
	return (*httpClientMux)(val)
}

func (mux *httpMux) ApplyRoutingConfig(cfg *routeConfig) {
	oldHTTPMux := mux.Get()

	newHTTPMux := newHTTPClientMux(cfg, mux.breakerCfg)

	mux.Update(oldHTTPMux, newHTTPMux)
}

func (mux *httpMux) CapiEps() []string {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	return clientMux.capiEpList
}

func (mux *httpMux) MgmtEps() []string {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	return clientMux.mgmtEpList
}

func (mux *httpMux) N1qlEps() []string {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	return clientMux.n1qlEpList
}

func (mux *httpMux) CbasEps() []string {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	return clientMux.cbasEpList
}

func (mux *httpMux) FtsEps() []string {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	return clientMux.ftsEpList
}

func (mux *httpMux) Close() error {
	mux.Clear()
	return nil
}
