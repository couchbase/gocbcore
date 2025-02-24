package gocbcore

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"unsafe"
)

type columnarMux struct {
	muxPtr unsafe.Pointer
	cfgMgr configManager
}

func newColumnarMux(cfgMgr configManager, muxState *httpClientMux, noSeedNodeTLS bool) *columnarMux {
	mux := &columnarMux{
		cfgMgr: cfgMgr,
		muxPtr: unsafe.Pointer(muxState),
	}

	cfgMgr.AddConfigWatcher(mux)

	return mux
}

func (mux *columnarMux) Get() *columnarClientMux {
	return (*columnarClientMux)(atomic.LoadPointer(&mux.muxPtr))
}

func (mux *columnarMux) Update(old, newMux *columnarClientMux) bool {
	if newMux == nil {
		logErrorf("Attempted to update to nil columnarClientMux")
		return false
	}

	if old != nil {
		return atomic.CompareAndSwapPointer(&mux.muxPtr, unsafe.Pointer(old), unsafe.Pointer(newMux))
	}

	if atomic.SwapPointer(&mux.muxPtr, unsafe.Pointer(newMux)) != nil {
		logErrorf("Updated from nil attempted on initialized columnarClientMux")
		return false
	}

	return true
}

func (mux *columnarMux) Clear() *columnarClientMux {
	val := atomic.SwapPointer(&mux.muxPtr, nil)
	return (*columnarClientMux)(val)
}

func (mux *columnarMux) OnNewRouteConfig(cfg *routeConfig) {
	oldHTTPMux := mux.Get()
	if oldHTTPMux == nil {
		logWarnf("HTTP mux received new route config after shutdown")
		return
	}

	var endpoints []routeEndpoint
	if oldHTTPMux.tlsConfig != nil {
		endpoints = cfg.cbasEpList.SSLEndpoints
	} else {
		endpoints = cfg.cbasEpList.NonSSLEndpoints
	}

	var buffer bytes.Buffer
	addEps := func(title string, eps []routeEndpoint) {
		fmt.Fprintf(&buffer, "%s Eps:\n", title)
		for _, ep := range eps {
			fmt.Fprintf(&buffer, "  - %s\n", ep.Address)
		}
	}

	buffer.WriteString(fmt.Sprintln("Columnar muxer applying endpoints:"))
	addEps("Columnar", endpoints)

	logDebugf(buffer.String())

	newColumnarMux := newColumnarClientMux(cfg, endpoints, oldHTTPMux.tlsConfig, oldHTTPMux.auth)

	if !mux.Update(oldHTTPMux, newColumnarMux) {
		logDebugf("Failed to update columnar mux")
	}
}

func (mux *columnarMux) ColumnarEps() []routeEndpoint {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	return clientMux.epList
}

func (mux *columnarMux) Close() error {
	mux.cfgMgr.RemoveConfigWatcher(mux)
	mux.Clear()
	return nil
}

func (mux *columnarMux) Auth() AuthProvider {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	return clientMux.auth
}
