package gocbcore

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

type routeData struct {
	revId   int64
	bktType bucketType

	ketamaMap   *ketamaContinuum
	vbMap       *vbucketMap
	kvPipelines []*memdPipeline
	deadPipe    *memdPipeline

	capiEpList []string
	mgmtEpList []string
	n1qlEpList []string
	ftsEpList  []string

	source *routeConfig
}

func (rd *routeData) DebugString() string {
	var outStr string

	outStr += fmt.Sprintf("Revision ID: %d\n", rd.revId)

	for i, n := range rd.kvPipelines {
		outStr += fmt.Sprintf("Pipeline %d:\n", i)
		outStr += reindentLog("  ", n.debugString()) + "\n"
	}

	outStr += "Dead Pipeline:\n"
	if rd.deadPipe != nil {
		outStr += reindentLog("  ", rd.deadPipe.debugString()) + "\n"
	} else {
		outStr += "  Disabled\n"
	}

	outStr += "Capi Eps:\n"
	for _, ep := range rd.capiEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "Mgmt Eps:\n"
	for _, ep := range rd.mgmtEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "N1ql Eps:\n"
	for _, ep := range rd.n1qlEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "FTS Eps:\n"
	for _, ep := range rd.ftsEpList {
		outStr += fmt.Sprintf("  - %s\n", ep)
	}

	outStr += "Source Data: *"
	//outStr += fmt.Sprintf("  Source Data: %v", d.source)

	return outStr
}

type routeDataPtr struct {
	data unsafe.Pointer
}

func (ptr *routeDataPtr) Get() *routeData {
	return (*routeData)(atomic.LoadPointer(&ptr.data))
}

func (ptr *routeDataPtr) Update(old, new *routeData) bool {
	if new == nil {
		logErrorf("Attempted to update to nil routeData")
		return false
	}

	if old != nil {
		return atomic.CompareAndSwapPointer(&ptr.data, unsafe.Pointer(old), unsafe.Pointer(new))
	}

	if atomic.SwapPointer(&ptr.data, unsafe.Pointer(new)) != nil {
		logErrorf("Updated from nil attempted on initialized routeDataPtr")
		return false
	}

	return true
}

func (ptr *routeDataPtr) Clear() *routeData {
	val := atomic.SwapPointer(&ptr.data, nil)
	return (*routeData)(val)
}
