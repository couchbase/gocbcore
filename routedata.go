package gocbcore

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

type routeData struct {
	revId   int64
	bktType bucketType

	vbMap       [][]int
	kvPipelines []*memdPipeline
	deadPipe    *memdPipeline

	capiEpList []string
	mgmtEpList []string
	n1qlEpList []string
	ftsEpList  []string

	source *routeConfig
}

func (rd *routeData) debugString() string {
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

func (ptr *routeDataPtr) get() *routeData {
	return (*routeData)(atomic.LoadPointer(&ptr.data))
}

func (ptr *routeDataPtr) update(old, new *routeData) bool {
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

func (ptr *routeDataPtr) clear() *routeData {
	val := atomic.SwapPointer(&ptr.data, nil)
	return (*routeData)(val)
}

// Maps a key to a vBucket and a server
// repidx is the server index within the vbucket entry to select. 0 means
// the master server
func (rd *routeData) MapKeyVBucket(key []byte, repIdx int) (srvidx int, vbid uint16) {
	vbid = uint16(cbCrc(key) % uint32(len(rd.vbMap)))
	srvidx = rd.vbMap[vbid][repIdx]
	return
}

func (rd *routeData) MapKetama(key []byte) (srvidx int) {
	hash := rd.source.KetamaHash(key)
	return int(rd.source.KetamaNode(hash))
}
