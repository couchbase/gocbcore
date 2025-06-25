package gocbcore

import (
	"container/list"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

// memdOpTombstone is used to report additional information once an orphaned (i.e. zombie) response is
// received, after we've cancelled the request.
type memdOpTombstone struct {
	totalServerDuration time.Duration
	dispatchTime        time.Time
	lastAttemptTime     time.Time
	isDurable           bool
	command             memd.CmdCode
}

type memdOpTombstoneStoreItem struct {
	meta *memdOpTombstone
	elem *list.Element
}

// memdOpTombstoneStore is used to store tombstones with some metadata for operations that have been cancelled. This
// allows us to report additional information about the operation once/if we receive the orphaned response.
// Note that this structure is not thread safe, and uses should be guarded by a mutex.
type memdOpTombstoneStore struct {
	tombstones map[uint32]memdOpTombstoneStoreItem
	opaqueList *list.List
	capacity   int
}

func newMemdOpTombstoneStore() *memdOpTombstoneStore {
	return &memdOpTombstoneStore{
		tombstones: make(map[uint32]memdOpTombstoneStoreItem),
		capacity:   1000,
		opaqueList: list.New(),
	}
}

func (s *memdOpTombstoneStore) Add(opaque uint32, metadata *memdOpTombstone) bool {
	eviction := false

	if s.opaqueList.Len() >= s.capacity {
		eviction = true
		oldestOpaque := s.opaqueList.Front()
		s.opaqueList.Remove(oldestOpaque)
		delete(s.tombstones, oldestOpaque.Value.(uint32))
	}

	elem := s.opaqueList.PushBack(opaque)
	s.tombstones[opaque] = memdOpTombstoneStoreItem{
		meta: metadata,
		elem: elem,
	}
	return eviction
}

func (s *memdOpTombstoneStore) FindAndMaybeRemove(opaque uint32) *memdOpTombstone {
	item, ok := s.tombstones[opaque]
	if !ok {
		return nil
	}
	s.opaqueList.Remove(item.elem)
	delete(s.tombstones, opaque)
	return item.meta
}
