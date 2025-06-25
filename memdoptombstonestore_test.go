package gocbcore

import (
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *UnitTestSuite) TestTombstoneStore() {
	st := newMemdOpTombstoneStore()
	st.capacity = 2

	evicted := st.Add(1, &memdOpTombstone{
		totalServerDuration: 100 * time.Microsecond,
		dispatchTime:        time.Now(),
		lastAttemptTime:     time.Now(),
		isDurable:           false,
		command:             memd.CmdGet,
	})
	suite.Require().False(evicted)
	suite.Require().Equal(1, st.opaqueList.Len())
	suite.Require().Len(st.tombstones, 1)
	suite.Require().Contains(st.tombstones, uint32(1))

	evicted = st.Add(2, &memdOpTombstone{
		totalServerDuration: 200 * time.Microsecond,
		dispatchTime:        time.Now(),
		lastAttemptTime:     time.Now(),
		isDurable:           false,
		command:             memd.CmdGet,
	})
	suite.Require().False(evicted)
	suite.Require().Equal(2, st.opaqueList.Len())
	suite.Require().Len(st.tombstones, 2)
	suite.Require().Contains(st.tombstones, uint32(1))
	suite.Require().Contains(st.tombstones, uint32(2))

	evicted = st.Add(3, &memdOpTombstone{
		totalServerDuration: 300 * time.Microsecond,
		dispatchTime:        time.Now(),
		lastAttemptTime:     time.Now(),
		isDurable:           false,
		command:             memd.CmdGet,
	})
	suite.Require().True(evicted)
	suite.Require().Equal(2, st.opaqueList.Len())
	suite.Require().Len(st.tombstones, 2)
	suite.Require().NotContains(st.tombstones, uint32(1))
	suite.Require().Contains(st.tombstones, uint32(2))
	suite.Require().Contains(st.tombstones, uint32(3))

	tombstone := st.FindAndMaybeRemove(1)
	suite.Require().Nil(tombstone)
	suite.Require().Equal(2, st.opaqueList.Len())
	suite.Require().Len(st.tombstones, 2)
	suite.Require().NotContains(st.tombstones, uint32(1))
	suite.Require().Contains(st.tombstones, uint32(2))
	suite.Require().Contains(st.tombstones, uint32(3))

	tombstone = st.FindAndMaybeRemove(3)
	suite.Require().Equal(300*time.Microsecond, tombstone.totalServerDuration)
	suite.Require().Equal(1, st.opaqueList.Len())
	suite.Require().Len(st.tombstones, 1)
	suite.Require().Contains(st.tombstones, uint32(2))

	tombstone = st.FindAndMaybeRemove(2)
	suite.Require().Equal(200*time.Microsecond, tombstone.totalServerDuration)
	suite.Require().Equal(0, st.opaqueList.Len())
	suite.Require().Empty(st.tombstones)

	tombstone = st.FindAndMaybeRemove(2)
	suite.Require().Nil(tombstone)
}
