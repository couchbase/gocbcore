package gocbcore

import (
	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *StandardTestSuite) TestOpMap() {
	rd := newMemdOpMap()

	testOp1 := &memdQRequest{
		Packet: memd.Packet{},
	}
	testOp2 := &memdQRequest{
		Packet: memd.Packet{},
	}

	testOp3 := &memdQRequest{
		Packet:     memd.Packet{},
		Persistent: true,
	}

	// Single Remove
	rd.Add(testOp1)
	if rd.Remove(testOp1) != true {
		suite.T().Fatalf("The op should be there")
	}
	if rd.Remove(testOp1) != false {
		suite.T().Fatalf("There should be nothing to remove")
	}

	// Single opaque remove
	rd.Add(testOp1)
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != testOp1 {
		suite.T().Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != nil {
		suite.T().Fatalf("The op should not have been there")
	}

	// In order remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.Remove(testOp1) != true {
		suite.T().Fatalf("The op should be there")
	}
	if rd.Remove(testOp2) != true {
		suite.T().Fatalf("The op should be there")
	}
	if rd.Remove(testOp1) != false {
		suite.T().Fatalf("There should be nothing to remove")
	}
	if rd.Remove(testOp2) != false {
		suite.T().Fatalf("There should be nothing to remove")
	}

	// Out of order remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.Remove(testOp2) != true {
		suite.T().Fatalf("The op should be there")
	}
	if rd.Remove(testOp1) != true {
		suite.T().Fatalf("The op should be there")
	}
	if rd.Remove(testOp2) != false {
		suite.T().Fatalf("There should be nothing to remove")
	}
	if rd.Remove(testOp1) != false {
		suite.T().Fatalf("There should be nothing to remove")
	}

	// In order opaque remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != testOp1 {
		suite.T().Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != testOp2 {
		suite.T().Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != nil {
		suite.T().Fatalf("The op should not have been there")
	}
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != nil {
		suite.T().Fatalf("The op should not have been there")
	}

	// Out of order opaque remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != testOp2 {
		suite.T().Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != testOp1 {
		suite.T().Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != nil {
		suite.T().Fatalf("The op should not have been there")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != nil {
		suite.T().Fatalf("The op should not have been there")
	}

	rd.Add(testOp3)
	if rd.FindAndMaybeRemove(testOp3.Opaque, true) != testOp3 {
		suite.T().Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp3.Opaque, true) != nil {
		suite.T().Fatalf("The op should not have been there")
	}

	// Drain
	rd.Add(testOp2)
	rd.Add(testOp1)
	found1 := 0
	found2 := 0
	rd.Drain(func(op *memdQRequest) {
		if op == testOp1 {
			found1++
		}
		if op == testOp2 {
			found2++
		}
	})
	if found1 != 1 || found2 != 1 {
		suite.T().Fatalf("Drain behaved incorrected")
	}
}
