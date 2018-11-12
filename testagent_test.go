package gocbcore

import (
	"math"
	"testing"
	"time"

	"gopkg.in/couchbaselabs/gojcbmock.v1"
)

type TestFeatureCode int

var (
	srvVer551  = NodeVersion{5, 5, 1, 0, ""}
	srvVer552  = NodeVersion{5, 5, 2, 0, ""}
	mockVer156 = NodeVersion{1, 5, 6, 0, ""}
)

var (
	TestAdjoinFeature     = TestFeatureCode(1)
	TestErrMapFeature     = TestFeatureCode(2)
	TestTimeTravelFeature = TestFeatureCode(3)
)

type testNode struct {
	*Agent
	Mock    *gojcbmock.Mock
	Version *NodeVersion
}

func (c *testNode) isMock() bool {
	return c.Mock != nil
}

func (c *testNode) supportsMockFeature(feature TestFeatureCode) bool {
	return true
}

func (c *testNode) supportsServerFeature(feature TestFeatureCode) bool {
	switch feature {
	case TestAdjoinFeature:
		return !c.Version.Equal(srvVer551) && !c.Version.Equal(srvVer552)
	case TestErrMapFeature:
		return false
	case TestTimeTravelFeature:
		return false
	}

	return false
}

func (c *testNode) SupportsFeature(feature TestFeatureCode) bool {
	if c.isMock() {
		return c.supportsMockFeature(feature)
	}

	return c.supportsServerFeature(feature)
}

func (c *testNode) NotSupportsFeature(feature TestFeatureCode) bool {
	return !c.SupportsFeature(feature)
}

type Signaler struct {
	t      *testing.T
	signal chan int
	op     PendingOp
}

func (s *Signaler) Continue() {
	s.signal <- 0
}

func (s *Signaler) Wrap(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			// Rethrow actual panics
			if r != s {
				panic(r)
			}
		}
	}()
	fn()
	s.signal <- 0
}

func (s *Signaler) Fatalf(fmt string, args ...interface{}) {
	s.t.Logf(fmt, args...)
	s.signal <- 1
	panic(s)
}

func (s *Signaler) Skipf(fmt string, args ...interface{}) {
	s.t.Logf(fmt, args...)
	s.signal <- 2
	panic(s)
}

func (s *Signaler) Wait(waitSecs int) {
	if s.op == nil {
		panic("Cannot wait if there is no op set on signaler")
	}
	if waitSecs <= 0 {
		waitSecs = 5
	}

	select {
	case v := <-s.signal:
		s.op = nil
		if v == 1 {
			s.t.FailNow()
		} else if v == 2 {
			s.t.SkipNow()
		}
	case <-time.After(time.Duration(waitSecs) * time.Second):
		if !s.op.Cancel() {
			<-s.signal
		}
		s.op = nil
		s.t.Fatalf("Wait timeout expired")
	}
}

func (s *Signaler) PushOp(op PendingOp, err error) {
	if err != nil {
		s.t.Fatal(err.Error())
		return
	}
	if s.op != nil {
		panic("Can only set one op on the signaler at a time")
	}
	s.op = op
}

func (c *testNode) TimeTravel(waitDura time.Duration) {
	if c.isMock() {
		waitSecs := int(math.Ceil(float64(waitDura) / float64(time.Second)))
		c.Mock.Control(gojcbmock.NewCommand(gojcbmock.CTimeTravel, map[string]interface{}{
			"Offset": waitSecs,
		}))
	} else {
		time.Sleep(waitDura)
	}
}

func (c *testNode) getSignaler(t *testing.T) *Signaler {
	signaler := &Signaler{
		t:      t,
		signal: make(chan int),
	}
	return signaler
}
