package gocbcore

import (
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/golang/snappy"
)

// fakeMemdConn is a minimal implementation of memdConn for unit testing.
type fakeMemdConn struct {
	features map[memd.HelloFeature]bool
}

func (c *fakeMemdConn) LocalAddr() string                      { return "127.0.0.1:0" }
func (c *fakeMemdConn) RemoteAddr() string                     { return "127.0.0.1:11210" }
func (c *fakeMemdConn) WritePacket(*memd.Packet) error         { return nil }
func (c *fakeMemdConn) ReadPacket() (*memd.Packet, int, error) { return nil, 0, nil }
func (c *fakeMemdConn) Close() error                           { return nil }
func (c *fakeMemdConn) Release()                               {}

func (c *fakeMemdConn) EnableFeature(feature memd.HelloFeature) {
	if c.features == nil {
		c.features = make(map[memd.HelloFeature]bool)
	}
	c.features[feature] = true
}
func (c *fakeMemdConn) IsFeatureEnabled(feature memd.HelloFeature) bool {
	if c.features == nil {
		return false
	}
	return c.features[feature]
}

// newTestMemdClient creates a memdClient with the minimal setup needed for resolveRequest tests.
// It does not call run() so no background goroutines are started.
func newTestMemdClient(disableDecompression bool) *memdClient {
	client := &memdClient{
		conn:                 &fakeMemdConn{},
		opList:               newMemdOpMap(),
		breaker:              newNoopCircuitBreaker(),
		closeNotify:          make(chan bool),
		connReleaseNotify:    make(chan struct{}),
		connReleasedNotify:   make(chan struct{}),
		disableDecompression: disableDecompression,
		postErrHandler: func(resp *memdQResponse, req *memdQRequest, err error) (bool, error) {
			return false, err
		},
	}
	return client
}

// callbackResult captures the fields we need to inspect from the callback,
// since the underlying memd.Packet is released (zeroed) after resolveRequest returns.
type callbackResult struct {
	called   bool
	err      error
	value    []byte
	datatype uint8
}

// TestResolveRequestCompressedEmptyValue reproduces an issue seen over DCP when a
// document has no xattrs and the body is omitted (e.g. by the NoValueWithUnderlyingDatatype
// flag). The server sends a packet with the compressed datatype flag set but an empty value.
// resolveRequest must not drop the event due to a snappy decompression failure on the
// empty value — it should clear the compressed flag and deliver the callback.
func (s *UnitTestSuite) TestResolveRequestCompressedEmptyValue() {
	client := newTestMemdClient(false)

	var result callbackResult

	// Register a persistent DCP stream request so resolveRequest can find it.
	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:   memd.CmdMagicReq,
			Command: memd.CmdDcpMutation,
			Key:     []byte("testDoc"),
		},
		Persistent: true,
		Callback: func(resp *memdQResponse, req *memdQRequest, err error) {
			result.called = true
			result.err = err
			if resp != nil {
				// Copy before the deferred ReleasePacket zeroes the packet.
				result.value = append([]byte(nil), resp.Value...)
				result.datatype = resp.Datatype
			}
		},
	}

	client.opList.Add(req)
	opaque := req.Opaque

	// Build a DCP mutation response with the compressed flag set but an empty value,
	// simulating the NoValueWithUnderlyingDatatype scenario.
	pkt := memd.AcquirePacket()
	pkt.Magic = memd.CmdMagicReq
	pkt.Command = memd.CmdDcpMutation
	pkt.Opaque = opaque
	pkt.Datatype = uint8(memd.DatatypeFlagJSON) | uint8(memd.DatatypeFlagCompressed)
	pkt.Value = nil // empty value — body omitted by server

	resp := &memdQResponse{Packet: pkt}

	client.resolveRequest(resp)

	s.True(result.called, "callback was not invoked — the event was dropped due to snappy decompression failure on empty value")
	s.NoError(result.err)
	// The compressed flag should still be set as the user explicitly wants the DataType.
	s.Equal(uint8(2), result.datatype&uint8(memd.DatatypeFlagCompressed), "expected compressed flag to be set")
}

// TestResolveRequestCompressedNonEmptyValue is a control test that verifies
// resolveRequest correctly decompresses a valid snappy-compressed value.
func (s *UnitTestSuite) TestResolveRequestCompressedNonEmptyValue() {
	client := newTestMemdClient(false)

	var result callbackResult

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:   memd.CmdMagicReq,
			Command: memd.CmdDcpMutation,
			Key:     []byte("testDoc"),
		},
		Persistent: true,
		Callback: func(resp *memdQResponse, req *memdQRequest, err error) {
			result.called = true
			result.err = err
			if resp != nil {
				result.value = append([]byte(nil), resp.Value...)
				result.datatype = resp.Datatype
			}
		},
	}

	client.opList.Add(req)
	opaque := req.Opaque

	originalValue := []byte(`{"foo":"bar"}`)
	compressedValue := snappy.Encode(nil, originalValue)

	pkt := memd.AcquirePacket()
	pkt.Magic = memd.CmdMagicReq
	pkt.Command = memd.CmdDcpMutation
	pkt.Opaque = opaque
	pkt.Datatype = uint8(memd.DatatypeFlagJSON) | uint8(memd.DatatypeFlagCompressed)
	pkt.Value = compressedValue

	resp := &memdQResponse{Packet: pkt}

	client.resolveRequest(resp)

	s.True(result.called, "callback was not invoked")
	s.NoError(result.err)
	s.Equal(string(originalValue), string(result.value), "expected decompressed value")
	s.Zero(result.datatype&uint8(memd.DatatypeFlagCompressed), "expected compressed flag to be cleared")
}

// TestResolveRequestDecompressionDisabled verifies that when decompression is
// disabled, the compressed flag and value are preserved as-is (including empty).
func (s *UnitTestSuite) TestResolveRequestDecompressionDisabled() {
	client := newTestMemdClient(true)

	var result callbackResult

	req := &memdQRequest{
		Packet: memd.Packet{
			Magic:   memd.CmdMagicReq,
			Command: memd.CmdDcpMutation,
			Key:     []byte("testDoc"),
		},
		Persistent: true,
		Callback: func(resp *memdQResponse, req *memdQRequest, err error) {
			result.called = true
			result.err = err
			if resp != nil {
				result.value = append([]byte(nil), resp.Value...)
				result.datatype = resp.Datatype
			}
		},
	}

	client.opList.Add(req)
	opaque := req.Opaque

	pkt := memd.AcquirePacket()
	pkt.Magic = memd.CmdMagicReq
	pkt.Command = memd.CmdDcpMutation
	pkt.Opaque = opaque
	pkt.Datatype = uint8(memd.DatatypeFlagJSON) | uint8(memd.DatatypeFlagCompressed)
	pkt.Value = nil // empty value

	resp := &memdQResponse{Packet: pkt}

	client.resolveRequest(resp)

	s.True(result.called, "callback was not invoked")
	s.NoError(result.err)
	// With decompression disabled, the compressed flag should be preserved.
	s.NotZero(result.datatype&uint8(memd.DatatypeFlagCompressed), "expected compressed flag to be preserved when decompression is disabled")
}
