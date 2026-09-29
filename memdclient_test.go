package gocbcore

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
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
	client.canonicalAddress.Store("127.0.0.1:11210")
	return client
}

// blockingMemdConn is a variant of fakeMemdConn where ReadPacket blocks until the connection is dropped either by the
// client, or by dropByServer which simulates the peer closing socket.
type blockingMemdConn struct {
	fakeMemdConn
	unblockCh             chan struct{}
	simulateServerCloseCh chan struct{}
}

func newBlockingMemdConn() *blockingMemdConn {
	return &blockingMemdConn{
		unblockCh:             make(chan struct{}),
		simulateServerCloseCh: make(chan struct{}),
	}
}

func (c *blockingMemdConn) unblock() {
	close(c.unblockCh)
}

func (c *blockingMemdConn) simulateServerSocketClose() {
	close(c.simulateServerCloseCh)
}

func (c *blockingMemdConn) ReadPacket() (*memd.Packet, int, error) {
	select {
	case <-c.unblockCh:
		return nil, 0, io.EOF
	case <-c.simulateServerCloseCh:
		return nil, 0, io.EOF
	}
}

func (c *blockingMemdConn) Close() error {
	c.unblock()
	return nil
}

func (suite *UnitTestSuite) TestMemdClientSocketCloseRequestError() {
	type testCase struct {
		name                   string
		trigger                func(*memdClient, *blockingMemdConn) error
		requestErrorValidation func(*UnitTestSuite, error)
	}

	testCases := []testCase{
		{
			name: "server closes connection",
			trigger: func(client *memdClient, conn *blockingMemdConn) error {
				conn.simulateServerSocketClose()
				return nil
			},
			requestErrorValidation: func(suite *UnitTestSuite, err error) {
				suite.Assert().ErrorIs(err, io.EOF)
				suite.Assert().ErrorIs(err, ErrSocketClosed)
				suite.Assert().NotErrorIs(err, ErrSocketClosedByClient)
			},
		},
		{
			name: "client closes connection",
			trigger: func(client *memdClient, conn *blockingMemdConn) error {
				return client.Close()
			},
			requestErrorValidation: func(suite *UnitTestSuite, err error) {
				suite.Assert().ErrorIs(err, io.EOF)
				suite.Assert().ErrorIs(err, ErrSocketClosed)
				suite.Assert().ErrorIs(err, ErrSocketClosedByClient)
			},
		},
	}

	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			fakeConn := newBlockingMemdConn()
			client := newTestMemdClient(true)
			client.conn = fakeConn

			errCh := make(chan error, 1)
			req := &memdQRequest{
				Packet: memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet},
				Callback: func(_ *memdQResponse, _ *memdQRequest, err error) {
					errCh <- err
				},
			}

			client.run()
			err := client.SendRequest(req)
			suite.Require().NoError(err)

			err = tc.trigger(client, fakeConn)
			suite.Require().NoError(err)

			select {
			case requestErr := <-errCh:
				tc.requestErrorValidation(suite, requestErr)
			case <-time.After(5 * time.Second):
				suite.Fail("request callback not called")
			}

		})
	}
}

func (suite *UnitTestSuite) TestResolveRequestCompressedFlagXattrOnlyValue() {
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

	// Xattr section only, uncompressed: 4-byte total length, then one
	// xattr of 4-byte length, key, NUL, value, NUL.
	pair := append([]byte("_sync\x00"), []byte(`{"rev":"1-a"}`+"\x00")...)
	entry := binary.BigEndian.AppendUint32(nil, uint32(len(pair)))
	entry = append(entry, pair...)
	xattrs := binary.BigEndian.AppendUint32(nil, uint32(len(entry)))
	xattrs = append(xattrs, entry...)

	pkt := memd.AcquirePacket()
	pkt.Magic = memd.CmdMagicReq
	pkt.Command = memd.CmdDcpMutation
	pkt.Opaque = req.Opaque
	pkt.Datatype = uint8(memd.DatatypeFlagJSON) | uint8(memd.DatatypeFlagCompressed) | uint8(memd.DatatypeFlagXattrs)
	pkt.Value = xattrs

	client.resolveRequest(&memdQResponse{Packet: pkt})

	suite.Assert().True(result.called)
	suite.Assert().NoError(result.err)
	suite.Assert().Equal(string(xattrs), string(result.value), "expected value to be the uncompressed xattrs")
}
