package gocbcore

import (
	"context"
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

func (suite *UnitTestSuite) newTelemetryTestServer(handler func(conn *websocket.Conn, r *http.Request)) (*httptest.Server, string) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(rw, r, nil)
		if !suite.Assert().NoError(err) {
			return
		}
		defer func() {
			_ = conn.CloseNow()
		}()

		handler(conn, r)
	}))

	return srv, strings.Replace(srv.URL, "http://", "ws://", 1)
}

func (suite *UnitTestSuite) closeTelemetryClient(client *telemetryWebsocketClient) {
	client.Close()
	suite.Require().Eventually(func() bool { return !client.connectionInitiated() }, 5*time.Second, 10*time.Millisecond)
}

func (suite *UnitTestSuite) TestTelemetryWebsocketClientRequestResponse() {
	type result struct {
		authHeader string
		responses  [][]byte
	}
	resultCh := make(chan result, 1)

	srv, address := suite.newTelemetryTestServer(func(conn *websocket.Conn, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		const unknownTelemetryCommand = 0x7f

		res := result{authHeader: r.Header.Get("Authorization")}
		for _, cmd := range []byte{byte(telemetryCommandGetTelemetry), byte(unknownTelemetryCommand)} {
			if err := conn.Write(ctx, websocket.MessageBinary, []byte{cmd}); err != nil {
				return
			}
			typ, msg, err := conn.Read(ctx)
			if err != nil {
				return
			}
			suite.Assert().Equal(websocket.MessageBinary, typ)
			res.responses = append(res.responses, msg)
		}
		resultCh <- res
	})
	defer srv.Close()

	client := newTelemetryWebsocketClient(time.Second, time.Hour, time.Second, func() string {
		return "some_metric 1"
	})
	defer suite.closeTelemetryClient(client)
	client.endpoints = telemetryEndpoints{
		epList: []routeEndpoint{{Address: address}},
		auth:   PasswordAuthProvider{Username: "user", Password: "pass"},
	}
	client.connectIfNotStarted()

	select {
	case res := <-resultCh:
		suite.Assert().Equal("Basic "+base64.StdEncoding.EncodeToString([]byte("user:pass")), res.authHeader)
		suite.Require().Len(res.responses, 2)
		suite.Assert().Equal(append([]byte{byte(telemetryStatusSuccess)}, []byte("some_metric 1")...), res.responses[0])
		suite.Assert().Equal([]byte{byte(telemetryStatusUnknownCommand)}, res.responses[1])
	case <-time.After(10 * time.Second):
		suite.T().Fatal("timed out waiting for telemetry responses")
	}
}

func (suite *UnitTestSuite) TestTelemetryWebsocketClientReconnectsWhenPongsNotReceived() {
	var connections atomic.Int32
	serverDone := make(chan struct{})
	srv, address := suite.newTelemetryTestServer(func(conn *websocket.Conn, r *http.Request) {
		connections.Add(1)
		// Never read from the connection, so pings are never answered.
		<-serverDone
	})
	defer srv.Close()
	defer close(serverDone)

	client := newTelemetryWebsocketClient(100*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond, func() string { return "" })
	defer suite.closeTelemetryClient(client)
	client.fixedEndpoints = true
	client.endpoints = telemetryEndpoints{epList: []routeEndpoint{{Address: address}}}
	client.connectIfNotStarted()

	suite.Require().Eventually(func() bool { return connections.Load() >= 2 }, 5*time.Second, 10*time.Millisecond)

	// Closing the client while it is reconnecting (connected or in backoff) should stop the connection loop.
	client.Close()
	suite.Assert().Eventually(func() bool { return !client.connectionInitiated() }, 5*time.Second, 10*time.Millisecond)
}

func (suite *UnitTestSuite) TestTelemetryWebsocketClientCloseClosesConnection() {
	connClosed := make(chan error, 1)
	connOpened := make(chan struct{})
	srv, address := suite.newTelemetryTestServer(func(conn *websocket.Conn, r *http.Request) {
		// Do a request/response round trip, so we know the client has finished dialing and is in its read loop.
		if err := conn.Write(context.Background(), websocket.MessageBinary, []byte{byte(telemetryCommandGetTelemetry)}); err != nil {
			return
		}
		if _, _, err := conn.Read(context.Background()); err != nil {
			return
		}
		close(connOpened)

		_, _, err := conn.Read(context.Background())
		connClosed <- err
	})
	defer srv.Close()

	client := newTelemetryWebsocketClient(time.Second, time.Hour, time.Second, func() string { return "" })
	client.fixedEndpoints = true
	client.endpoints = telemetryEndpoints{epList: []routeEndpoint{{Address: address}}}
	client.connectIfNotStarted()

	select {
	case <-connOpened:
	case <-time.After(5 * time.Second):
		suite.T().Fatal("timed out waiting for connection")
	}

	client.Close()
	// Closing twice must be safe.
	client.Close()

	select {
	case err := <-connClosed:
		// The client closes the connection without a close handshake.
		suite.Assert().Error(err)
		suite.Assert().Equal(websocket.StatusCode(-1), websocket.CloseStatus(err))
	case <-time.After(5 * time.Second):
		suite.T().Fatal("timed out waiting for connection to be closed by client")
	}
	suite.Assert().Eventually(func() bool { return !client.connectionInitiated() }, 5*time.Second, 10*time.Millisecond)
}

func (suite *UnitTestSuite) TestTelemetryWebsocketClientRejectedUpgrade() {
	globalTestLogger.SuppressWarnings(true)
	defer globalTestLogger.SuppressWarnings(false)

	// The server rejects the upgrade with a short response, which net/http would normally keep alive in the client
	// transport's idle pool. Each attempt uses a new transport, so those connections must be closed rather than left
	// open (along with their goroutines).
	var attempts, openConns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(rw, "unauthorized", http.StatusUnauthorized)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			openConns.Add(1)
		case http.StateClosed, http.StateHijacked:
			openConns.Add(-1)
		}
	}
	srv.Start()
	defer srv.Close()

	client := newTelemetryWebsocketClient(100*time.Millisecond, time.Hour, time.Second, func() string { return "" })
	client.fixedEndpoints = true
	client.endpoints = telemetryEndpoints{epList: []routeEndpoint{{Address: strings.Replace(srv.URL, "http://", "ws://", 1)}}}
	client.connectIfNotStarted()

	suite.Require().Eventually(func() bool { return attempts.Load() >= 3 }, 5*time.Second, 10*time.Millisecond)
	client.Close()
	suite.Require().Eventually(func() bool { return !client.connectionInitiated() }, 5*time.Second, 10*time.Millisecond)

	// The client must have closed every connection it opened.
	suite.Assert().Eventually(func() bool { return openConns.Load() == 0 }, 2*time.Second, 10*time.Millisecond)
}

func (suite *UnitTestSuite) TestTelemetryWebsocketClientEmptyMessageReconnects() {
	var connections atomic.Int32
	firstConnClosedByClient := make(chan bool, 1)
	secondConnResponse := make(chan []byte, 1)

	srv, address := suite.newTelemetryTestServer(func(conn *websocket.Conn, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		switch connections.Add(1) {
		case 1:
			// An empty message is a protocol error, the client should close the connection.
			if err := conn.Write(ctx, websocket.MessageBinary, []byte{}); err != nil {
				return
			}
			_, _, err := conn.Read(ctx)
			firstConnClosedByClient <- err != nil && ctx.Err() == nil
		case 2:
			// After reconnecting, the client should serve requests as normal.
			if err := conn.Write(ctx, websocket.MessageBinary, []byte{byte(telemetryCommandGetTelemetry)}); err != nil {
				return
			}
			_, msg, err := conn.Read(ctx)
			if err != nil {
				return
			}
			secondConnResponse <- msg
		}
	})
	defer srv.Close()

	client := newTelemetryWebsocketClient(100*time.Millisecond, time.Hour, time.Second, func() string { return "some_metric 1" })
	defer suite.closeTelemetryClient(client)
	client.fixedEndpoints = true
	client.endpoints = telemetryEndpoints{epList: []routeEndpoint{{Address: address}}}
	client.connectIfNotStarted()

	select {
	case closed := <-firstConnClosedByClient:
		suite.Assert().True(closed, "expected the client to close the connection after an empty message")
	case <-time.After(5 * time.Second):
		suite.T().Fatal("timed out waiting for the client to close the first connection")
	}

	select {
	case msg := <-secondConnResponse:
		suite.Assert().Equal(append([]byte{byte(telemetryStatusSuccess)}, []byte("some_metric 1")...), msg)
	case <-time.After(5 * time.Second):
		suite.T().Fatal("timed out waiting for the client to reconnect and respond")
	}
}
