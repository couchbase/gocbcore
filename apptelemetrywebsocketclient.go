package gocbcore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type telemetryCommand uint8

const (
	telemetryCommandGetTelemetry = telemetryCommand(0x00)
)

type telemetryStatus uint8

const (
	telemetryStatusSuccess        = telemetryStatus(0x00)
	telemetryStatusUnknownCommand = telemetryStatus(0x01)
)

type telemetryResponse struct {
	status telemetryStatus
	data   []byte
}

func (r *telemetryResponse) encode() []byte {
	encoded := r.data
	encoded = append(encoded, byte(0))
	copy(encoded[1:], encoded)
	encoded[0] = byte(r.status)
	return encoded
}

type telemetryEndpoints struct {
	epList    []routeEndpoint
	tlsConfig *dynTLSConfig
	auth      AuthProvider
}

func (e *telemetryEndpoints) selectEndpoint(excludeAddress string) (string, bool) {
	if len(e.epList) == 0 {
		return "", false
	}
	if len(e.epList) == 1 {
		return e.epList[0].Address, true
	}

	var candidates []string
	for _, ep := range e.epList {
		if ep.Address != excludeAddress {
			candidates = append(candidates, ep.Address)
		}
	}
	return candidates[rand.Intn(len(candidates))], true // #nosec G404
}

func (e *telemetryEndpoints) createDialer(address string) (*websocket.Dialer, error) {
	var tlsConfig *tls.Config
	var err error

	if e.tlsConfig != nil {
		tlsConfig, err = e.tlsConfig.MakeForAddr(trimSchemePrefix(address))
		if err != nil {
			return nil, err
		}
	}

	return &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
		TLSClientConfig:  tlsConfig,
	}, nil
}

func (e *telemetryEndpoints) getAuthHeader(address string) (http.Header, error) {
	creds, err := e.auth.Credentials(AuthCredsRequest{Service: MgmtService, Endpoint: address})
	if err != nil {
		return nil, err
	}
	if len(creds) != 1 {
		return nil, errInvalidCredentials
	}

	return http.Header{
		"Authorization": []string{
			"Basic " + base64.StdEncoding.EncodeToString(
				[]byte(fmt.Sprintf("%s:%s", creds[0].Username, creds[0].Password))),
		},
	}, nil
}

type telemetryWebsocketClient struct {
	endpoints      telemetryEndpoints
	fixedEndpoints bool

	backoff      time.Duration
	pingInterval time.Duration
	pingTimeout  time.Duration

	getMetricsFn func() string

	conn                *websocket.Conn
	endpointLookupMutex sync.Mutex
	lastEndpointAddress string
	connInitiated       atomic.Bool

	shutdownSig   chan struct{}
	parentContext context.Context
}

func newTelemetryWebsocketClient(backoff, pingInterval, pingTimeout time.Duration, getMetricsFn func() string) *telemetryWebsocketClient {
	if backoff == time.Duration(0) {
		backoff = 5 * time.Second
	}
	if pingInterval == time.Duration(0) {
		pingInterval = 30 * time.Second
	}
	if pingTimeout == time.Duration(0) {
		pingTimeout = 5 * time.Second
	}

	w := &telemetryWebsocketClient{
		backoff:      backoff,
		pingInterval: pingInterval,
		pingTimeout:  pingTimeout,
		getMetricsFn: getMetricsFn,
		shutdownSig:  make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-w.shutdownSig
		cancel()
	}()

	w.parentContext = ctx

	return w
}

func (w *telemetryWebsocketClient) usesExternalEndpoint() bool {
	return w.fixedEndpoints
}

func (w *telemetryWebsocketClient) connectionInitiated() bool {
	return w.connInitiated.Load()
}

func (w *telemetryWebsocketClient) updateEndpoints(endpoints telemetryEndpoints) {
	if w.fixedEndpoints {
		// We shouldn't get here (telemetry components will not be listening to config updates in this case), but if
		// we do then ignore the update.
		return
	}

	w.endpointLookupMutex.Lock()
	w.endpoints = endpoints
	w.endpointLookupMutex.Unlock()
}

func (w *telemetryWebsocketClient) connectIfNotStarted() {
	w.endpointLookupMutex.Lock()
	defer w.endpointLookupMutex.Unlock()

	address, ok := w.endpoints.selectEndpoint(w.lastEndpointAddress)
	if !ok {
		return
	}

	shouldConnect := w.connInitiated.CompareAndSwap(false, true)
	if !shouldConnect {
		return
	}

	var header http.Header
	var err error
	if !w.fixedEndpoints {
		header, err = w.endpoints.getAuthHeader(address)
		if err != nil {
			w.connInitiated.Store(false)
			logWarnf("Failed to create auth header for telemetry reporting: %v", err)
			return
		}
	}

	dialer, err := w.endpoints.createDialer(address)
	if err != nil {
		w.connInitiated.Store(false)
		logWarnf("Failed to create websocket dialer for telemetry reporting: %v", err)
		return
	}

	w.lastEndpointAddress = address

	go w.connect(address, header, dialer)
}

func (w *telemetryWebsocketClient) connect(address string, header http.Header, dialer *websocket.Dialer) {
	conn, err := w.dialConn(address, header, dialer)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// The client has been closed, don't attempt to reconnect.
			w.connInitiated.Store(false)
			return
		}
		logWarnf("Failed to establish websocket connection for telemetry reporting, retrying in %s: %v", w.backoff, err)

		w.reconnect()
		return
	}
	w.conn = conn

	go func() {
		err := w.readWritePump()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			logDebugf("Error from readWritePump: %s. Attempting to reconnect to app telemetry websocket in %s.", err, w.backoff)
			w.conn = nil
			w.connInitiated.Store(false)
			w.reconnect()
		}
	}()
}

func (w *telemetryWebsocketClient) reconnect() {
	select {
	case <-w.shutdownSig:
		logDebugf("Telemetry reporter shutting down, canceling connection attempt")
		return
	case <-time.After(w.backoff):
	}

	w.endpointLookupMutex.Lock()

	address, ok := w.endpoints.selectEndpoint(w.lastEndpointAddress)
	if !ok {
		w.connInitiated.Store(false)
		return
	}

	var header http.Header
	var err error
	if !w.fixedEndpoints {
		header, err = w.endpoints.getAuthHeader(address)
		if err != nil {
			w.connInitiated.Store(false)
			logWarnf("Failed to create auth header for telemetry reporting: %v", err)
			return
		}
	}

	dialer, err := w.endpoints.createDialer(address)
	if err != nil {
		w.connInitiated.Store(false)
		logWarnf("Failed to create websocket dialer for telemetry reporting: %v", err)
		return
	}

	w.lastEndpointAddress = address
	w.endpointLookupMutex.Unlock()
	w.connect(address, header, dialer)
}

func (w *telemetryWebsocketClient) dialConn(address string, header http.Header, dialer *websocket.Dialer) (*websocket.Conn, error) {
	logDebugf("Connecting to app telemetry endpoint: dialing %s", address)
	conn, _, err := dialer.DialContext(w.parentContext, address, header) // nolint: bodyclose
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (w *telemetryWebsocketClient) readWritePump() error {
	pingStopCh := make(chan struct{})
	defer close(pingStopCh)
	defer w.conn.Close()

	err := w.startPingTicker(pingStopCh)
	if err != nil {
		return err
	}

	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			logInfof("Error reading from telemetry reporter websocket: %v", err)
			return err
		}
		cmd := telemetryCommand(message[0])

		var resp telemetryResponse
		switch cmd {
		case telemetryCommandGetTelemetry:
			logSchedf("Received GET_TELEMETRY command from server telemetry collector")
			resp.status = telemetryStatusSuccess
			metrics := w.getMetricsFn()
			resp.data = []byte(metrics)
		default:
			logSchedf("Received unknown command from server telemetry collector")
			resp.status = telemetryStatusUnknownCommand
		}
		logSchedf("Sending telemetry response to server telemetry collector. Size=%d bytes", len(resp.data))
		err = w.conn.WriteMessage(websocket.BinaryMessage, resp.encode())
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			logInfof("Error writing to telemetry reporter websocket: %v", err)
			return err
		}
	}
}

func (w *telemetryWebsocketClient) startPingTicker(stopCh chan struct{}) error {
	err := w.conn.SetReadDeadline(time.Now().Add(w.pingInterval + w.pingTimeout))
	if err != nil {
		return wrapError(err, "Could not update read deadline")
	}

	lastPingTimestamp := time.Now().UnixMilli()

	w.conn.SetPongHandler(func(string) error {
		err := w.conn.SetReadDeadline(time.UnixMilli(atomic.LoadInt64(&lastPingTimestamp)).Add(w.pingInterval + w.pingTimeout))
		if err != nil {
			return wrapError(err, "Could not update read deadline in pong handler")
		}
		return nil
	})

	go func() {
		ticker := time.NewTicker(w.pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				atomic.StoreInt64(&lastPingTimestamp, time.Now().UnixMilli())
				err := w.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(w.pingTimeout))
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					logInfof("Error writing PING message telemetry reporter websocket: %v", err)
					// No need to take any action on this error - reads will time out if we are unable to send pings, as
					// the read deadline will not be increased.
					return
				}
			}
		}
	}()

	return nil
}

func (w *telemetryWebsocketClient) Close() {
	close(w.shutdownSig)
}
