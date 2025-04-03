package gocbcore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
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

	shutdownSig chan struct{}
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
	props, ok := w.createConnectionAttemptProps()
	if !ok {
		return
	}

	go w.connect(props)
}

type telemetryWebsocketClientConnectionProps struct {
	address string
	dialer  *websocket.Dialer
	header  http.Header
}

func (w *telemetryWebsocketClient) createConnectionAttemptProps() (telemetryWebsocketClientConnectionProps, bool) {
	w.endpointLookupMutex.Lock()
	defer w.endpointLookupMutex.Unlock()

	address, ok := w.endpoints.selectEndpoint(w.lastEndpointAddress)
	if !ok {
		logDebugf("No app telemetry endpoints available")
		return telemetryWebsocketClientConnectionProps{}, false
	}

	shouldConnect := w.connInitiated.CompareAndSwap(false, true)
	if !shouldConnect {
		return telemetryWebsocketClientConnectionProps{}, false
	}

	var header http.Header
	var err error
	if !w.fixedEndpoints {
		header, err = w.endpoints.getAuthHeader(address)
		if err != nil {
			w.connInitiated.Store(false)
			logWarnf("Failed to create auth header for telemetry reporting: %v", err)
			return telemetryWebsocketClientConnectionProps{}, false
		}
	}

	dialer, err := w.endpoints.createDialer(address)
	if err != nil {
		w.connInitiated.Store(false)
		logWarnf("Failed to create websocket dialer for telemetry reporting: %v", err)
		return telemetryWebsocketClientConnectionProps{}, false
	}

	w.lastEndpointAddress = address

	return telemetryWebsocketClientConnectionProps{
		address: address,
		dialer:  dialer,
		header:  header,
	}, true
}

func (w *telemetryWebsocketClient) connectionAttempt(props telemetryWebsocketClientConnectionProps) (retry bool) {
	defer w.connInitiated.Store(false)

	{
		ctx, cancel := context.WithCancel(context.Background())
		stopCh := make(chan struct{})
		go func() {
			select {
			case <-w.shutdownSig:
				cancel()
			case <-stopCh:
			}
		}()

		conn, err := w.dialConn(ctx, props.address, props.header, props.dialer)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// The client has been closed, don't attempt to reconnect.
				return
			}
			logWarnf("Failed to establish websocket connection for telemetry reporting, retrying in %s: %v", w.backoff, err)
			close(stopCh)
			return true
		}
		close(stopCh)
		w.conn = conn
	}

	var clientClosedConnection atomic.Bool
	stopCh := make(chan struct{})
	go func() {
		select {
		case <-w.shutdownSig:
			clientClosedConnection.Store(true)
			w.conn.Close()
		case <-stopCh:
		}
	}()

	err := w.readWritePump() // This will always return an error – even if err could be nil the behaviour is the same.
	if clientClosedConnection.Load() {
		w.conn = nil
		logInfof("App telemetry websocket connection closed")
		return false
	}
	close(stopCh)
	w.conn.Close()
	w.conn = nil
	logDebugf("Error from readWritePump: %s. Attempting to reconnect to app telemetry websocket in %s.", err, w.backoff)
	return true
}

func (w *telemetryWebsocketClient) connect(props telemetryWebsocketClientConnectionProps) {
	for {
		retry := w.connectionAttempt(props)
		if !retry {
			return
		}

		select {
		case <-w.shutdownSig:
			logDebugf("Telemetry reporter shutting down, canceling connection attempt")
			return
		case <-time.After(w.backoff):
		}

		var ok bool
		props, ok = w.createConnectionAttemptProps()
		if !ok {
			return
		}
	}
}

func (w *telemetryWebsocketClient) dialConn(ctx context.Context, address string, header http.Header, dialer *websocket.Dialer) (*websocket.Conn, error) {
	logDebugf("Connecting to app telemetry endpoint: dialing %s", address)
	conn, _, err := dialer.DialContext(ctx, address, header) // nolint: bodyclose
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (w *telemetryWebsocketClient) readWritePump() error {
	pingStopCh := make(chan struct{})
	defer close(pingStopCh)

	err := w.startPingTicker(pingStopCh)
	if err != nil {
		return err
	}

	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			return wrapError(err, "Error reading from app telemetry websocket")
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
			return wrapError(err, "Error writing to app telemetry websocket")
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
	logInfof("Closing app telemetry websocket client")
	close(w.shutdownSig)
}
