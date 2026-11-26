package gocbcore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	telemetryWebsocketClientMinBackoff          = 100 * time.Millisecond
	telemetryWebsocketClientDefaultMaxBackoff   = 1 * time.Hour
	telemetryWebsocketClientDefaultPingInterval = 30 * time.Second
	telemetryWebsocketClientDefaultPingTimeout  = 2 * time.Second
	telemetryWebsocketClientHandshakeTimeout    = 45 * time.Second
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

	var candidates []string
	for _, ep := range e.epList {
		if ep.Address != excludeAddress {
			candidates = append(candidates, ep.Address)
		}
	}

	if len(candidates) == 0 {
		// This means that we only have a single endpoint or multiple endpoints exist but have the same address.
		// Multiple endpoints with the same address is possible in load balanced environments.
		return e.epList[0].Address, true
	}

	return candidates[rand.Intn(len(candidates))], true // #nosec G404
}

func (e *telemetryEndpoints) createTLSConfig(address string) (*tls.Config, error) {
	if e.tlsConfig == nil {
		return nil, nil
	}

	return e.tlsConfig.MakeForAddr(trimSchemePrefix(address))
}

func (e *telemetryEndpoints) getAuthHeader(address string) (http.Header, error) {
	creds, err := e.auth.Credentials(AuthCredsRequest{Service: MgmtService, Endpoint: address})
	if err != nil {
		return nil, err
	}
	if len(creds) != 1 {
		return nil, errInvalidCredentials
	}

	if creds[0].Username == "" && creds[0].Password == "" {
		return http.Header{}, nil
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

	maxBackoff   time.Duration
	pingInterval time.Duration
	pingTimeout  time.Duration

	getMetricsFn func() string

	endpointLookupMutex sync.Mutex
	lastEndpointAddress string
	connInitiated       atomic.Bool // This is set to true if there is an active connection attempt loop in progress.

	shutdownSig  chan struct{}
	shutdownOnce sync.Once
}

func newTelemetryWebsocketClient(maxBackoff, pingInterval, pingTimeout time.Duration, getMetricsFn func() string) *telemetryWebsocketClient {
	if maxBackoff == time.Duration(0) {
		maxBackoff = telemetryWebsocketClientDefaultMaxBackoff
	}
	if pingInterval == time.Duration(0) {
		pingInterval = telemetryWebsocketClientDefaultPingInterval
	}
	if pingTimeout == time.Duration(0) {
		pingTimeout = telemetryWebsocketClientDefaultPingTimeout
	}

	w := &telemetryWebsocketClient{
		maxBackoff:   maxBackoff,
		pingInterval: pingInterval,
		pingTimeout:  pingTimeout,
		getMetricsFn: getMetricsFn,
		shutdownSig:  make(chan struct{}),
	}

	return w
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
	props, ok := w.createConnectionAttemptProps(true)
	if !ok {
		return
	}

	go w.connectionLoop(props)
}

type telemetryWebsocketClientConnectionProps struct {
	address   string
	header    http.Header
	tlsConfig *tls.Config
}

func (w *telemetryWebsocketClient) createConnectionAttemptProps(withShouldConnectCheck bool) (telemetryWebsocketClientConnectionProps, bool) {
	w.endpointLookupMutex.Lock()
	defer w.endpointLookupMutex.Unlock()

	// If this function is called by an active connection attempt loop, as part of a retry, we shouldn't do this check.
	if withShouldConnectCheck {
		shouldConnect := w.connInitiated.CompareAndSwap(false, true)
		if !shouldConnect {
			return telemetryWebsocketClientConnectionProps{}, false
		}
	}

	address, ok := w.endpoints.selectEndpoint(w.lastEndpointAddress)
	if !ok {
		logDebugf("No app telemetry endpoints available")
		w.connInitiated.Store(false)
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

	tlsConfig, err := w.endpoints.createTLSConfig(address)
	if err != nil {
		w.connInitiated.Store(false)
		logWarnf("Failed to create TLS config for telemetry reporting: %v", err)
		return telemetryWebsocketClientConnectionProps{}, false
	}

	w.lastEndpointAddress = address

	return telemetryWebsocketClientConnectionProps{
		address:   address,
		header:    header,
		tlsConfig: tlsConfig,
	}, true
}

func (w *telemetryWebsocketClient) connectionLoop(props telemetryWebsocketClientConnectionProps) {
	defer w.connInitiated.Store(false)

	// Cancel the context when the client is closed, so that in-flight dials, reads, writes and pings are aborted.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-w.shutdownSig:
			cancel()
		case <-ctx.Done():
		}
	}()

	backoffCalculator := ExponentialBackoff(telemetryWebsocketClientMinBackoff, w.maxBackoff, 2)
	var retryAttempts uint32 = 0

	for {
		if w.connectAndServe(ctx, props) {
			retryAttempts = 0 // We created a connection successfully, reset exponential backoff
		}
		if ctx.Err() != nil {
			return
		}

		backoff := backoffCalculator(retryAttempts)
		retryAttempts++
		logInfof("App telemetry connection closed. Retrying in %s", backoff)

		select {
		case <-ctx.Done():
			logInfof("App telemetry websocket client shutting down, canceling connection attempt")
			return
		case <-time.After(backoff):
		}

		var ok bool
		props, ok = w.createConnectionAttemptProps(false)
		if !ok {
			return
		}
	}
}

func (w *telemetryWebsocketClient) connectAndServe(ctx context.Context, props telemetryWebsocketClientConnectionProps) bool {
	conn, err := w.dialConn(ctx, props)
	if err != nil {
		if ctx.Err() != nil {
			logInfof("App telemetry websocket client shutting down, canceled connection attempt")
			return false
		}
		logWarnf("Failed to establish websocket connection for telemetry reporter: %v", err)
		return false
	}

	logDebugf("Starting app telemetry listener. Address=%s", props.address)
	pumpErr := w.readWritePump(ctx, conn) // The read-write pump always exits with an error.
	logDebugf("Error from app telemetry readWritePump: %s.", pumpErr)
	closeErr := conn.CloseNow()
	if closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
		logInfof("Got error when closing app telemetry websocket connection: %v", closeErr)
	} else if ctx.Err() != nil {
		logInfof("Closed app telemetry websocket connection")
	}
	return true
}

func (w *telemetryWebsocketClient) dialConn(ctx context.Context, props telemetryWebsocketClientConnectionProps) (*websocket.Conn, error) {
	logDebugf("Connecting to app telemetry endpoint: dialing %s", props.address)

	ctx, cancel := context.WithTimeout(ctx, telemetryWebsocketClientHandshakeTimeout)
	defer cancel()

	transport := &http.Transport{
		TLSClientConfig: props.tlsConfig,
	}
	// If the server rejects the upgrade (e.g. 401), the connection is returned to this transport's idle pool. The
	// transport is not reused, so close any idle connections. A successful upgrade takes the connection out of the
	// pool, so this does not affect the websocket connection.
	defer transport.CloseIdleConnections()

	opts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		HTTPHeader:      props.header,
		CompressionMode: websocket.CompressionDisabled,
	}

	conn, _, err := websocket.Dial(ctx, props.address, opts) //nolint:bodyclose
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (w *telemetryWebsocketClient) readWritePump(ctx context.Context, conn *websocket.Conn) error {
	pingLoopCtx, cancel := context.WithCancel(ctx)
	pingLoopDone := w.startPingTicker(pingLoopCtx, conn)
	defer func() {
		cancel()
		<-pingLoopDone
	}()

	for {
		_, message, err := conn.Read(ctx)
		if err != nil {
			return wrapError(err, "Error reading from app telemetry websocket")
		}
		if len(message) == 0 {
			// The message must contain at least the command byte. Treat this as a protocol error and reconnect.
			return wrapError(errProtocol, "Received empty message from app telemetry websocket")
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
		err = conn.Write(ctx, websocket.MessageBinary, resp.encode())
		if err != nil {
			return wrapError(err, "Error writing to app telemetry websocket")
		}
	}
}

func (w *telemetryWebsocketClient) startPingTicker(ctx context.Context, conn *websocket.Conn) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)

		ticker := time.NewTicker(w.pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, cancel := context.WithTimeout(ctx, w.pingTimeout)
				err := conn.Ping(pingCtx)
				cancel()
				if err != nil {
					if ctx.Err() != nil {
						// The pump has exited or the client is closing, the connection is being torn down anyway.
						return
					}
					logDebugf("Error pinging telemetry reporter websocket, closing connection: %v", err)
					closeErr := conn.CloseNow()
					if closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
						logInfof("Got error when closing app telemetry websocket connection: %v", closeErr)
					}
					return
				}
			}
		}
	}()

	return done
}

func (w *telemetryWebsocketClient) Close() {
	logDebugf("Closing app telemetry websocket client")
	w.shutdownOnce.Do(func() {
		close(w.shutdownSig)
	})
}
