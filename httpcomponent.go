package gocbcore

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type httpComponentInterface interface {
	DoInternalHTTPRequest(req *httpRequest, skipConfigCheck bool) (*HTTPResponse, error)
}

type httpComponent struct {
	cli                  *http.Client
	muxer                *httpMux
	userAgent            string
	tracer               *tracerComponent
	telemetry            *telemetryComponent
	defaultRetryStrategy RetryStrategy

	shutdownSig chan struct{}
}

type httpComponentProps struct {
	UserAgent            string
	DefaultRetryStrategy RetryStrategy
}

type httpClientProps struct {
	connectTimeout      time.Duration
	maxIdleConns        int
	maxIdleConnsPerHost int
	maxConnsPerHost     int
	idleTimeout         time.Duration
}

func newHTTPComponent(props httpComponentProps, clientProps httpClientProps, muxer *httpMux, tracer *tracerComponent, telemetry *telemetryComponent) *httpComponent {
	hc := &httpComponent{
		muxer:                muxer,
		userAgent:            props.UserAgent,
		defaultRetryStrategy: props.DefaultRetryStrategy,
		tracer:               tracer,
		telemetry:            telemetry,
		shutdownSig:          make(chan struct{}),
	}

	hc.cli = hc.createHTTPClient(clientProps.maxIdleConns, clientProps.maxIdleConnsPerHost, clientProps.maxConnsPerHost, clientProps.idleTimeout,
		clientProps.connectTimeout)

	return hc
}

func (hc *httpComponent) Close() {
	close(hc.shutdownSig)

	if err := hc.muxer.Close(); err != nil {
		logDebugf("Error closing http muxer: %s", err)
	}
	if tsport, ok := hc.cli.Transport.(*http.Transport); ok {
		tsport.CloseIdleConnections()
	} else {
		logDebugf("Could not close idle connections for transport")
	}
}

func (hc *httpComponent) DoHTTPRequest(req *HTTPRequest, cb DoHTTPRequestCallback) (PendingOp, error) {
	tracer := hc.tracer.StartTelemeteryHandler(metricValueServiceHTTPValue, "http", req.TraceContext)

	retryStrategy := hc.defaultRetryStrategy
	if req.RetryStrategy != nil {
		retryStrategy = req.RetryStrategy
	}

	ctx, cancel := context.WithCancel(context.Background())

	ireq := &httpRequest{
		Service:          req.Service,
		Endpoint:         req.Endpoint,
		Method:           req.Method,
		Path:             req.Path,
		Headers:          req.Headers,
		ContentType:      req.ContentType,
		Username:         req.Username,
		Password:         req.Password,
		Body:             req.Body,
		IsIdempotent:     req.IsIdempotent,
		UniqueID:         req.UniqueID,
		Deadline:         req.Deadline,
		RetryStrategy:    retryStrategy,
		RootTraceContext: tracer.RootContext(),
		Context:          ctx,
		CancelFunc:       cancel,
		User:             req.User,
	}

	go func() {
		resp, err := hc.DoInternalHTTPRequest(ireq, false)
		if err != nil {
			cancel()
			if errors.Is(err, ErrRequestCanceled) {
				cb(nil, err)
				return
			}

			tracer.Finish()
			cb(nil, wrapHTTPError(ireq, err))
			return
		}

		tracer.Finish()
		cb(resp, nil)
	}()

	return ireq, nil
}

type httpRequestState struct {
	denylist              []string
	start                 time.Time
	lastAttemptStart      time.Time
	endpoint              routeEndpoint
	cancellationIsTimeout uint32
}

func (hc *httpComponent) DoInternalHTTPRequest(req *httpRequest, skipConfigCheck bool) (*HTTPResponse, error) {
	if req.Service == MemdService {
		return nil, errInvalidService
	}

	state := &httpRequestState{
		start: time.Now(),
	}

	// This creates a context that has a parent with no cancel function. As such WithCancel will not set up any
	// extra go routines, and we only need to call cancel on (non-timeout) failure.
	ctx := req.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, ctxCancel := context.WithCancel(ctx)

	doneCh := make(chan struct{}, 1)
	defer func() {
		doneCh <- struct{}{}
	}()

	// Having no deadline is a legitimate case.
	if !req.Deadline.IsZero() {
		go func() {
			select {
			case <-time.After(req.Deadline.Sub(state.start)):
				// We use cancellationIsTimeout instead of a context with a deadline, as we want the timeout to only
				// apply to the request-response part of the operation, not in the stream that we will return.
				atomic.StoreUint32(&state.cancellationIsTimeout, 1)
				ctxCancel()
			case <-hc.shutdownSig:
				ctxCancel()
			case <-doneCh:
			}
		}()
	} else {
		go func() {
			select {
			case <-hc.shutdownSig:
				ctxCancel()
			case <-doneCh:
			}
		}()
	}

	if !skipConfigCheck {
		if err := hc.waitForConfig(ctx, req.IsIdempotent, &state.cancellationIsTimeout); err != nil {
			ctxCancel()
			return nil, err
		}
	}

	generator := newHTTPRequestGenerator(ctx, req, hc.userAgent)

	for {
		resp, retry, err := hc.doHTTPRequestAttempt(req, generator, state)
		if err != nil {
			outcome := telemetryOutcomeError
			if errors.Is(err, ErrTimeout) {
				outcome = telemetryOutcomeTimedout
			} else if errors.Is(err, ErrRequestCanceled) {
				outcome = telemetryOutcomeCanceled
			}
			hc.recordTelemetry(state, req, outcome)
			ctxCancel()
			return nil, err
		}
		if retry {
			hc.recordTelemetry(state, req, telemetryOutcomeError)
			continue
		}
		hc.recordTelemetry(state, req, telemetryOutcomeSuccess)

		return resp, nil
	}
}

func (hc *httpComponent) doHTTPRequestAttempt(req *httpRequest, generator *httpRequestGenerator, state *httpRequestState) (*HTTPResponse, bool, error) {
	state.lastAttemptStart = time.Now()

	state.endpoint = routeEndpoint{
		Address:  req.Endpoint,
		NodeUUID: req.NodeUUID,
	}
	if state.endpoint.Address == "" {
		var err error
		state.endpoint, err = hc.randomEndpoint(req.Service, state.denylist)
		if err != nil {
			return nil, false, err
		}
	} else {
		err := hc.checkEndpointAddressExists(req.Service, state.endpoint.Address)
		if err != nil {
			return nil, false, err
		}
	}
	var creds []UserPassPair
	if req.Username == "" && req.Password == "" {
		auth := hc.muxer.Auth()
		if auth == nil {
			// Shouldn't happen but if it does then probably better to not panic with a nil pointer.
			return nil, false, errCliInternalError
		}

		var err error
		creds, err = auth.Credentials(AuthCredsRequest{
			Service:  req.Service,
			Endpoint: state.endpoint.Address,
		})
		if err != nil {
			if err := hc.maybeWait(req, CredentialsFetchFailedRetryReason, err, state.start, state.endpoint.Address, true); err != nil {
				return nil, false, err
			}
			state.denylist = append(state.denylist, state.endpoint.Address)

			// Retry
			return nil, true, nil
		}
	}

	hreq, err := generator.NewRequest(state.endpoint.Address, creds)
	if err != nil {
		return nil, false, err
	}

	dSpan := hc.tracer.StartHTTPDispatchSpan(req, spanNameDispatchToServer)
	logSchedf("Writing HTTP request to %s ID=%s", hreq.URL, req.UniqueID)
	// we can't close the body of this response as it's long-lived beyond the function
	hresp, err := hc.cli.Do(hreq) // nolint: bodyclose
	hc.tracer.StopHTTPDispatchSpan(dSpan, hreq, req.UniqueID, req.RetryAttempts())
	if err != nil {
		logDebugf("Received HTTP Response for ID=%s, errored: %v", req.UniqueID, err)

		// We check for net errors first, an i/o timeout can satisfy the DeadlineExceeded check and then we'd end
		// up returning an i/o timeout error to the user.
		var retryReason RetryReason
		if os.IsTimeout(err) {
			retryReason = SocketNotAvailableRetryReason
		} else if errors.Is(err, io.ErrUnexpectedEOF) {
			retryReason = SocketCloseInFlightRetryReason
		} else {
			var netErr *net.OpError
			if errors.As(err, &netErr) {
				// We need to be care about what we consider not available, if the request has been written to the
				// network then it's socket closed in flight. This isn't easy to figure out so err on the side of
				// caution.
				if netErr.Op == "dial" {
					retryReason = SocketNotAvailableRetryReason
				} else {
					retryReason = SocketCloseInFlightRetryReason
				}
			} else {
				var dnsErr *net.DNSError
				if errors.As(err, &dnsErr) {
					retryReason = SocketNotAvailableRetryReason
				}
			}
		}

		if retryReason != nil {
			err := hc.maybeWait(req, retryReason, err, state.start, state.endpoint.Address, false)
			if err != nil {
				return nil, false, err
			}

			// Retry
			return nil, true, nil
		}

		// Because we don't use the http request context itself to perform timeouts we need to do some translation
		// of the error message here for better UX.
		if errors.Is(err, context.Canceled) {
			isTimeout := atomic.LoadUint32(&state.cancellationIsTimeout)
			if isTimeout == 1 {
				var base error
				if req.IsIdempotent {
					base = errUnambiguousTimeout
				} else {
					base = errAmbiguousTimeout
				}
				err = &TimeoutError{
					InnerError:       base,
					OperationID:      "http",
					Opaque:           req.Identifier(),
					TimeObserved:     time.Since(state.start),
					RetryReasons:     req.retryReasons,
					RetryAttempts:    req.retryCount,
					LastDispatchedTo: state.endpoint.Address,
				}
			} else {
				err = errRequestCanceled
			}
		}

		// If we've got to here then either the error is ours timeout/canceled or we don't know it.
		return nil, false, err
	}
	logSchedf("Received HTTP Response for ID=%s, status=%d", req.UniqueID, hresp.StatusCode)

	hresp = wrapHttpResponse(hresp) // nolint: bodyclose

	respOut := HTTPResponse{
		Endpoint:      state.endpoint.Address,
		StatusCode:    hresp.StatusCode,
		ContentLength: hresp.ContentLength,
		Body:          hresp.Body,
	}

	return &respOut, false, nil
}

func (hc *httpComponent) recordTelemetry(state *httpRequestState, req *httpRequest, outcome telemetryOutcome) {
	if hc.telemetry == nil {
		return
	}
	switch req.Service {
	case N1qlService, FtsService, CbasService, MgmtService, EventingService:
		// We only report app telemetry metrics for these HTTP services
		break
	default:
		return
	}

	var node, altNode string
	if state.endpoint.CanonicalAddress != "" && state.endpoint.CanonicalAddress != state.endpoint.Address {
		node = hostnameFromURI(state.endpoint.CanonicalAddress)
		altNode = hostnameFromURI(state.endpoint.Address)
	} else {
		node = hostnameFromURI(state.endpoint.Address)
	}

	hc.telemetry.RecordOp(telemetryOperationAttributes{
		node:     node,
		altNode:  altNode,
		nodeUUID: state.endpoint.NodeUUID,
		duration: time.Since(state.lastAttemptStart),
		outcome:  outcome,
		service:  req.Service,
	})
}

func (hc *httpComponent) waitForConfig(ctx context.Context, isIdempotent bool, cancellationIsTimeout *uint32) error {
	for {
		revID, err := hc.muxer.ConfigRev()
		if err != nil {
			return err
		}

		if revID > -1 {
			return nil
		}

		// We've not successfully been setup with a cluster map yet
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, context.Canceled) {
				isTimeout := atomic.LoadUint32(cancellationIsTimeout)
				if isTimeout == 1 {
					if isIdempotent {
						return errUnambiguousTimeout
					}
					return errAmbiguousTimeout
				}

				return errRequestCanceled
			}

			return err
		case <-time.After(500 * time.Microsecond):
		}
	}
}

func (hc *httpComponent) randomEndpoint(service ServiceType, denylist []string) (routeEndpoint, error) {
	var endpoint routeEndpoint
	var err error
	switch service {
	case MgmtService:
		endpoint, err = hc.getMgmtEp(denylist)
	case CapiService:
		endpoint, err = hc.getCapiEp(denylist)
	case N1qlService:
		endpoint, err = hc.getN1qlEp(denylist)
	case FtsService:
		endpoint, err = hc.getFtsEp(denylist)
	case CbasService:
		endpoint, err = hc.getCbasEp(denylist)
	case EventingService:
		endpoint, err = hc.getEventingEp(denylist)
	case GSIService:
		endpoint, err = hc.getGSIEp(denylist)
	case BackupService:
		endpoint, err = hc.getBackupEp(denylist)
	}
	return endpoint, err
}

func (hc *httpComponent) checkEndpointAddressExists(service ServiceType, address string) error {
	var err error
	switch service {
	case MgmtService:
		err = hc.validateEndpointAddress(address, hc.muxer.MgmtEps())
	case CapiService:
		err = hc.validateEndpointAddress(address, hc.muxer.CapiEps())
	case N1qlService:
		err = hc.validateEndpointAddress(address, hc.muxer.N1qlEps())
	case FtsService:
		err = hc.validateEndpointAddress(address, hc.muxer.FtsEps())
	case CbasService:
		err = hc.validateEndpointAddress(address, hc.muxer.CbasEps())
	case EventingService:
		err = hc.validateEndpointAddress(address, hc.muxer.EventingEps())
	case GSIService:
		err = hc.validateEndpointAddress(address, hc.muxer.GSIEps())
	case BackupService:
		err = hc.validateEndpointAddress(address, hc.muxer.BackupEps())
	}
	return err
}

func (hc *httpComponent) maybeWait(req *httpRequest, retryReason RetryReason, err error, start time.Time, endpoint string, returnOriginalOnTimeout bool) error {
	shouldRetry, retryTime := retryOrchMaybeRetry(req, retryReason)
	if !shouldRetry {
		return err
	}

	select {
	case <-time.After(time.Until(retryTime)):
		// continue!
	case <-time.After(time.Until(req.Deadline)):
		if returnOriginalOnTimeout {
			if errors.Is(err, context.DeadlineExceeded) {
				err = &TimeoutError{
					InnerError:       errUnambiguousTimeout,
					OperationID:      "http",
					Opaque:           req.Identifier(),
					TimeObserved:     time.Since(start),
					RetryReasons:     req.retryReasons,
					RetryAttempts:    req.retryCount,
					LastDispatchedTo: endpoint,
				}
			}

			return err
		}

		return &TimeoutError{
			InnerError:       errUnambiguousTimeout,
			OperationID:      "http",
			Opaque:           req.Identifier(),
			TimeObserved:     time.Since(start),
			RetryReasons:     req.retryReasons,
			RetryAttempts:    req.retryCount,
			LastDispatchedTo: endpoint,
		}
	}

	return nil
}

func (hc *httpComponent) getMgmtEp(denylist []string) (routeEndpoint, error) {
	endpoints, err := randFromServiceEndpoints(hc.muxer.MgmtEps(), denylist)
	return endpoints, err
}

func (hc *httpComponent) getCapiEp(denylist []string) (routeEndpoint, error) {
	return randFromServiceEndpoints(hc.muxer.CapiEps(), denylist)
}

func (hc *httpComponent) getN1qlEp(denylist []string) (routeEndpoint, error) {
	return randFromServiceEndpoints(hc.muxer.N1qlEps(), denylist)
}

func (hc *httpComponent) getFtsEp(denylist []string) (routeEndpoint, error) {
	return randFromServiceEndpoints(hc.muxer.FtsEps(), denylist)
}

func (hc *httpComponent) getCbasEp(denylist []string) (routeEndpoint, error) {
	return randFromServiceEndpoints(hc.muxer.CbasEps(), denylist)
}

func (hc *httpComponent) getEventingEp(denylist []string) (routeEndpoint, error) {
	return randFromServiceEndpoints(hc.muxer.EventingEps(), denylist)
}

func (hc *httpComponent) getGSIEp(denylist []string) (routeEndpoint, error) {
	return randFromServiceEndpoints(hc.muxer.GSIEps(), denylist)
}

func (hc *httpComponent) getBackupEp(denylist []string) (routeEndpoint, error) {
	return randFromServiceEndpoints(hc.muxer.BackupEps(), denylist)
}

func (hc *httpComponent) validateEndpointAddress(endpointAddress string, endpoints []routeEndpoint) error {
	for _, ep := range endpoints {
		if ep.Address == endpointAddress {
			return nil
		}
	}

	return errInvalidServer
}

func createTLSConfig(auth AuthProvider, cipherSuite []*tls.CipherSuite, caProvider func() *x509.CertPool) *dynTLSConfig {
	var suites []uint16
	if cipherSuite != nil {
		suites = make([]uint16, len(cipherSuite))
		for i, suite := range cipherSuite {
			var s uint16
			for _, suiteID := range tls.CipherSuites() {
				if suite.Name == suiteID.Name {
					s = suiteID.ID
					break
				}
			}
			for _, suiteID := range tls.InsecureCipherSuites() {
				if suite.Name == suiteID.Name {
					s = suiteID.ID
					break
				}
			}

			if s > 0 {
				suites[i] = s
			} else {
				logWarnf("Unknown cipher suite %s, ignoring", suite.Name)
			}
		}
	}

	return &dynTLSConfig{
		BaseConfig: &tls.Config{
			GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, err := auth.Certificate(AuthCertRequest{})
				if err != nil {
					return nil, err
				}

				if cert == nil {
					return &tls.Certificate{}, nil
				}

				return cert, nil
			},
			MinVersion:   tls.VersionTLS12,
			CipherSuites: suites,
		},
		Provider: caProvider,
	}
}

func (hc *httpComponent) createHTTPClient(maxIdleConns, maxIdleConnsPerHost, maxConnsPerHost int, idleTimeout time.Duration, connectTimeout time.Duration) *http.Client {
	httpDialer := &net.Dialer{
		Timeout:   connectTimeout,
		KeepAlive: 30 * time.Second,
	}

	// We set ForceAttemptHTTP2, which will update the base-config to support HTTP2
	// automatically, so that all configs from it will look for that.
	httpTransport := &http.Transport{
		ForceAttemptHTTP2: true,

		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return httpDialer.DialContext(ctx, network, addr)
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			tcpConn, err := httpDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			// We set up the transport to point at the BaseConfig from the dynamic TLS system.
			clientMux := hc.muxer.Get()
			if clientMux == nil {
				return nil, errShutdown
			}
			httpTLSConfig := clientMux.tlsConfig
			if httpTLSConfig == nil {
				return nil, errors.New("TLS is not configured on this Agent")
			}

			srvTLSConfig, err := httpTLSConfig.MakeForAddr(addr)
			if err != nil {
				return nil, err
			}

			tlsConn := tls.Client(tcpConn, srvTLSConfig)
			return tlsConn, nil
		},
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		MaxConnsPerHost:     maxConnsPerHost,
		IdleConnTimeout:     idleTimeout,
	}

	httpCli := &http.Client{
		Transport: httpTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// All that we're doing here is setting auth on any redirects.
			// For that reason we can just pull it off the oldest (first) request.
			if len(via) >= 10 {
				// Just duplicate the default behaviour for maximum redirects.
				return errors.New("stopped after 10 redirects")
			}

			oldest := via[0]
			auth := oldest.Header.Get("Authorization")
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}

			return nil
		},
	}
	return httpCli
}

/* #nosec G404 */
func randFromServiceEndpoints(endpoints []routeEndpoint, denylist []string) (routeEndpoint, error) {
	var allowList []routeEndpoint
	for _, ep := range endpoints {
		if inDenyList(ep, denylist) {
			continue
		}
		allowList = append(allowList, ep)
	}
	if len(allowList) == 0 {
		return routeEndpoint{}, errServiceNotAvailable
	}

	return allowList[rand.Intn(len(allowList))], nil
}

func inDenyList(ep routeEndpoint, denylist []string) bool {
	for _, b := range denylist {
		if ep.Address == b {
			return true
		}
	}

	return false
}

func injectJSONCreds(body []byte, creds []UserPassPair) []byte {
	var props map[string]json.RawMessage
	err := json.Unmarshal(body, &props)
	if err == nil {
		if _, ok := props["creds"]; ok {
			// Early out if the user has already passed a set of credentials.
			return body
		}

		jsonCreds, err := json.Marshal(creds)
		if err == nil {
			props["creds"] = json.RawMessage(jsonCreds)

			newBody, err := json.Marshal(props)
			if err == nil {
				return newBody
			}
		}
	}

	return body
}

type httpRequestGenerator struct {
	ctx     context.Context
	request *httpRequest
	header  http.Header
}

func newHTTPRequestGenerator(ctx context.Context, req *httpRequest, userAgent string) *httpRequestGenerator {
	header := make(http.Header)
	if req.ContentType != "" {
		header.Set("Content-Type", req.ContentType)
	} else {
		header.Set("Content-Type", "application/json")
	}
	if len(req.User) > 0 {
		header.Set("cb-on-behalf-of", req.User)
	}
	for key, val := range req.Headers {
		header.Set(key, val)
	}

	var uniqueID string
	if req.UniqueID != "" {
		uniqueID = req.UniqueID
	} else {
		uniqueID = uuid.New().String()
	}
	header.Set("User-Agent", clientInfoString(uniqueID, userAgent))

	return &httpRequestGenerator{
		ctx:     ctx,
		request: req,
		header:  header,
	}
}

func (hrg *httpRequestGenerator) NewRequest(endpoint string, creds []UserPassPair) (*http.Request, error) {
	// Generate a request URI
	reqURI := endpoint + hrg.request.Path

	hreq, err := http.NewRequestWithContext(hrg.ctx, hrg.request.Method, reqURI, nil)
	if err != nil {
		return nil, err
	}
	hreq.Header = hrg.header

	body := hrg.request.Body

	// Inject credentials into the request
	if hrg.request.Username != "" || hrg.request.Password != "" {
		hreq.SetBasicAuth(hrg.request.Username, hrg.request.Password)
	} else {
		if hrg.request.Service == N1qlService || hrg.request.Service == CbasService ||
			hrg.request.Service == FtsService {
			// Handle service which support multi-bucket authentication using
			// injection into the body of the request.
			if len(creds) == 1 {
				hreq.SetBasicAuth(creds[0].Username, creds[0].Password)
			} else {
				body = injectJSONCreds(body, creds)
			}
		} else {
			if len(creds) != 1 {
				return nil, errInvalidCredentials
			}

			hreq.SetBasicAuth(creds[0].Username, creds[0].Password)
		}
	}

	hreq.Body = io.NopCloser(bytes.NewReader(body))

	return hreq, nil
}
