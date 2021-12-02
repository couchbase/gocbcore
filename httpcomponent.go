package gocbcore

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
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
	defaultRetryStrategy RetryStrategy
}

type httpComponentProps struct {
	UserAgent            string
	DefaultRetryStrategy RetryStrategy
}

type httpClientProps struct {
	maxIdleConns        int
	maxIdleConnsPerHost int
	idleTimeout         time.Duration
}

func newHTTPComponent(props httpComponentProps, clientProps httpClientProps, muxer *httpMux, tracer *tracerComponent) *httpComponent {
	hc := &httpComponent{
		muxer:                muxer,
		userAgent:            props.UserAgent,
		defaultRetryStrategy: props.DefaultRetryStrategy,
		tracer:               tracer,
	}

	hc.cli = hc.createHTTPClient(clientProps.maxIdleConns, clientProps.maxIdleConnsPerHost, clientProps.idleTimeout)

	return hc
}

func (hc *httpComponent) Close() {
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

func (hc *httpComponent) DoInternalHTTPRequest(req *httpRequest, skipConfigCheck bool) (*HTTPResponse, error) {
	if req.Service == MemdService {
		return nil, errInvalidService
	}

	// This creates a context that has a parent with no cancel function. As such WithCancel will not setup any
	// extra go routines and we only need to call cancel on (non-timeout) failure.
	ctx := req.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, ctxCancel := context.WithCancel(ctx)

	// This is easy to do with a bool and a defer than to ensure that we cancel after every error.
	doneCh := make(chan struct{}, 1)
	querySuccess := false
	defer func() {
		doneCh <- struct{}{}
		if !querySuccess {
			ctxCancel()
		}
	}()

	start := time.Now()
	var cancelationIsTimeout uint32
	// Having no deadline is a legitimate case.
	if !req.Deadline.IsZero() {
		go func() {
			select {
			case <-time.After(req.Deadline.Sub(start)):
				atomic.StoreUint32(&cancelationIsTimeout, 1)
				ctxCancel()
			case <-doneCh:
			}
		}()
	}

	if !skipConfigCheck {
		for {
			revID, err := hc.muxer.ConfigRev()
			if err != nil {
				return nil, err
			}

			if revID > -1 {
				break
			}

			// We've not successfully been setup with a cluster map yet
			select {
			case <-ctx.Done():
				err := ctx.Err()
				if errors.Is(err, context.Canceled) {
					isTimeout := atomic.LoadUint32(&cancelationIsTimeout)
					if isTimeout == 1 {
						if req.IsIdempotent {
							return nil, errUnambiguousTimeout
						}
						return nil, errAmbiguousTimeout
					}

					return nil, errRequestCanceled
				}

				return nil, err
			case <-time.After(500 * time.Microsecond):
			}
		}
	}

	// Identify an endpoint to use for the request
	endpoint := req.Endpoint
	if endpoint == "" {
		var err error
		switch req.Service {
		case MgmtService:
			endpoint, err = hc.getMgmtEp()
		case CapiService:
			endpoint, err = hc.getCapiEp()
		case N1qlService:
			endpoint, err = hc.getN1qlEp()
		case FtsService:
			endpoint, err = hc.getFtsEp()
		case CbasService:
			endpoint, err = hc.getCbasEp()
		case EventingService:
			endpoint, err = hc.getEventingEp()
		case GSIService:
			endpoint, err = hc.getGSIEp()
		case BackupService:
			endpoint, err = hc.getBackupEp()
		}
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		switch req.Service {
		case MgmtService:
			err = hc.validateEndpoint(endpoint, hc.muxer.MgmtEps())
		case CapiService:
			err = hc.validateEndpoint(endpoint, hc.muxer.CapiEps())
		case N1qlService:
			err = hc.validateEndpoint(endpoint, hc.muxer.N1qlEps())
		case FtsService:
			err = hc.validateEndpoint(endpoint, hc.muxer.FtsEps())
		case CbasService:
			err = hc.validateEndpoint(endpoint, hc.muxer.CbasEps())
		case EventingService:
			err = hc.validateEndpoint(endpoint, hc.muxer.EventingEps())
		case GSIService:
			err = hc.validateEndpoint(endpoint, hc.muxer.GSIEps())
		case BackupService:
			err = hc.validateEndpoint(endpoint, hc.muxer.BackupEps())
		}
		if err != nil {
			return nil, err
		}
	}

	// Generate a request URI
	reqURI := endpoint + req.Path

	// Create a new request
	hreq, err := http.NewRequest(req.Method, reqURI, nil)
	if err != nil {
		return nil, err
	}

	// Lets add our context to the httpRequest
	hreq = hreq.WithContext(ctx)

	body := req.Body

	// Inject credentials into the request
	if req.Username != "" || req.Password != "" {
		hreq.SetBasicAuth(req.Username, req.Password)
	} else {
		auth := hc.muxer.Auth()
		if auth == nil {
			// Shouldn't happen but if it does then probably better to not panic with a nil pointer.
			return nil, errCliInternalError
		}
		creds, err := auth.Credentials(AuthCredsRequest{
			Service:  req.Service,
			Endpoint: endpoint,
		})
		if err != nil {
			return nil, err
		}

		if req.Service == N1qlService || req.Service == CbasService ||
			req.Service == FtsService {
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

	hreq.Body = ioutil.NopCloser(bytes.NewReader(body))

	if req.ContentType != "" {
		hreq.Header.Set("Content-Type", req.ContentType)
	} else {
		hreq.Header.Set("Content-Type", "application/json")
	}
	if len(req.User) > 0 {
		hreq.Header.Set("cb-on-behalf-of", req.User)
	}
	for key, val := range req.Headers {
		hreq.Header.Set(key, val)
	}

	var uniqueID string
	if req.UniqueID != "" {
		uniqueID = req.UniqueID
	} else {
		uniqueID = uuid.New().String()
	}
	hreq.Header.Set("User-Agent", clientInfoString(uniqueID, hc.userAgent))

	for {
		dSpan := hc.tracer.StartHTTPDispatchSpan(req, spanNameDispatchToServer)
		logSchedf("Writing HTTP request to %s ID=%s", reqURI, req.UniqueID)
		// we can't close the body of this response as it's long lived beyond the function
		hresp, err := hc.cli.Do(hreq) // nolint: bodyclose
		hc.tracer.StopHTTPDispatchSpan(dSpan, hreq, req.UniqueID, req.RetryAttempts())
		if err != nil {
			logSchedf("Received HTTP Response for ID=%s, errored", req.UniqueID)
			// Because we don't use the http request context itself to perform timeouts we need to do some translation
			// of the error message here for better UX.
			if errors.Is(err, context.Canceled) {
				isTimeout := atomic.LoadUint32(&cancelationIsTimeout)
				if isTimeout == 1 {
					if req.IsIdempotent {
						err = &TimeoutError{
							InnerError:       errUnambiguousTimeout,
							OperationID:      "http",
							Opaque:           req.Identifier(),
							TimeObserved:     time.Since(start),
							RetryReasons:     req.retryReasons,
							RetryAttempts:    req.retryCount,
							LastDispatchedTo: endpoint,
						}
					} else {
						err = &TimeoutError{
							InnerError:       errAmbiguousTimeout,
							OperationID:      "http",
							Opaque:           req.Identifier(),
							TimeObserved:     time.Since(start),
							RetryReasons:     req.retryReasons,
							RetryAttempts:    req.retryCount,
							LastDispatchedTo: endpoint,
						}
					}
				} else {
					err = errRequestCanceled
				}
			}

			if !req.IsIdempotent {
				return nil, err
			}

			isUserError := false
			isUserError = isUserError || errors.Is(err, context.DeadlineExceeded)
			isUserError = isUserError || errors.Is(err, context.Canceled)
			isUserError = isUserError || errors.Is(err, ErrRequestCanceled)
			isUserError = isUserError || errors.Is(err, ErrTimeout)
			if isUserError {
				return nil, err
			}

			var retryReason RetryReason
			if errors.Is(err, io.ErrUnexpectedEOF) {
				retryReason = SocketCloseInFlightRetryReason
			}

			if retryReason == nil {
				return nil, err
			}

			shouldRetry, retryTime := retryOrchMaybeRetry(req, retryReason)
			if !shouldRetry {
				return nil, err
			}

			select {
			case <-time.After(time.Until(retryTime)):
				// continue!
			case <-time.After(time.Until(req.Deadline)):
				if errors.Is(err, context.DeadlineExceeded) {
					err = &TimeoutError{
						InnerError:       errAmbiguousTimeout,
						OperationID:      "http",
						Opaque:           req.Identifier(),
						TimeObserved:     time.Since(start),
						RetryReasons:     req.retryReasons,
						RetryAttempts:    req.retryCount,
						LastDispatchedTo: endpoint,
					}
				}

				return nil, err
			}

			continue
		}
		logSchedf("Received HTTP Response for ID=%s, status=%d", req.UniqueID, hresp.StatusCode)

		respOut := HTTPResponse{
			Endpoint:      endpoint,
			StatusCode:    hresp.StatusCode,
			ContentLength: hresp.ContentLength,
			Body:          hresp.Body,
		}

		querySuccess = true

		return &respOut, nil
	}
}

func (hc *httpComponent) getMgmtEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.MgmtEps())
}

func (hc *httpComponent) getCapiEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.CapiEps())
}

func (hc *httpComponent) getN1qlEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.N1qlEps())
}

func (hc *httpComponent) getFtsEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.FtsEps())
}

func (hc *httpComponent) getCbasEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.CbasEps())
}

func (hc *httpComponent) getEventingEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.EventingEps())
}

func (hc *httpComponent) getGSIEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.GSIEps())
}

func (hc *httpComponent) getBackupEp() (string, error) {
	return randFromServiceEndpoints(hc.muxer.BackupEps())
}

func (hc *httpComponent) validateEndpoint(endpoint string, endpoints []string) error {
	for _, ep := range endpoints {
		if ep == endpoint {
			return nil
		}
	}

	return errInvalidServer
}

func createTLSConfig(auth AuthProvider, caProvider func() *x509.CertPool) *dynTLSConfig {
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
			MinVersion: tls.VersionTLS12,
		},
		Provider: caProvider,
	}
}

func (hc *httpComponent) createHTTPClient(maxIdleConns, maxIdleConnsPerHost int, idleTimeout time.Duration) *http.Client {
	httpDialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	// We set ForceAttemptHTTP2, which will update the base-config to support HTTP2
	// automatically, so that all configs from it will look for that.
	httpTransport := &http.Transport{
		ForceAttemptHTTP2: true,

		Dial: func(network, addr string) (net.Conn, error) {
			return httpDialer.Dial(network, addr)
		},
		DialTLS: func(network, addr string) (net.Conn, error) {
			tcpConn, err := httpDialer.Dial(network, addr)
			if err != nil {
				return nil, err
			}

			// We set up the transport to point at the BaseConfig from the dynamic TLS system.
			httpTLSConfig := hc.muxer.Get().tlsConfig
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
func randFromServiceEndpoints(endpoints []string) (string, error) {
	if len(endpoints) == 0 {
		return "", errServiceNotAvailable
	}

	return endpoints[rand.Intn(len(endpoints))], nil
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
