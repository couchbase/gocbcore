package gocbcore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type httpRequest struct {
	Service       ServiceType
	Endpoint      string
	Method        string
	Path          string
	Username      string
	Password      string
	Headers       map[string]string
	ContentType   string
	Body          []byte
	IsIdempotent  bool
	UniqueID      string
	Deadline      time.Time
	RetryStrategy RetryStrategy

	retryCount   uint32
	retryReasons []RetryReason
}

func (hr *httpRequest) retryStrategy() RetryStrategy {
	return hr.RetryStrategy
}

func (hr *httpRequest) RetryAttempts() uint32 {
	return atomic.LoadUint32(&hr.retryCount)
}

func (hr *httpRequest) Identifier() string {
	return hr.UniqueID
}

func (hr *httpRequest) Idempotent() bool {
	return hr.IsIdempotent
}

func (hr *httpRequest) RetryReasons() []RetryReason {
	return hr.retryReasons
}

func (hr *httpRequest) incrementRetryAttempts() {
	atomic.AddUint32(&hr.retryCount, 1)
}

func (hr *httpRequest) addRetryReason(reason RetryReason) {
	idx := sort.Search(len(hr.retryReasons), func(i int) bool {
		return hr.retryReasons[i] == reason
	})

	// if idx is out of the range of retryReasons then it wasn't found.
	if idx > len(hr.retryReasons)-1 {
		hr.retryReasons = append(hr.retryReasons, reason)
	}
}

// HTTPRequest contains the description of an HTTP request to perform.
type HTTPRequest struct {
	Service       ServiceType
	Method        string
	Endpoint      string
	Path          string
	Username      string
	Password      string
	Body          []byte
	Headers       map[string]string
	ContentType   string
	IsIdempotent  bool
	UniqueID      string
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// HTTPResponse encapsulates the response from an HTTP request.
type HTTPResponse struct {
	Endpoint   string
	StatusCode int
	Body       io.ReadCloser
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

func (agent *Agent) getMgmtEp() (string, error) {
	mgmtEps := agent.MgmtEps()
	if len(mgmtEps) == 0 {
		return "", errServiceNotAvailable
	}
	return mgmtEps[rand.Intn(len(mgmtEps))], nil
}

func (agent *Agent) getCapiEp() (string, error) {
	capiEps := agent.CapiEps()
	if len(capiEps) == 0 {
		return "", errServiceNotAvailable
	}
	return capiEps[rand.Intn(len(capiEps))], nil
}

func (agent *Agent) getN1qlEp() (string, error) {
	n1qlEps := agent.N1qlEps()
	if len(n1qlEps) == 0 {
		return "", errServiceNotAvailable
	}
	return n1qlEps[rand.Intn(len(n1qlEps))], nil
}

func (agent *Agent) getFtsEp() (string, error) {
	ftsEps := agent.FtsEps()
	if len(ftsEps) == 0 {
		return "", errServiceNotAvailable
	}
	return ftsEps[rand.Intn(len(ftsEps))], nil
}

func (agent *Agent) getCbasEp() (string, error) {
	cbasEps := agent.CbasEps()
	if len(cbasEps) == 0 {
		return "", errServiceNotAvailable
	}
	return cbasEps[rand.Intn(len(cbasEps))], nil
}

func wrapHTTPError(req *httpRequest, err error) HTTPError {
	if err == nil {
		err = errors.New("http error")
	}

	ierr := HTTPError{
		InnerError: err,
	}

	if req != nil {
		ierr.Endpoint = req.Endpoint
		ierr.UniqueID = req.UniqueID
		ierr.RetryAttempts = req.RetryAttempts()
		ierr.RetryReasons = req.RetryReasons()
	}

	return ierr
}

// DoHTTPRequest will perform an HTTP request against one of the HTTP
// services which are available within the SDK.
func (agent *Agent) DoHTTPRequest(req *HTTPRequest) (*HTTPResponse, error) {
	retryStrategy := agent.defaultRetryStrategy
	if req.RetryStrategy != nil {
		retryStrategy = req.RetryStrategy
	}

	deadline := time.Now().Add(req.Timeout)

	ireq := &httpRequest{
		Service:       req.Service,
		Endpoint:      req.Endpoint,
		Method:        req.Method,
		Path:          req.Path,
		Headers:       req.Headers,
		ContentType:   req.ContentType,
		Username:      req.Username,
		Password:      req.Password,
		Body:          req.Body,
		IsIdempotent:  req.IsIdempotent,
		UniqueID:      req.UniqueID,
		Deadline:      deadline,
		RetryStrategy: retryStrategy,
	}

	resp, err := agent.execHTTPRequest(ireq)
	if err != nil {
		return nil, wrapHTTPError(ireq, err)
	}

	return resp, nil
}

// DoHTTPRequest will perform an HTTP request against one of the HTTP
// services which are available within the SDK.
func (agent *Agent) execHTTPRequest(req *httpRequest) (*HTTPResponse, error) {
	if req.Service == MemdService {
		return nil, errInvalidService
	}

	// Identify an endpoint to use for the request
	endpoint := req.Endpoint
	if endpoint == "" {
		var err error
		switch req.Service {
		case MgmtService:
			endpoint, err = agent.getMgmtEp()
		case CapiService:
			endpoint, err = agent.getCapiEp()
		case N1qlService:
			endpoint, err = agent.getN1qlEp()
		case FtsService:
			endpoint, err = agent.getFtsEp()
		case CbasService:
			endpoint, err = agent.getCbasEp()
		}
		if err != nil {
			return nil, err
		}

		req.Endpoint = endpoint
	}

	// Generate a request URI
	reqURI := endpoint + req.Path

	// Create a new request
	hreq, err := http.NewRequest(req.Method, reqURI, nil)
	if err != nil {
		return nil, err
	}

	// Create a context to deadline with
	ctx := context.Background()
	ctx, ctxCancel := context.WithDeadline(ctx, req.Deadline)
	defer ctxCancel()

	// Lets add our context to the httpRequest
	hreq = hreq.WithContext(ctx)

	body := req.Body

	// Inject credentials into the request
	if req.Username != "" || req.Password != "" {
		hreq.SetBasicAuth(req.Username, req.Password)
	} else {
		creds, err := agent.auth.Credentials(AuthCredsRequest{
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
	for key, val := range req.Headers {
		hreq.Header.Set(key, val)
	}

	var uniqueID string
	if req.UniqueID != "" {
		uniqueID = req.UniqueID
	} else {
		uniqueID = uuid.New().String()
	}
	hreq.Header.Set("User-Agent", agent.clientInfoString(uniqueID))

	for {
		hresp, err := agent.httpCli.Do(hreq)
		if err != nil {
			// Because we have to hijack the context for our own timeout specification
			// purposes, we need to perform some translation here if we hijacked it.
			if errors.Is(err, context.DeadlineExceeded) {
				err = errAmbiguousTimeout
			}

			if !req.IsIdempotent {
				return nil, err
			}

			isUserError := false
			isUserError = isUserError || errors.Is(err, context.DeadlineExceeded)
			isUserError = isUserError || errors.Is(err, context.Canceled)
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
			case <-time.After(retryTime.Sub(time.Now())):
				// continue!
			case <-ctx.Done():
				// context timed out
				if errors.Is(err, context.DeadlineExceeded) {
					err = errUnambiguousTimeout
				}

				return nil, err
			}

			continue
		}

		respOut := HTTPResponse{
			Endpoint:   endpoint,
			StatusCode: hresp.StatusCode,
			Body:       hresp.Body,
		}

		return &respOut, nil
	}
}
