package gocbcore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

// HTTPRequest contains the description of an HTTP request to perform.
type HTTPRequest struct {
	Service     ServiceType
	Method      string
	Endpoint    string
	Path        string
	Username    string
	Password    string
	Body        []byte
	Context     context.Context
	Headers     map[string]string
	ContentType string
	UniqueID    string

	IsIdempotent  bool
	RetryStrategy RetryStrategy
	retryCount    uint32
	retryReasons  []RetryReason

	// If the request is in the process of being retried then this is the function
	// to call to stop the retry wait for this request.
	cancelRetryTimerFunc func() bool
	cancelTimerLock      sync.Mutex
}

// HTTPResponse encapsulates the response from an HTTP request.
type HTTPResponse struct {
	Endpoint   string
	StatusCode int
	Body       io.ReadCloser
}

// RetryAttempts is the number of times that this request has been retried.
func (hr *HTTPRequest) RetryAttempts() uint32 {
	return atomic.LoadUint32(&hr.retryCount)
}

// incrementRetryAttempts increments the number of retry attempts.
func (hr *HTTPRequest) incrementRetryAttempts() {
	atomic.AddUint32(&hr.retryCount, 1)
}

// Identifier returns the unique identifier for this request.
func (hr *HTTPRequest) Identifier() string {
	return hr.UniqueID
}

// Idempotent returns whether or not this request is idempotent.
func (hr *HTTPRequest) Idempotent() bool {
	return hr.IsIdempotent
}

// RetryReasons returns the set of reasons why this request has been retried.
func (hr *HTTPRequest) RetryReasons() []RetryReason {
	return hr.retryReasons
}

// setCancelRetry sets the retry cancel function for this request.
func (hr *HTTPRequest) setCancelRetry(cancelFunc func() bool) {
	hr.cancelTimerLock.Lock()
	hr.cancelRetryTimerFunc = cancelFunc
	hr.cancelTimerLock.Unlock()
}

// CancelRetry will cancel any retry that this request is waiting for.
func (hr *HTTPRequest) CancelRetry() bool {
	if hr.cancelRetryTimerFunc == nil {
		return true
	}

	return hr.cancelRetryTimerFunc()
}

// addRetryReason adds a reason why this request has been retried.
func (hr *HTTPRequest) addRetryReason(reason RetryReason) {
	idx := sort.Search(len(hr.retryReasons), func(i int) bool {
		return hr.retryReasons[i] == reason
	})

	// if idx is out of the range of retryReasons then it wasn't found.
	if idx > len(hr.retryReasons)-1 {
		hr.retryReasons = append(hr.retryReasons, reason)
	}
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
		return "", ErrNoMgmtService
	}
	return mgmtEps[rand.Intn(len(mgmtEps))], nil
}

func (agent *Agent) getCapiEp() (string, error) {
	capiEps := agent.CapiEps()
	if len(capiEps) == 0 {
		return "", ErrNoCapiService
	}
	return capiEps[rand.Intn(len(capiEps))], nil
}

func (agent *Agent) getN1qlEp() (string, error) {
	n1qlEps := agent.N1qlEps()
	if len(n1qlEps) == 0 {
		return "", ErrNoN1qlService
	}
	return n1qlEps[rand.Intn(len(n1qlEps))], nil
}

func (agent *Agent) getFtsEp() (string, error) {
	ftsEps := agent.FtsEps()
	if len(ftsEps) == 0 {
		return "", ErrNoFtsService
	}
	return ftsEps[rand.Intn(len(ftsEps))], nil
}

func (agent *Agent) getCbasEp() (string, error) {
	cbasEps := agent.CbasEps()
	if len(cbasEps) == 0 {
		return "", ErrNoCbasService
	}
	return cbasEps[rand.Intn(len(cbasEps))], nil
}

// DoHTTPRequest will perform an HTTP request against one of the HTTP
// services which are available within the SDK.
func (agent *Agent) DoHTTPRequest(req *HTTPRequest) (*HTTPResponse, error) {
	if req.Service == MemdService {
		return nil, ErrInvalidService
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
	// hreq.WithContext will panic if ctx is nil so make absolutely sure it isn't
	if req.Context == nil {
		req.Context = context.Background()
	}
	hreq = hreq.WithContext(req.Context)

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
				return nil, ErrInvalidCredentials
			}

			hreq.SetBasicAuth(creds[0].Username, creds[0].Password)
		}
	}

	if req.RetryStrategy == nil {
		req.RetryStrategy = agent.defaultRetryStrategy
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

	var hresp *http.Response
	for {
		hresp, err = agent.httpCli.Do(hreq)
		if err != nil {
			if !req.IsIdempotent {
				return nil, err
			}

			if err == context.DeadlineExceeded || err == context.Canceled {
				return nil, err
			}

			waitCh := make(chan struct{})
			retried := agent.retryOrchestrator.MaybeRetry(req, UnknownRetryReason, req.RetryStrategy, func() {
				waitCh <- struct{}{}
			})
			if retried {
				select {
				case <-waitCh:
					continue
				case <-req.Context.Done():
					if !req.CancelRetry() {
						// Read the channel so that we don't leave it hanging
						<-waitCh
					}

					return nil, req.Context.Err()
				}
			}

			return nil, err
		}
		break
	}

	respOut := HTTPResponse{
		Endpoint:   endpoint,
		StatusCode: hresp.StatusCode,
		Body:       hresp.Body,
	}

	return &respOut, nil
}
