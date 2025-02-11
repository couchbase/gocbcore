package gocbcore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"time"
)

// ColumnarQueryOptions represents the various options available for a columnar query.
type ColumnarQueryOptions struct {
	Payload  map[string]interface{}
	Priority *int

	// Internal: This should never be used and is not supported.
	User string

	TraceContext RequestSpanContext
}

// ColumnarRowReader providers access to the rows of a columnar query
type ColumnarRowReader struct {
	streamer   *queryStreamer
	statement  string
	endpoint   string
	statusCode int
	peeked     []byte
}

// NextRow reads the next rows bytes from the stream
func (q *ColumnarRowReader) NextRow() []byte {
	if len(q.peeked) > 0 {
		peeked := q.peeked
		q.peeked = nil

		return peeked
	}

	return q.streamer.NextRow()
}

// Err returns any errors that occurred during streaming.
func (q *ColumnarRowReader) Err() error {
	err := q.streamer.Err()
	if err != nil {
		return err
	}

	meta, metaErr := q.streamer.MetaData()
	if metaErr != nil {
		return metaErr
	}

	cErr := parseColumnarErrorResponse(meta, q.statement, q.endpoint, q.statusCode, 0, "")
	if cErr != nil {
		return cErr
	}

	return nil
}

// MetaData fetches the non-row bytes streamed in the response.
func (q *ColumnarRowReader) MetaData() ([]byte, error) {
	return q.streamer.MetaData()
}

// Close immediately shuts down the connection
func (q *ColumnarRowReader) Close() error {
	return q.streamer.Close()
}

type columnarComponent struct {
	cli       *http.Client
	muxer     *columnarMux
	userAgent string
}

type columnarComponentProps struct {
	UserAgent string
}

type columnarHTTPClientProps struct {
	ConnectTimeout      time.Duration
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleTimeout         time.Duration
}

func newColumnarComponent(props columnarComponentProps, clientProps columnarHTTPClientProps, muxer *columnarMux) *columnarComponent {
	cc := &columnarComponent{
		muxer:     muxer,
		userAgent: props.UserAgent,
	}

	cc.cli = cc.createHTTPClient(clientProps.MaxIdleConns, clientProps.MaxIdleConnsPerHost, clientProps.IdleTimeout,
		clientProps.ConnectTimeout)

	return cc
}

func (hc *columnarComponent) Close() {
	if tsport, ok := hc.cli.Transport.(*http.Transport); ok {
		tsport.CloseIdleConnections()
	} else {
		logDebugf("Could not close idle connections for transport")
	}
}

func (cc *columnarComponent) Query(ctx context.Context, opts ColumnarQueryOptions) (*ColumnarRowReader, error) {
	statement := getMapValueString(opts.Payload, "statement", "")

	body, err := json.Marshal(opts.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query payload: %v", err)
	}

	header := make(http.Header)
	header.Set("Content-Type", "application/json")

	if len(opts.User) > 0 {
		header.Set("cb-on-behalf-of", opts.User)
	}
	if opts.Priority != nil {
		header.Set("Analytics-Priority", fmt.Sprintf("%d", *opts.Priority))
	}

	ctxDeadline, _ := ctx.Deadline()
	var serverTimeout time.Duration
	st, ok := opts.Payload["timeout"]
	if ok {
		var err error
		serverTimeout, err = time.ParseDuration(st.(string))
		if err != nil {
			return nil, fmt.Errorf("failed to parse server timeout: %v", err)
		}
	}

	var lastCode uint32
	var lastMessage string
	var retries uint32
	backoff := columnarExponentialBackoffWithJitter(100*time.Millisecond, 1*time.Minute, 2)
	var denylist []string
	for {
		endpoint, err := cc.getColumnarEp(denylist)
		if err != nil {
			return nil, err
		}

		auth := cc.muxer.Auth()
		if auth == nil {
			// Shouldn't happen but if it does then probably better to not panic with a nil pointer.
			return nil, errCliInternalError
		}

		creds, err := auth.Credentials(AuthCredsRequest{
			Service:  CbasService,
			Endpoint: endpoint,
		})
		if err != nil {
			denylist = append(denylist, endpoint)

			continue
		}

		reqURI := fmt.Sprintf("%s/query/service", endpoint)
		req, err := http.NewRequestWithContext(ctx, "POST", reqURI, ioutil.NopCloser(bytes.NewReader(body)))
		if err != nil {
			return nil, err
		}

		req.Header = header
		req.SetBasicAuth(creds[0].Username, creds[0].Password)

		// we can't close the body of this response as it's long-lived beyond the function
		resp, err := cc.cli.Do(req) // nolint: bodyclose
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}

			newBody, err := handleMaybeRetryColumnar(ctxDeadline, serverTimeout, backoff, retries, opts.Payload)
			if err != nil {
				return nil, ColumnarError{
					InnerError:       err,
					Statement:        statement,
					Errors:           nil,
					LastErrorCode:    lastCode,
					LastErrorMsg:     lastMessage,
					Endpoint:         endpoint,
					ErrorText:        "",
					HTTPResponseCode: resp.StatusCode,
				}
			}

			body = newBody
			retries++

			continue
		}

		resp = wrapHttpResponse(resp) // nolint: bodyclose

		if resp.StatusCode != 200 {
			respBody, readErr := ioutil.ReadAll(resp.Body)
			if readErr != nil {
				return nil, newColumnarError(fmt.Errorf("failed to read response body: %s", readErr), statement, endpoint, resp.StatusCode)
			}

			cErr := parseColumnarErrorResponse(respBody, statement, endpoint, resp.StatusCode, lastCode, lastMessage)
			if cErr != nil {
				first, retriable := isColumnarErrorRetriable(cErr)
				if !retriable {
					return nil, cErr
				}

				if first != nil {
					lastCode = first.Code
					lastMessage = first.Message
				}

				newBody, err := handleMaybeRetryColumnar(ctxDeadline, serverTimeout, backoff, retries, opts.Payload)
				if err != nil {
					return nil, newColumnarError(err, statement, endpoint, resp.StatusCode).
						withErrors(cErr.Errors).
						withErrorText(string(respBody)).
						withLastDetail(lastCode, lastMessage)
				}

				body = newBody
				retries++

				continue
			}

			return nil, newColumnarError(
				fmt.Errorf("query returned non-200 status code but no errors in body"),
				statement,
				endpoint,
				resp.StatusCode).
				withErrorText(string(respBody)).
				withLastDetail(lastCode, lastMessage)
		}

		streamer, err := newQueryStreamer(resp.Body, "results")
		if err != nil {
			respBody, readErr := ioutil.ReadAll(resp.Body)
			if readErr != nil {
				logDebugf("Failed to read response body: %v", readErr)
			}
			return nil, ColumnarError{
				InnerError:       fmt.Errorf("failed to parse success response body: %s", readErr),
				Statement:        statement,
				Errors:           nil,
				Endpoint:         endpoint,
				ErrorText:        string(respBody),
				HTTPResponseCode: resp.StatusCode,
			}
		}

		peeked := streamer.NextRow()
		if peeked == nil {
			err := streamer.Err()
			if err != nil {
				return nil, ColumnarError{
					InnerError:       err,
					Statement:        statement,
					Errors:           nil,
					Endpoint:         endpoint,
					ErrorText:        "",
					HTTPResponseCode: resp.StatusCode,
				}
			}

			meta, metaErr := streamer.MetaData()
			if metaErr != nil {
				return nil, ColumnarError{
					InnerError:       metaErr,
					Statement:        statement,
					Errors:           nil,
					Endpoint:         endpoint,
					ErrorText:        "",
					HTTPResponseCode: resp.StatusCode,
				}
			}

			cErr := parseColumnarErrorResponse(meta, statement, endpoint, resp.StatusCode, lastCode, lastMessage)
			if cErr != nil {
				first, retriable := isColumnarErrorRetriable(cErr)
				if !retriable {
					return nil, cErr
				}

				if first != nil {
					lastCode = first.Code
					lastMessage = first.Message
				}

				newBody, err := handleMaybeRetryColumnar(ctxDeadline, serverTimeout, backoff, retries, opts.Payload)
				if err != nil {
					return nil, newColumnarError(err, statement, endpoint, resp.StatusCode).
						withErrors(cErr.Errors).
						withErrorText(string(meta)).
						withLastDetail(lastCode, lastMessage)
				}

				body = newBody
				retries++

				continue
			}
		}

		return &ColumnarRowReader{
			streamer:   streamer,
			statement:  statement,
			endpoint:   endpoint,
			statusCode: resp.StatusCode,
			peeked:     peeked,
		}, nil
	}
}

func (cc *columnarComponent) getColumnarEp(denylist []string) (string, error) {
	return randFromServiceEndpoints(cc.muxer.ColumnarEps(), denylist)
}

func parseColumnarErrorResponse(respBody []byte, statement, endpoint string, statusCode int, lastCode uint32, lastMsg string) *ColumnarError {
	var rawRespParse jsonAnalyticsErrorResponse
	parseErr := json.Unmarshal(respBody, &rawRespParse)
	if parseErr != nil {
		return newColumnarError(fmt.Errorf("failed to parse response errors: %s", parseErr), statement, endpoint, statusCode).
			withLastDetail(lastCode, lastMsg).
			withErrorText(string(respBody))
	}

	var respParse []jsonAnalyticsError
	parseErr = json.Unmarshal(rawRespParse.Errors, &respParse)
	if parseErr != nil {
		return newColumnarError(fmt.Errorf("failed to parse response errors: %s", parseErr), statement, endpoint, statusCode).
			withLastDetail(lastCode, lastMsg).
			withErrorText(string(respBody))
	}

	if len(respParse) == 0 {
		return nil
	}

	errDescs := make([]ColumnarErrorDesc, len(respParse))
	for i, jsonErr := range respParse {
		errDescs[i] = ColumnarErrorDesc{
			Code:    jsonErr.Code,
			Message: jsonErr.Msg,
			Retry:   jsonErr.Retry,
		}
	}

	return newColumnarError(errColumnar, statement, endpoint, statusCode).
		withLastDetail(lastCode, lastMsg).
		withErrorText(string(respBody)).
		withErrors(errDescs)
}

func isColumnarErrorRetriable(cErr *ColumnarError) (*ColumnarErrorDesc, bool) {
	var first *ColumnarErrorDesc
	allRetriable := true
	for _, err := range cErr.Errors {
		if !err.Retry {
			allRetriable = false
			if first == nil {
				first = &ColumnarErrorDesc{
					Code:    err.Code,
					Message: err.Message,
				}
			}
		}
	}

	if !allRetriable {
		return nil, false
	}

	if first == nil && len(cErr.Errors) > 0 {
		first = &cErr.Errors[0]
	}

	return first, true
}

// Note in he interest of keeping this signature sane, we return a raw base error here.
func handleMaybeRetryColumnar(ctxDeadline time.Time, serverTimeout time.Duration, calc BackoffCalculator,
	retries uint32, payload map[string]interface{}) ([]byte, error) {
	b := calc(retries)
	var body []byte
	if !ctxDeadline.IsZero() {
		if time.Now().Add(b).Before(ctxDeadline.Add(-b)) {
			return nil, errDeadlineWouldBeExceeded
		}
	}
	if serverTimeout > 0 {
		if time.Now().Add(b).Before(time.Now().Add(serverTimeout)) {
			return nil, errTimeout
		}

		serverTimeout = serverTimeout - b
		payload["timeout"] = serverTimeout.String()
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			logWarnf("Failed to marshal query payload: %v", err)
		}
	}

	time.Sleep(b)

	return body, nil
}

func (cc *columnarComponent) createHTTPClient(maxIdleConns, maxIdleConnsPerHost int, idleTimeout time.Duration, connectTimeout time.Duration) *http.Client {
	httpDialer := &net.Dialer{
		Timeout:   connectTimeout,
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
			httpTLSConfig := cc.muxer.Get().tlsConfig
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

func columnarExponentialBackoffWithJitter(min, max time.Duration, backoffFactor float64) BackoffCalculator {
	var minBackoff float64 = 1000000   // 1 Millisecond
	var maxBackoff float64 = 500000000 // 500 Milliseconds
	var factor float64 = 2

	if min > 0 {
		minBackoff = float64(min)
	}
	if max > 0 {
		maxBackoff = float64(max)
	}
	if backoffFactor > 0 {
		factor = backoffFactor
	}

	return func(retryAttempts uint32) time.Duration {
		backoff := minBackoff * (math.Pow(factor, float64(retryAttempts)))

		backoff = rand.Float64() * (backoff) // #nosec G404

		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		if backoff < minBackoff {
			backoff = minBackoff
		}

		return time.Duration(backoff)
	}
}
