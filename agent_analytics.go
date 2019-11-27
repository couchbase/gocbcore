package gocbcore

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
)

// AnalyticsRowReader providers access to the rows of a analytics query
type AnalyticsRowReader struct {
	streamer *queryStreamer
}

// NextRow reads the next rows bytes from the stream
func (q *AnalyticsRowReader) NextRow() []byte {
	return q.streamer.NextRow()
}

// Err returns any errors that occured during streaming.
func (q AnalyticsRowReader) Err() error {
	return q.streamer.Err()
}

// MetaData fetches the non-row bytes streamed in the response.
func (q *AnalyticsRowReader) MetaData() ([]byte, error) {
	return q.streamer.MetaData()
}

// Close immediately shuts down the connection
func (q *AnalyticsRowReader) Close() error {
	return q.streamer.Close()
}

// AnalyticsQueryOptions represents the various options available for an analytics query.
type AnalyticsQueryOptions struct {
	Payload       []byte
	Priority      int
	RetryStrategy RetryStrategy
	Deadline      time.Time
}

func wrapAnalyticsError(req *httpRequest, statement string, err error) AnalyticsError {
	ierr := AnalyticsError{
		InnerError: err,
	}

	if req != nil {
		ierr.Endpoint = req.Endpoint
		ierr.ClientContextID = req.UniqueID
		ierr.RetryAttempts = req.RetryAttempts()
		ierr.RetryReasons = req.RetryReasons()
	}

	ierr.Statement = statement

	return ierr
}

type jsonAnalyticsError struct {
	Code uint32 `json:"code"`
	Msg  string `json:"msg"`
}

type jsonAnalyticsErrorResponse struct {
	Errors []jsonAnalyticsError
}

func parseAnalyticsError(req *httpRequest, statement string, resp *HTTPResponse) AnalyticsError {
	var err error
	var errorDescs []AnalyticsErrorDesc

	respBody, readErr := ioutil.ReadAll(resp.Body)
	if readErr == nil {
		var respParse jsonAnalyticsErrorResponse
		parseErr := json.Unmarshal(respBody, &respParse)
		if parseErr == nil {

			for _, jsonErr := range respParse.Errors {
				errorDescs = append(errorDescs, AnalyticsErrorDesc{
					Code:    jsonErr.Code,
					Message: jsonErr.Msg,
				})
			}
		}
	}

	if len(errorDescs) >= 1 {
		firstErr := errorDescs[0]
		errCode := firstErr.Code
		errCodeGroup := errCode / 1000

		if errCodeGroup == 25 {
			err = errInternalServerFailure
		}
		if errCodeGroup == 20 {
			err = errAuthenticationFailure
		}
		if errCode == 23000 || errCode == 23003 {
			err = errTemporaryFailure
		}
		if errCode == 24000 {
			err = errParsingFailure
		}
		if errCode == 24047 {
			err = errIndexNotFound
		}
		if errCode == 24048 {
			err = errIndexExists
		}

		if errCodeGroup == 24 {
			err = errCompilationFailure
		}
		if errCode == 23007 {
			err = errJobQueueFull
		}
		if errCode == 24025 || errCode == 24044 || errCode == 24045 {
			err = errDatasetNotFound
		}
		if errCode == 24034 {
			err = errDataverseNotFound
		}
		if errCode == 24040 {
			err = errDatasetExists
		}
		if errCode == 24039 {
			err = errDataverseExists
		}
		if errCode == 24006 {
			err = errLinkNotFound
		}
	}

	errOut := wrapAnalyticsError(req, statement, err)
	errOut.Errors = errorDescs
	return errOut
}

// AnalyticsQuery executes an analytics query
func (agent *Agent) AnalyticsQuery(opts AnalyticsQueryOptions) (*AnalyticsRowReader, error) {
	var payloadMap map[string]interface{}
	err := json.Unmarshal(opts.Payload, &payloadMap)
	if err != nil {
		return nil, wrapAnalyticsError(nil, "", wrapError(err, "expected a JSON payload"))
	}

	statement, _ := payloadMap["statement"].(string)
	clientContextID, _ := payloadMap["client_context_id"].(string)
	readOnly, _ := payloadMap["readonly"].(bool)

	ireq := &httpRequest{
		Service: CbasService,
		Method:  "POST",
		Path:    "/query/service",
		Headers: map[string]string{
			"Analytics-Priority": fmt.Sprintf("%d", opts.Priority),
		},
		Body:          opts.Payload,
		IsIdempotent:  readOnly,
		UniqueID:      clientContextID,
		Deadline:      opts.Deadline,
		RetryStrategy: opts.RetryStrategy,
	}

ExecuteLoop:
	for {
		{ // Produce an updated payload with the appropriate timeout
			timeoutLeft := ireq.Deadline.Sub(time.Now())
			payloadMap["timeout"] = timeoutLeft.String()

			newPayload, err := json.Marshal(payloadMap)
			if err != nil {
				return nil, wrapAnalyticsError(nil, "", wrapError(err, "failed to produce payload"))
			}
			ireq.Body = newPayload
		}

		resp, err := agent.execHTTPRequest(ireq)
		if err != nil {
			// execHTTPRequest will handle retrying due to in-flight socket close based
			// on whether or not IsIdempotent is set on the httpRequest
			return nil, wrapAnalyticsError(ireq, statement, err)
		}

		if resp.StatusCode != 200 {
			analyticsErr := parseAnalyticsError(ireq, statement, resp)

			var retryReason RetryReason
			if len(analyticsErr.Errors) >= 1 {
				firstErrDesc := analyticsErr.Errors[0]

				if firstErrDesc.Code == 23000 {
					retryReason = AnalyticsTemporaryFailureRetryReason
				} else if firstErrDesc.Code == 23003 {
					retryReason = AnalyticsTemporaryFailureRetryReason
				} else if firstErrDesc.Code == 23007 {
					retryReason = AnalyticsTemporaryFailureRetryReason
				}
			}

			if retryReason == nil {
				// analyticsErr is already wrapped here
				return nil, analyticsErr
			}

			shouldRetry, retryTime := retryOrchMaybeRetry(ireq, retryReason)
			if !shouldRetry {
				// analyticsErr is already wrapped here
				return nil, analyticsErr
			}

			select {
			case <-time.After(retryTime.Sub(time.Now())):
				continue ExecuteLoop
			case <-time.After(ireq.Deadline.Sub(time.Now())):
				return nil, wrapAnalyticsError(ireq, statement, errUnambiguousTimeout)
			}
		}

		streamer, err := newQueryStreamer(resp.Body, "results")
		if err != nil {
			return nil, wrapAnalyticsError(ireq, statement, err)
		}

		return &AnalyticsRowReader{
			streamer: streamer,
		}, nil
	}
}
