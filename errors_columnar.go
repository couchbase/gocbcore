package gocbcore

import "encoding/json"

// ColumnarErrorDesc represents specific Columnar error data.
type ColumnarErrorDesc struct {
	Code    uint32
	Message string
	Retry   bool
}

// MarshalJSON implements the Marshaler interface.
func (e ColumnarErrorDesc) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Code    uint32 `json:"code"`
		Message string `json:"msg"`
	}{
		Code:    e.Code,
		Message: e.Message,
	})
}

// ColumnarError represents an error returned from an Columnar query.
type ColumnarError struct {
	InnerError       error
	Statement        string
	Errors           []ColumnarErrorDesc
	LastErrorCode    uint32
	LastErrorMsg     string
	Endpoint         string
	ErrorText        string
	HTTPResponseCode int
}

func newColumnarError(innerError error, statement string, endpoint string, responseCode int) *ColumnarError {
	return &ColumnarError{
		InnerError:       innerError,
		Statement:        statement,
		Endpoint:         endpoint,
		HTTPResponseCode: responseCode,
	}
}

func (e ColumnarError) withErrorText(errText string) *ColumnarError {
	e.ErrorText = errText
	return &e
}

func (e ColumnarError) withLastDetail(code uint32, msg string) *ColumnarError {
	e.LastErrorCode = code
	e.LastErrorMsg = msg
	return &e
}

func (e ColumnarError) withErrors(errors []ColumnarErrorDesc) *ColumnarError {
	e.Errors = errors
	return &e
}

// MarshalJSON implements the Marshaler interface.
func (e ColumnarError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		InnerError       string              `json:"msg,omitempty"`
		Statement        string              `json:"statement,omitempty"`
		Errors           []ColumnarErrorDesc `json:"errors,omitempty"`
		LastCode         uint32              `json:"lastCode,omitempty"`
		LastMessage      string              `json:"lastMsg,omitempty"`
		Endpoint         string              `json:"endpoint,omitempty"`
		HTTPResponseCode int                 `json:"status_code,omitempty"`
	}{
		InnerError:       e.InnerError.Error(),
		Statement:        e.Statement,
		Errors:           e.Errors,
		LastCode:         e.LastErrorCode,
		LastMessage:      e.LastErrorMsg,
		Endpoint:         e.Endpoint,
		HTTPResponseCode: e.HTTPResponseCode,
	})
}

// Error returns the string representation of this error.
func (e ColumnarError) Error() string {
	errBytes, serErr := json.Marshal(struct {
		InnerError       error               `json:"-"`
		Statement        string              `json:"statement,omitempty"`
		Errors           []ColumnarErrorDesc `json:"errors,omitempty"`
		LastCode         uint32              `json:"lastCode,omitempty"`
		LastMessage      string              `json:"lastMsg,omitempty"`
		Endpoint         string              `json:"endpoint,omitempty"`
		ErrorText        string              `json:"error_text,omitempty"`
		HTTPResponseCode int                 `json:"status_code,omitempty"`
	}{
		InnerError:       e.InnerError,
		Statement:        e.Statement,
		Errors:           e.Errors,
		LastCode:         e.LastErrorCode,
		LastMessage:      e.LastErrorMsg,
		Endpoint:         e.Endpoint,
		ErrorText:        e.ErrorText,
		HTTPResponseCode: e.HTTPResponseCode,
	})
	if serErr != nil {
		logErrorf("failed to serialize error to json: %s", serErr.Error())
	}

	return e.InnerError.Error() + " | " + string(errBytes)
}

// Unwrap returns the underlying reason for the error
func (e ColumnarError) Unwrap() error {
	return e.InnerError
}
