package gocbcore

import (
	"bytes"
	"errors"
	"io"
)

func (suite *UnitTestSuite) TestViewQueryErrorHTTPCode() {
	type test struct {
		name        string
		statusCode  int
		body        string
		expectedErr error
	}

	tests := []test{
		{
			name:        "400 with unmapped message",
			statusCode:  400,
			body:        `{"error":"query_parse_error","reason":"Invalid value for integer parameter: \"abc\""}`,
			expectedErr: ErrInvalidArgument,
		},
		{
			name:        "400 with unparseable body",
			statusCode:  400,
			body:        `not json`,
			expectedErr: ErrInvalidArgument,
		},
		{
			name:        "400 view not found",
			statusCode:  400,
			body:        `{"node1":"not_found"}`,
			expectedErr: ErrViewNotFound,
		},
		{
			name:        "500",
			statusCode:  500,
			body:        `{"error":"something went wrong"}`,
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			resp := &HTTPResponse{
				StatusCode: tt.statusCode,
				Body:       io.NopCloser(bytes.NewReader([]byte(tt.body))),
			}

			err := parseViewQueryError(nil, "ddoc", "view", resp)
			suite.Require().NotNil(err)
			suite.Assert().Equal(tt.statusCode, err.HTTPResponseCode)

			if tt.expectedErr == nil {
				suite.Assert().False(errors.Is(err, ErrInvalidArgument))
			} else {
				suite.Assert().ErrorIs(err, tt.expectedErr)
			}
		})
	}
}
