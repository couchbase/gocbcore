package gocbcore

import "net/http"

func newHTTPComponentWithClient(props httpComponentProps, client *http.Client, muxer *httpMux,
	tracer *tracerComponent) *httpComponent {
	hc := &httpComponent{
		muxer:                muxer,
		userAgent:            props.UserAgent,
		defaultRetryStrategy: props.DefaultRetryStrategy,
		tracer:               tracer,
		cli:                  client,
	}

	return hc
}
