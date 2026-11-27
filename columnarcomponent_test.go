package gocbcore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"time"

	"github.com/stretchr/testify/mock"
)

func (suite *UnitTestSuite) TestColumnarQuerySendsUserAgent() {
	userAgentCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		userAgentCh <- r.Header.Get("User-Agent")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte(`{"requestID":"1","results":[],"status":"success"}`))
	}))
	defer srv.Close()

	cfgMgr := new(mockConfigManager)
	cfgMgr.On("AddConfigWatcher", mock.Anything).Return()

	muxer := newColumnarMux(cfgMgr, &columnarClientMux{
		epList: []routeEndpoint{{Address: srv.URL}},
		revID:  1,
		auth:   PasswordAuthProvider{Username: "user", Password: "pass"},
	})

	cc := newColumnarComponent(
		columnarComponentProps{UserAgent: "gocb-columnar/1.0.0"},
		columnarHTTPClientProps{ConnectTimeout: time.Second},
		muxer,
	)
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reader, err := cc.Query(ctx, ColumnarQueryOptions{
		Payload: map[string]interface{}{
			"statement":         "SELECT 1",
			"client_context_id": "ctx-id",
		},
	})
	suite.Require().NoError(err)
	suite.Require().NoError(reader.Close())

	expected := fmt.Sprintf("gocb-columnar/1.0.0 (gocbcore/%s; %s/%s; %s)",
		strings.TrimPrefix(goCbCoreVersionStr, "v"), runtime.GOOS, runtime.GOARCH, runtime.Version())
	suite.Assert().Equal(expected, <-userAgentCh)
}
