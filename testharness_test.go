package gocbcore

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v9/jcbmock"
)

var (
	srvVer450   = NodeVersion{4, 5, 0, 0, 0, ""}
	srvVer551   = NodeVersion{5, 5, 1, 0, 0, ""}
	srvVer552   = NodeVersion{5, 5, 2, 0, 0, ""}
	srvVer553   = NodeVersion{5, 5, 3, 0, 0, ""}
	srvVer600   = NodeVersion{6, 0, 0, 0, 0, ""}
	srvVer650   = NodeVersion{6, 5, 0, 0, 0, ""}
	srvVer650DP = NodeVersion{6, 5, 0, 0, 0, "dp"}
	srvVer700   = NodeVersion{7, 0, 0, 0, 0, ""}
	mockVer156  = NodeVersion{1, 5, 6, 0, 0, ""}
)

type TestFeatureCode string

var (
	TestFeatureReplicas       = TestFeatureCode("replicas")
	TestFeatureSsl            = TestFeatureCode("ssl")
	TestFeatureViews          = TestFeatureCode("views")
	TestFeatureN1ql           = TestFeatureCode("n1ql")
	TestFeatureCbas           = TestFeatureCode("cbas")
	TestFeatureFts            = TestFeatureCode("fts")
	TestFeatureAdjoin         = TestFeatureCode("adjoin")
	TestFeatureErrMap         = TestFeatureCode("errmap")
	TestFeatureCollections    = TestFeatureCode("collections")
	TestFeatureDCPExpiry      = TestFeatureCode("dcpexpiry")
	TestFeatureDCPDeleteTimes = TestFeatureCode("dcpdeletetimes")
	TestFeatureMemd           = TestFeatureCode("memd")
	TestFeatureGetMeta        = TestFeatureCode("getmeta")
	TestFeatureGCCCP          = TestFeatureCode("gcccp")
	TestFeaturePingServices   = TestFeatureCode("pingservices")
)

type TestFeatureFlag struct {
	Enabled bool
	Feature TestFeatureCode
}

func ParseFeatureFlags(featuresToTest string) []TestFeatureFlag {
	var featureFlags []TestFeatureFlag
	featureFlagStrs := strings.Split(featuresToTest, ",")
	for _, featureFlagStr := range featureFlagStrs {
		if len(featureFlagStr) == 0 {
			continue
		}

		if featureFlagStr[0] == '+' {
			featureFlags = append(featureFlags, TestFeatureFlag{
				Enabled: true,
				Feature: TestFeatureCode(featureFlagStr[1:]),
			})
			continue
		} else if featureFlagStr[0] == '-' {
			featureFlags = append(featureFlags, TestFeatureFlag{
				Enabled: false,
				Feature: TestFeatureCode(featureFlagStr[1:]),
			})
			continue
		}

		panic("failed to parse specified feature codes")
	}

	return featureFlags
}

func ParseCerts(path string) (*x509.CertPool, *tls.Certificate, error) {
	ca, err := ioutil.ReadFile(path + "/ca.pem")
	if err != nil {
		return nil, nil, err
	}

	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)

	clientCert, err := ioutil.ReadFile(path + "/client.pem")
	if err == os.ErrNotExist {
		return roots, nil, nil
	} else if err != nil {
		return nil, nil, err
	}

	clientKey, err := ioutil.ReadFile(path + "/client.key")
	if err != nil {
		return nil, nil, err
	}

	cert, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, nil, err
	}

	return roots, &cert, nil
}

func TimeTravel(waitDura time.Duration, mockInst *jcbmock.Mock) {
	if mockInst == nil {
		time.Sleep(waitDura)
		return
	}

	waitSecs := int(math.Ceil(float64(waitDura) / float64(time.Second)))
	mockInst.Control(jcbmock.NewCommand(jcbmock.CTimeTravel, map[string]interface{}{
		"Offset": waitSecs,
	}))
}

// Gets a set of keys evenly distributed across all server nodes.
// the result is an array of strings, each index representing an index
// of a server
func MakeDistKeys(agent *Agent, deadline time.Time) (keys []string, errOut error) {
	// Get the routing information
	// We can't make dist keys until we're connected.
	var clientMux *kvMuxState
	for {
		clientMux = agent.kvMux.getState()

		if clientMux.revID > -1 {
			break
		}

		select {
		case <-time.After(time.Until(deadline)):
			errOut = errTimeout
			return
		case <-time.After(time.Millisecond):
		}
	}
	keys = make([]string, clientMux.NumPipelines())
	remaining := len(keys)

	for i := 0; remaining > 0; i++ {
		keyTmp := fmt.Sprintf("DistKey_%d", i)
		// Map the vBucket and server
		vbID := clientMux.vbMap.VbucketByKey([]byte(keyTmp))
		srvIx, err := clientMux.vbMap.NodeByVbucket(vbID, 0)
		if err != nil || srvIx < 0 || srvIx >= len(keys) || keys[srvIx] != "" {
			continue
		}
		keys[srvIx] = keyTmp
		remaining--
	}
	return
}

type TestSubHarness struct {
	sigT  *testing.T
	sigCh chan int
	sigOp PendingOp
}

func makeTestSubHarness(t *testing.T) *TestSubHarness {
	// Note that the signaling channel here must have a queue of
	// at least 1 to avoid deadlocks during cancellations.
	return &TestSubHarness{
		sigT:  t,
		sigCh: make(chan int, 1),
	}
}

func (h *TestSubHarness) Continue() {
	h.sigCh <- 0
}

func (h *TestSubHarness) Wrap(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			// Rethrow actual panics
			if r != h {
				panic(r)
			}
		}
	}()
	fn()
	h.sigCh <- 0
}

func (h *TestSubHarness) Fatalf(fmt string, args ...interface{}) {
	h.sigT.Helper()

	h.sigT.Logf(fmt, args...)
	h.sigCh <- 1
	panic(h)
}

func (h *TestSubHarness) Skipf(fmt string, args ...interface{}) {
	h.sigT.Helper()

	h.sigT.Logf(fmt, args...)
	h.sigCh <- 2
	panic(h)
}

func (h *TestSubHarness) Wait(waitSecs int) {
	if h.sigOp == nil {
		panic("Cannot wait if there is no op set on signaler")
	}
	if waitSecs <= 0 {
		waitSecs = 5
	}

	select {
	case v := <-h.sigCh:
		h.sigOp = nil
		if v == 1 {
			h.sigT.FailNow()
		} else if v == 2 {
			h.sigT.SkipNow()
		}
	case <-time.After(time.Duration(waitSecs) * time.Second):
		h.sigOp.Cancel()
		<-h.sigCh
		h.sigT.FailNow()
	}
}

func (h *TestSubHarness) PushOp(op PendingOp, err error) {
	if err != nil {
		h.sigT.Fatal(err.Error())
		return
	}
	if h.sigOp != nil {
		panic("Can only set one op on the signaler at a time")
	}
	h.sigOp = op
}
