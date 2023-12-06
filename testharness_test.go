package gocbcore

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	srvVer450   = NodeVersion{4, 5, 0, 0, 0, ""}
	srvVer551   = NodeVersion{5, 5, 1, 0, 0, ""}
	srvVer552   = NodeVersion{5, 5, 2, 0, 0, ""}
	srvVer553   = NodeVersion{5, 5, 3, 0, 0, ""}
	srvVer600   = NodeVersion{6, 0, 0, 0, 0, ""}
	srvVer650   = NodeVersion{6, 5, 0, 0, 0, ""}
	srvVer650DP = NodeVersion{6, 5, 0, 0, 0, "dp"}
	srvVer660   = NodeVersion{6, 6, 0, 0, 0, ""}
	srvVer700   = NodeVersion{7, 0, 0, 0, 0, ""}
	srvVer710   = NodeVersion{7, 1, 0, 0, 0, ""}
	srvVer720   = NodeVersion{7, 2, 0, 0, 0, ""}
	srvVer720DP = NodeVersion{7, 2, 0, 0, 0, "dp"}
	srvVer750   = NodeVersion{7, 5, 0, 0, 0, ""}
	mockVer156  = NodeVersion{1, 5, 6, 0, 0, ""}
)

type TestFeatureCode string

var (
	TestFeatureReplicas             = TestFeatureCode("replicas")
	TestFeatureSsl                  = TestFeatureCode("ssl")
	TestFeatureViews                = TestFeatureCode("views")
	TestFeatureN1ql                 = TestFeatureCode("n1ql")
	TestFeatureCbas                 = TestFeatureCode("cbas")
	TestFeatureFts                  = TestFeatureCode("fts")
	TestFeatureErrMap               = TestFeatureCode("errmap")
	TestFeatureCollections          = TestFeatureCode("collections")
	TestFeatureDCPChangeStreams     = TestFeatureCode("dcpchangestreams")
	TestFeatureDCPExpiry            = TestFeatureCode("dcpexpiry")
	TestFeatureDCPDeleteTimes       = TestFeatureCode("dcpdeletetimes")
	TestFeatureMemd                 = TestFeatureCode("memd")
	TestFeatureGetMeta              = TestFeatureCode("getmeta")
	TestFeatureGCCCP                = TestFeatureCode("gcccp")
	TestFeatureEnhancedDurability   = TestFeatureCode("durability")
	TestFeatureCreateDeleted        = TestFeatureCode("createasdeleted")
	TestFeatureReplaceBodyWithXattr = TestFeatureCode("replacebodywithxattr")
	TestFeatureExpandMacros         = TestFeatureCode("expandmacros")
	TestFeaturePreserveExpiry       = TestFeatureCode("preserveexpiry")
	TestFeatureExpandMacrosSeqNo    = TestFeatureCode("expandmacrosseqno")
	TestFeatureTransactions         = TestFeatureCode("transactions")
	TestFeatureN1qlReasons          = TestFeatureCode("n1qlreasons")
	TestFeatureResourceUnits        = TestFeatureCode("computeunits")
	TestFeatureRangeScan            = TestFeatureCode("rangescan")
	TestFeatureSubdocReplicaReads   = TestFeatureCode("subdocreplicas")
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

// Gets a set of keys evenly distributed across all server nodes.
// the result is an array of strings, each index representing an index
// of a server
func MakeDistKeys(agent *Agent, deadline time.Time) (keys []string, errOut error) {
	// Get the routing information
	// We can't make dist keys until we're connected.
	var clientMux *kvMuxState
	for {
		clientMux = agent.kvMux.getState()

		if clientMux.RevID() > -1 {
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
		vbID := clientMux.VBMap().VbucketByKey([]byte(keyTmp))
		srvIx, err := clientMux.VBMap().NodeByVbucket(vbID, 0)
		if err != nil || srvIx < 0 || srvIx >= len(keys) || keys[srvIx] != "" {
			continue
		}
		keys[srvIx] = keyTmp
		remaining--
	}
	return
}

type TestSubHarnessBase struct {
	sigT  testing.TB
	sigCh chan int
}

func (h *TestSubHarnessBase) Continue() {
	h.sigCh <- 0
}

func (h *TestSubHarnessBase) Wrap(fn func()) {
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

func (h *TestSubHarnessBase) Fatalf(fmt string, args ...interface{}) {
	h.sigT.Helper()

	h.sigT.Logf(fmt, args...)
	h.sigCh <- 1
	panic(h)
}

func (h *TestSubHarnessBase) Skipf(fmt string, args ...interface{}) {
	h.sigT.Helper()

	h.sigT.Logf(fmt, args...)
	h.sigCh <- 2
	panic(h)
}

func (h *TestSubHarnessBase) Wait(waitSecs int, cb func(bool)) {
	if waitSecs <= 0 {
		waitSecs = 5
	}

	select {
	case v := <-h.sigCh:
		cb(true)
		if v == 1 {
			h.sigT.FailNow()
		} else if v == 2 {
			h.sigT.SkipNow()
		}
	case <-time.After(time.Duration(waitSecs) * time.Second):
		cb(false)
		<-h.sigCh
		h.sigT.FailNow()
	}
}

func (h *TestSubHarnessBase) VerifyOpSent(err error) {
	if err != nil {
		h.sigT.Fatal(err.Error())
		return
	}
}

type TestSubHarness struct {
	TestSubHarnessBase
	sigOp PendingOp
}

func makeTestSubHarness(t testing.TB) *TestSubHarness {
	// Note that the signaling channel here must have a queue of
	// at least 1 to avoid deadlocks during cancellations.
	h := &TestSubHarness{
		TestSubHarnessBase: TestSubHarnessBase{
			sigT:  t,
			sigCh: make(chan int, 1),
		},
	}

	return h
}

func (h *TestSubHarness) Wait(waitSecs int) {
	if h.sigOp == nil {
		panic("Cannot wait if there is no op set on signaler")
	}

	h.TestSubHarnessBase.Wait(waitSecs, func(success bool) {
		if success {
			h.sigOp = nil
			return
		}

		h.sigOp.Cancel()
	})
}

func (h *TestSubHarness) PushOp(op PendingOp, err error) {
	h.TestSubHarnessBase.VerifyOpSent(err)

	if h.sigOp != nil {
		panic("Can only set one op on the signaler at a time")
	}
	h.sigOp = op
}

type TestTxnsSubHarness struct {
	TestSubHarnessBase
}

func makeTestTxnsSubHarness(t *testing.T) *TestTxnsSubHarness {
	// Note that the signaling channel here must have a queue of
	// at least 1 to avoid deadlocks during cancellations.
	h := &TestTxnsSubHarness{
		TestSubHarnessBase: TestSubHarnessBase{
			sigT:  t,
			sigCh: make(chan int, 1),
		},
	}

	return h
}

func (h *TestTxnsSubHarness) Wait(waitSecs int) {
	h.TestSubHarnessBase.Wait(waitSecs, func(success bool) {})
}
