package gocbcore

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/couchbaselabs/gojcbmock"
)

var (
	srvVer450  = NodeVersion{4, 5, 0, 0, ""}
	srvVer551  = NodeVersion{5, 5, 1, 0, ""}
	srvVer552  = NodeVersion{5, 5, 2, 0, ""}
	srvVer553  = NodeVersion{5, 5, 3, 0, ""}
	srvVer650  = NodeVersion{6, 5, 0, 0, ""}
	srvVer700  = NodeVersion{7, 0, 0, 0, ""}
	mockVer156 = NodeVersion{1, 5, 6, 0, ""}
)

type TestFeatureCode string

var (
	TestFeatureReplicas    = TestFeatureCode("replicas")
	TestFeatureSsl         = TestFeatureCode("ssl")
	TestFeatureViews       = TestFeatureCode("views")
	TestFeatureN1ql        = TestFeatureCode("n1ql")
	TestFeatureAdjoin      = TestFeatureCode("adjoin")
	TestFeatureErrMap      = TestFeatureCode("errmap")
	TestFeatureCollections = TestFeatureCode("collections")
	TestFeatureDCP         = TestFeatureCode("dcp")
	TestFeatureDCPExpiry   = TestFeatureCode("dcpexpiry")
	TestFeatureMemd        = TestFeatureCode("memd")
	TestFeatureGetMeta     = TestFeatureCode("getmeta")
)

type TestFeatureFlag struct {
	Enabled bool
	Feature TestFeatureCode
}

type TestHarness struct {
	ConnStr        string
	BucketName     string
	MemdBucketName string
	DcpBucketName  string
	ScopeName      string
	CollectionName string
	Username       string
	Password       string
	ClusterVersion NodeVersion
	FeatureFlags   []TestFeatureFlag

	mockInst     *gojcbmock.Mock
	defaultAgent *Agent
	memdAgent    *Agent
	dcpAgent     *Agent
}

func (h *TestHarness) init() error {
	if h.ConnStr == "" {
		mpath, err := gojcbmock.GetMockPath()
		if err != nil {
			panic(err.Error())
		}

		mock, err := gojcbmock.NewMock(mpath, 4, 1, 64, []gojcbmock.BucketSpec{
			{Name: "default", Type: gojcbmock.BCouchbase},
			{Name: "memd", Type: gojcbmock.BMemcached},
		}...)
		if err != nil {
			return err
		}

		mock.Control(gojcbmock.NewCommand(gojcbmock.CSetCCCP,
			map[string]interface{}{"enabled": "true"}))
		mock.Control(gojcbmock.NewCommand(gojcbmock.CSetSASLMechanisms,
			map[string]interface{}{"mechs": []string{"SCRAM-SHA512"}}))

		h.mockInst = mock

		var couchbaseAddrs []string
		for _, mcport := range mock.MemcachedPorts() {
			couchbaseAddrs = append(couchbaseAddrs, fmt.Sprintf("127.0.0.1:%d", mcport))
		}

		h.ConnStr = fmt.Sprintf("couchbase://%s", strings.Join(couchbaseAddrs, ","))
		h.BucketName = "default"
		h.MemdBucketName = "memd"
		h.Username = "Administrator"
		h.Password = "password"
	}

	err := h.initDefaultAgent()
	if err != nil {
		return err
	}

	if h.SupportsFeature(TestFeatureMemd) {
		err = h.initMemdAgent()
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *TestHarness) finish() {
	if h.dcpAgent != nil {
		h.dcpAgent.Close()
		h.dcpAgent = nil
	}

	if h.memdAgent != nil {
		h.memdAgent.Close()
		h.memdAgent = nil
	}

	if h.defaultAgent != nil {
		h.defaultAgent.Close()
		h.defaultAgent = nil
	}
}

func (h *TestHarness) initDefaultAgent() error {
	config := AgentConfig{}
	config.FromConnStr(h.ConnStr)

	config.UseMutationTokens = true
	config.UseCollections = true
	config.BucketName = h.BucketName

	config.Auth = &PasswordAuthProvider{
		Username: h.Username,
		Password: h.Password,
	}

	agent, err := CreateAgent(&config)
	if err != nil {
		return err
	}

	ch := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(2*time.Second),
		WaitUntilReadyOptions{},
		func(result *WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return err
	}

	err = <-ch
	if err != nil {
		return err
	}

	h.defaultAgent = agent

	return nil
}

func (h *TestHarness) initMemdAgent() error {
	config := AgentConfig{}
	config.FromConnStr(h.ConnStr)

	config.Auth = &PasswordAuthProvider{
		Username: h.Username,
		Password: h.Password,
	}

	config.BucketName = h.MemdBucketName

	agent, err := CreateAgent(&config)
	if err != nil {
		return err
	}

	ch := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(2*time.Second),
		WaitUntilReadyOptions{},
		func(result *WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return err
	}

	err = <-ch
	if err != nil {
		return err
	}

	h.memdAgent = agent

	return nil
}

func (h *TestHarness) initDcpAgent() error {
	return nil
}

func (h *TestHarness) DefaultAgent() *Agent {
	return h.defaultAgent
}

func (h *TestHarness) MemdAgent() *Agent {
	return h.memdAgent
}

func (h *TestHarness) DcpAgent() *Agent {
	return h.dcpAgent
}

func (h *TestHarness) IsMockServer() bool {
	return h.mockInst != nil
}

func (h *TestHarness) SupportsFeature(feature TestFeatureCode) bool {
	featureFlagValue := 0
	for _, featureFlag := range h.FeatureFlags {
		if featureFlag.Feature == feature || featureFlag.Feature == "*" {
			if featureFlag.Enabled {
				featureFlagValue = +1
			} else {
				featureFlagValue = -1
			}
		}
	}
	if featureFlagValue == -1 {
		return false
	} else if featureFlagValue == +1 {
		return true
	}

	switch feature {
	case TestFeatureSsl:
		return true
	case TestFeatureViews:
		return true
	case TestFeatureErrMap:
		return true
	case TestFeatureReplicas:
		return true
	case TestFeatureN1ql:
		return !h.IsMockServer() && !h.ClusterVersion.Equal(srvVer700)
	case TestFeatureAdjoin:
		return !h.IsMockServer()
	case TestFeatureCollections:
		return !h.IsMockServer()
	case TestFeatureMemd:
		return !h.IsMockServer()
	case TestFeatureGetMeta:
		return !h.IsMockServer()
	case TestFeatureDCP:
		return !h.IsMockServer() &&
			!h.ClusterVersion.Lower(srvVer450)
	case TestFeatureDCPExpiry:
		return !h.IsMockServer() &&
			!h.ClusterVersion.Lower(srvVer650)
	}

	panic("found unsupported feature code")
}

func (h *TestHarness) TimeTravel(waitDura time.Duration) {
	if h.IsMockServer() {
		waitSecs := int(math.Ceil(float64(waitDura) / float64(time.Second)))
		h.mockInst.Control(gojcbmock.NewCommand(gojcbmock.CTimeTravel, map[string]interface{}{
			"Offset": waitSecs,
		}))
	} else {
		time.Sleep(waitDura)
	}
}

// Gets a set of keys evenly distributed across all server nodes.
// the result is an array of strings, each index representing an index
// of a server
func (h *TestHarness) MakeDistKeys(agent *Agent) (keys []string) {
	// Get the routing information
	clientMux := agent.kvMux.getState()
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
	*TestHarness

	sigT  *testing.T
	sigCh chan int
	sigOp PendingOp
}

func makeTestSubHarness(t *testing.T, h *TestHarness) *TestSubHarness {
	// Note that the signaling channel here must have a queue of
	// at least 1 to avoid deadlocks during cancellations.
	return &TestSubHarness{
		TestHarness: h,
		sigT:        t,
		sigCh:       make(chan int, 1),
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
