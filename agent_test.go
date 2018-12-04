package gocbcore

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/couchbaselabs/gojcbmock.v1"
)

// Gets a set of keys evenly distributed across all server nodes.
// the result is an array of strings, each index representing an index
// of a server
func (agent *Agent) makeDistKeys() (keys []string) {
	// Get the routing information
	cfg := agent.routingInfo.Get()
	keys = make([]string, cfg.clientMux.NumPipelines())
	remaining := len(keys)

	for i := 0; remaining > 0; i++ {
		keyTmp := fmt.Sprintf("DistKey_%d", i)
		// Map the vBucket and server
		vbID := cfg.vbMap.VbucketByKey([]byte(keyTmp))
		srvIx, err := cfg.vbMap.NodeByVbucket(vbID, 0)
		if err != nil || srvIx < 0 || srvIx >= len(keys) || keys[srvIx] != "" {
			continue
		}
		keys[srvIx] = keyTmp
		remaining--
	}
	return
}

func saslAuthFn(bucket, password string) func(AuthClient, time.Time) error {
	return func(srv AuthClient, deadline time.Time) error {
		// Build PLAIN auth data
		userBuf := []byte(bucket)
		passBuf := []byte(password)
		authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
		authData[0] = 0
		copy(authData[1:], userBuf)
		authData[1+len(userBuf)] = 0
		copy(authData[1+len(userBuf)+1:], passBuf)

		// Execute PLAIN authentication
		_, err := srv.ExecSaslAuth([]byte("PLAIN"), authData, deadline)

		return err
	}
}

const (
	defaultServerVersion = "5.1.0"
)

var globalAgent *testNode
var globalMemdAgent *testNode

func getAgent(t *testing.T) *testNode {
	return globalAgent
}

func getAgentnSignaler(t *testing.T) (*testNode, *Signaler) {
	agent := getAgent(t)
	return agent, agent.getSignaler(t)
}

func TestBasicOps(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	// Set
	s.PushOp(agent.Set([]byte("test"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	// Get
	s.PushOp(agent.Get([]byte("test"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	// GetReplica Specific
	s.PushOp(agent.GetReplica([]byte("test"), 1, func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	// GetReplica Any
	s.PushOp(agent.GetReplica([]byte("test"), 0, func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}

func TestBasicReplace(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	oldCas := Cas(0)
	s.PushOp(agent.Set([]byte("testx"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		oldCas = cas
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Replace([]byte("testx"), []byte("[]"), 0, oldCas, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Replace operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}

func TestBasicRemove(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Set([]byte("testy"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Remove([]byte("testy"), 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Remove operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func TestBasicInsert(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Remove([]byte("testz"), 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Add([]byte("testz"), []byte("[]"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Add operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)
}

func TestBasicCounters(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	// Counters
	s.PushOp(agent.Remove([]byte("testCounters"), 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Increment([]byte("testCounters"), 5, 11, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if val != 11 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Increment([]byte("testCounters"), 5, 22, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if val != 16 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Decrement([]byte("testCounters"), 3, 65, 0, func(val uint64, cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Increment operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if val != 13 {
				s.Fatalf("Increment did not operate properly")
			}
		})
	}))
	s.Wait(0)
}

func TestBasicAdjoins(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	if !agent.SupportsFeature(TestAdjoinFeature) {
		t.Skip("Test does not work against server version due to serverside bug")
	}

	s.PushOp(agent.Set([]byte("testAdjoins"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Append([]byte("testAdjoins"), []byte(" Frank!"), func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Append operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Prepend([]byte("testAdjoins"), []byte("Hello "), func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Prepend operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get([]byte("testAdjoins"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}

			if string(value) != "Hello there Frank!" {
				s.Fatalf("Adjoin operations did not behave")
			}
		})
	}))
	s.Wait(0)
}

func isKeyNotFoundError(err error) bool {
	te, ok := err.(interface {
		KeyNotFound() bool
	})
	return ok && te.KeyNotFound()
}

func TestExpiry(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Set([]byte("testExpiry"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(2000 * time.Millisecond)

	s.PushOp(agent.Get([]byte("testExpiry"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if !isKeyNotFoundError(err) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	}))
	s.Wait(0)
}

func TestTouch(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Set([]byte("testTouch"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Touch([]byte("testTouch"), 0, 3, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful")
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(2500 * time.Millisecond)

	s.PushOp(agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if !isKeyNotFoundError(err) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	}))
	s.Wait(0)
}

func TestGetAndTouch(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Set([]byte("testTouch"), []byte("{}"), 0, 1, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.GetAndTouch([]byte("testTouch"), 3, func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Touch operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(1500 * time.Millisecond)

	s.PushOp(agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get should have been successful: %v", err)
			}
		})
	}))
	s.Wait(0)

	agent.TimeTravel(2500 * time.Millisecond)

	s.PushOp(agent.Get([]byte("testTouch"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if !isKeyNotFoundError(err) {
				s.Fatalf("Get should have returned key not found")
			}
		})
	}))
	s.Wait(0)
}

func TestObserve(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Continue()
	}))
	s.Wait(0)

	s.PushOp(agent.Observe([]byte("testObserve"), 1, func(ks KeyState, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)
}

func TestObserveSeqNo(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	origMt := MutationToken{}
	s.PushOp(agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Initial set operation failed: %v", err)
			}

			if mt.VbUuid == 0 && mt.SeqNo == 0 {
				s.Skipf("ObserveSeqNo not supported by server")
			}

			origMt = mt
		})
	}))
	s.Wait(0)

	origCurSeqNo := SeqNo(0)
	s.PushOp(agent.ObserveSeqNo([]byte("testObserve"), origMt.VbUuid, 1, func(curSeqNo, persistSeqNo SeqNo, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("ObserveSeqNo operation failed: %v", err)
			}

			origCurSeqNo = curSeqNo
		})
	}))
	s.Wait(0)

	newMt := MutationToken{}
	s.PushOp(agent.Set([]byte("testObserve"), []byte("there"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Second set operation failed: %v", err)
			}

			newMt = mt
		})
	}))
	s.Wait(0)

	s.PushOp(agent.ObserveSeqNo([]byte("testObserve"), newMt.VbUuid, 1, func(curSeqNo, persistSeqNo SeqNo, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("ObserveSeqNo operation failed: %v", err)
			}
			if curSeqNo < origCurSeqNo {
				s.Fatalf("SeqNo does not appear to be working")
			}
		})
	}))
	s.Wait(0)
}

func TestRandomGet(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	distkeys := agent.makeDistKeys()
	for _, k := range distkeys {
		s.PushOp(agent.Set([]byte(k), []byte("Hello World!"), 0, 0, func(cas Cas, mt MutationToken, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Couldn't store some items: %v", err)
				}
			})
		}))
		s.Wait(0)
	}

	s.PushOp(agent.GetRandom(func(key, value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Get operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
			if len(key) == 0 {
				s.Fatalf("Invalid key returned")
			}
			if len(value) == 0 {
				s.Fatalf("No value returned")
			}
		})
	}))
	s.Wait(0)
}

func TestSubdocXattrs(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Set([]byte("testXattr"), []byte("{\"x\":\"xattrs\"}"), 0, 0, func(cas Cas, token MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Set operation failed: %v", err)
			}
		})
	}))
	s.Wait(0)

	mutateOps := []SubDocOp{
		{
			Op:    SubDocOpDictSet,
			Flags: SubdocFlagXattrPath | SubdocFlagMkDirP,
			Path:  "xatest.test",
			Value: []byte("\"test value\""),
		},
		// TODO: Turn on Macro Expansion part of the xattr test
		/*{
			Op: SubDocOpDictSet,
			Flags: SubdocFlagXattrPath | SubdocFlagExpandMacros | SubdocFlagMkDirP,
			Path: "xatest.rev",
			Value: []byte("\"${Mutation.CAS}\""),
		},*/
		{
			Op:    SubDocOpDictSet,
			Flags: SubdocFlagNone,
			Path:  "x",
			Value: []byte("\"x value\""),
		},
	}
	s.PushOp(agent.SubDocMutate([]byte("testXattr"), mutateOps, 0, 0, 0, func(res []SubDocResult, cas Cas, token MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Mutate operation failed: %v", err)
			}
			if cas == Cas(0) {
				s.Fatalf("Invalid cas received")
			}
		})
	}))
	s.Wait(0)

	lookupOps := []SubDocOp{
		{
			Op:    SubDocOpGet,
			Flags: SubdocFlagXattrPath,
			Path:  "xatest",
		},
		{
			Op:    SubDocOpGet,
			Flags: SubdocFlagNone,
			Path:  "x",
		},
	}
	s.PushOp(agent.SubDocLookup([]byte("testXattr"), lookupOps, 0, func(res []SubDocResult, cas Cas, err error) {
		s.Wrap(func() {
			if len(res) != 2 {
				s.Fatalf("Lookup operation wrong count")
			}
			if res[0].Err != nil {
				s.Fatalf("Lookup operation 1 failed: %v", res[0].Err)
			}
			if res[1].Err != nil {
				s.Fatalf("Lookup operation 2 failed: %v", res[1].Err)
			}

			/*
				xatest := fmt.Sprintf(`{"test":"test value","rev":"0x%016x"}`, cas)
				if !bytes.Equal(res[0].Value, []byte(xatest)) {
					s.Fatalf("Unexpected xatest value %s (doc) != %s (header)", res[0].Value, xatest)
				}
			*/
			if !bytes.Equal(res[0].Value, []byte(`{"test":"test value"}`)) {
				s.Fatalf("Unexpected xatest value %s", res[0].Value)
			}
			if !bytes.Equal(res[1].Value, []byte(`"x value"`)) {
				s.Fatalf("Unexpected document value %s", res[1].Value)
			}
		})
	}))
	s.Wait(0)
}

func TestStats(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	numServers := agent.routingInfo.Get().clientMux.NumPipelines()

	s.PushOp(agent.Stats("", func(stats map[string]SingleServerStats) {
		s.Wrap(func() {
			if len(stats) != numServers {
				s.Fatalf("Didn't get all stats!")
			}
			for srv, curStats := range stats {
				if curStats.Error != nil {
					s.Fatalf("Got error %v in stats for %s", curStats.Error, srv)
				}

				if curStats.Stats == nil || len(curStats.Stats) == 0 {
					s.Fatalf("Got no stats in stats for %s", srv)
				}
			}
		})
	}))
	s.Wait(0)
}

func TestGetHttpEps(t *testing.T) {
	agent := getAgent(t)

	// Relies on a 3.0.0+ server
	n1qlEpList := agent.N1qlEps()
	if len(n1qlEpList) == 0 {
		t.Fatalf("Failed to retrieve N1QL endpoint list")
	}

	mgmtEpList := agent.MgmtEps()
	if len(mgmtEpList) == 0 {
		t.Fatalf("Failed to retrieve N1QL endpoint list")
	}

	capiEpList := agent.CapiEps()
	if len(capiEpList) == 0 {
		t.Fatalf("Failed to retrieve N1QL endpoint list")
	}
}

func TestMemcachedBucket(t *testing.T) {
	// Ensure we can do upserts..
	agent := globalMemdAgent
	s := agent.getSignaler(t)

	s.PushOp(agent.Set([]byte("key"), []byte("value"), 0, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get([]byte("key"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Couldn't get back key: %v", err)
			}
			if string(value) != "value" {
				s.Fatalf("Got back wrong value!")
			}
		})
	}))
	s.Wait(0)

	// Try to perform Observe: should fail since this isn't supported on Memcached buckets
	_, err := agent.Observe([]byte("key"), 0, func(ks KeyState, cas Cas, err error) {
		s.Wrap(func() {
			s.Fatalf("Scheduling should fail on memcached buckets!")
		})
	})

	if err != ErrNotSupported {
		t.Fatalf("Expected observe error for memcached bucket!")
	}
}

func TestFlagsRoundTrip(t *testing.T) {
	// Ensure flags are round-tripped with the server correctly.
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Set([]byte("flagskey"), []byte(""), 0x99889988, 0, func(cas Cas, mt MutationToken, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Got error for Set: %v", err)
			}
		})
	}))
	s.Wait(0)

	s.PushOp(agent.Get([]byte("flagskey"), func(value []byte, flags uint32, cas Cas, err error) {
		s.Wrap(func() {
			if err != nil {
				s.Fatalf("Couldn't get back key: %v", err)
			}
			if flags != 0x99889988 {
				s.Fatalf("flags failed to round-trip")
			}
		})
	}))
	s.Wait(0)
}

func TestMetaOps(t *testing.T) {
	// Currently disabled as CouchbaseMock does not support it
	/*
		agent, s := getAgentnSignaler(t)

		var currentCas Cas

		// Set
		s.PushOp(agent.Set([]byte("test"), []byte("{}"), 0, 0, func(cas Cas, mt MutationToken, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("Set operation failed")
				}
				if cas == Cas(0) {
					s.Fatalf("Invalid cas received")
				}

				currentCas = cas
			})
		})
		s.Wait(0)

		// GetMeta
		s.PushOp(agent.GetMeta([]byte("test"), func(value []byte, flags uint32, cas Cas, expiry uint32, seqNo SeqNo, dataType uint8, deleted uint32, err error) {
			s.Wrap(func() {
				if err != nil {
					s.Fatalf("GetMeta operation failed")
				}
				if expiry != 0 {
					s.Fatalf("Invalid expiry received")
				}
				if deleted != 0 {
					s.Fatalf("Invalid deleted flag received")
				}
				if cas != currentCas {
					s.Fatalf("Invalid cas received")
				}
			})
		})
		s.Wait(0)
	*/
}

func TestPing(t *testing.T) {
	agent, s := getAgentnSignaler(t)

	s.PushOp(agent.Ping(func(services []PingResult) {
		s.Wrap(func() {
			if len(services) == 0 {
				s.Fatalf("Ping report contained no results")
			}
		})
	}))
	s.Wait(5)
}

func TestDiagnostics(t *testing.T) {
	agent, _ := getAgentnSignaler(t)

	report, err := agent.Diagnostics()
	if err != nil {
		t.Fatalf("Failed to fetch diagnostics: %s", err)
	}

	if len(report.MemdConns) == 0 {
		t.Fatalf("Diagnostics report contained no results")
	}

	for _, conn := range report.MemdConns {
		if conn.RemoteAddr == "" {
			t.Fatalf("Diagnostic report contained invalid entry")
		}
	}
}

func TestAlternateAddressesEmptyStringConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	initialNetworkType := globalAgent.networkType
	globalAgent.networkType = ""
	cfg := globalAgent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if globalAgent.networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", globalAgent.networkType)
	}

	for i, server := range cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
	globalAgent.networkType = initialNetworkType
}

func TestAlternateAddressesAutoConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	initialNetworkType := globalAgent.networkType
	globalAgent.networkType = "auto"
	cfg := globalAgent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if globalAgent.networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", globalAgent.networkType)
	}

	for i, server := range cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
	globalAgent.networkType = initialNetworkType
}

func TestAlternateAddressesAutoInternalConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	initialNetworkType := globalAgent.networkType
	globalAgent.networkType = "auto"
	cfg := globalAgent.buildFirstRouteConfig(cfgBk, "172.17.0.4:11210")

	if globalAgent.networkType != "default" {
		t.Fatalf("Expected agent networkType to be external, was %s", globalAgent.networkType)
	}

	for i, server := range cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
	globalAgent.networkType = initialNetworkType
}

func TestAlternateAddressesDefaultConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	initialNetworkType := globalAgent.networkType
	globalAgent.networkType = "default"
	cfg := globalAgent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if globalAgent.networkType != "default" {
		t.Fatalf("Expected agent networkType to be default, was %s", globalAgent.networkType)
	}

	for i, server := range cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.Services.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
	globalAgent.networkType = initialNetworkType
}

func TestAlternateAddressesExternalConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	initialNetworkType := globalAgent.networkType
	globalAgent.networkType = "external"
	cfg := globalAgent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if globalAgent.networkType != "external" {
		t.Fatalf("Expected agent networkType to be external, was %s", globalAgent.networkType)
	}

	for i, server := range cfg.kvServerList {
		cfgBkNode := cfgBk.NodesExt[i]
		port := cfgBkNode.AltAddresses["external"].Ports.Kv
		cfgBkServer := fmt.Sprintf("%s:%d", cfgBkNode.AltAddresses["external"].Hostname, port)
		if server != cfgBkServer {
			t.Fatalf("Expected kv server to be %s but was %s", cfgBkServer, server)
		}
	}
	globalAgent.networkType = initialNetworkType
}

func TestAlternateAddressesInvalidConfig(t *testing.T) {
	cfgBk := loadConfigFromFile(t, "testdata/bucket_config_with_external_addresses.json")

	initialNetworkType := globalAgent.networkType
	globalAgent.networkType = "invalid"
	cfg := globalAgent.buildFirstRouteConfig(cfgBk, "192.168.132.234:32799")

	if globalAgent.networkType != "invalid" {
		t.Fatalf("Expected agent networkType to be invalid, was %s", globalAgent.networkType)
	}

	if cfg.IsValid() {
		t.Fatalf("Expected route config to be invalid, was valid")
	}
	if len(cfg.kvServerList) != 0 {
		t.Fatalf("Expected kvServerList to be empty, had %d items", len(cfg.kvServerList))
	}
	globalAgent.networkType = initialNetworkType
}

type testLogger struct {
	Parent   Logger
	LogCount []uint64
}

func (logger *testLogger) Log(level LogLevel, offset int, format string, v ...interface{}) error {
	if level >= 0 && level < LogMaxVerbosity {
		atomic.AddUint64(&logger.LogCount[level], 1)
	}

	return logger.Parent.Log(level, offset+1, format, v...)
}

func createTestLogger() *testLogger {
	return &testLogger{
		Parent:   VerboseStdioLogger(),
		LogCount: make([]uint64, LogMaxVerbosity),
	}
}

func TestMain(m *testing.M) {
	initialGoroutineCount := runtime.NumGoroutine()

	// Set up our special logger which logs the log level count
	logger := createTestLogger()
	SetLogger(logger)

	memdservers := flag.String("memdservers", "", "Comma separated list of connection strings to connect to for real memd servers")
	httpservers := flag.String("httpservers", "", "Comma separated list of connection strings to connect to for real http servers")
	bucketName := flag.String("bucket", "default", "The bucket to use to test against")
	memdBucketName := flag.String("memd-bucket", "memd", "The memd bucket to use to test against")
	user := flag.String("user", "", "The username to use to authenticate when using a real server")
	password := flag.String("pass", "", "The password to use to authenticate when using a real server")
	version := flag.String("version", "", "The server version being tested against (major.minor.patch.build_edition)")
	flag.Parse()

	if (*memdservers == "") != (*httpservers == "") {
		panic("If one of memdservers or httpservers is present then both must be present")
	}

	var err error
	var httpAuthHandler func(AuthClient, time.Time) error
	var memdAuthHandler func(AuthClient, time.Time) error
	var memdAddrs []string
	var httpAddrs []string

	var mock *gojcbmock.Mock
	if *memdservers == "" {
		if *version != "" {
			panic("Version cannot be specified with mock")
		}
		mpath, err := gojcbmock.GetMockPath()
		if err != nil {
			panic(err.Error())
		}

		mock, err = gojcbmock.NewMock(mpath, 4, 1, 64, []gojcbmock.BucketSpec{
			{Name: "default", Type: gojcbmock.BCouchbase},
			{Name: "memd", Type: gojcbmock.BMemcached},
		}...)

		if err != nil {
			panic(err.Error())
		}
		for _, mcport := range mock.MemcachedPorts() {
			memdAddrs = append(memdAddrs, fmt.Sprintf("127.0.0.1:%d", mcport))
		}

		httpAddrs = []string{fmt.Sprintf("127.0.0.1:%d", mock.EntryPort)}

		httpAuthHandler = saslAuthFn("default", "")
		memdAuthHandler = saslAuthFn("memd", "")

		*version = mock.Version()
	} else {
		memdAddrs = strings.Split(*memdservers, ",")
		httpAddrs = strings.Split(*httpservers, ",")

		if *version == "" {
			*version = defaultServerVersion
		}
	}

	nodeVersion, err := nodeVersionFromString(*version)
	if err != nil {
		panic(fmt.Sprintf("Failed to get node version from string: %v", err))
	}

	agentConfig := &AgentConfig{
		MemdAddrs:            memdAddrs,
		HttpAddrs:            httpAddrs,
		TlsConfig:            nil,
		BucketName:           *bucketName,
		Username:             *user,
		Password:             *password,
		AuthHandler:          httpAuthHandler,
		ConnectTimeout:       5 * time.Second,
		ServerConnectTimeout: 1 * time.Second,
		UseMutationTokens:    true,
		UseKvErrorMaps:       true,
		UseEnhancedErrors:    true,
	}

	agent, err := CreateAgent(agentConfig)
	if err != nil {
		panic("Failed to connect to server")
	}
	globalAgent = &testNode{
		Agent:   agent,
		Mock:    mock,
		Version: nodeVersion,
	}

	memdAgentConfig := &AgentConfig{}
	*memdAgentConfig = *agentConfig
	memdAgentConfig.MemdAddrs = nil
	memdAgentConfig.BucketName = *memdBucketName
	memdAgentConfig.Username = *user
	memdAgentConfig.Password = *password
	memdAgentConfig.AuthHandler = memdAuthHandler
	memdAgent, err := CreateAgent(memdAgentConfig)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to memcached bucket!: %v", err))
	}
	globalMemdAgent = &testNode{
		Agent:   memdAgent,
		Mock:    mock,
		Version: nodeVersion,
	}

	result := m.Run()

	err = agent.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to shut down global agent: %s", err))
	}

	err = globalMemdAgent.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to shut down global memcached agent: %s", err))
	}

	log.Printf("Log Messages Emitted:")
	for i := 0; i < int(LogMaxVerbosity); i++ {
		log.Printf("  (%s): %d", logLevelToString(LogLevel(i)), logger.LogCount[i])
	}

	abnormalLogCount := logger.LogCount[LogError] + logger.LogCount[LogWarn]
	if abnormalLogCount > 0 {
		log.Printf("Detected unexpected logging, failing")
		result = 1
	}

	// Loop for at most a second checking for goroutines leaks, this gives any HTTP goroutines time to shutdown
	start := time.Now()
	var finalGoroutineCount int
	for time.Now().Sub(start) <= 1*time.Second {
		runtime.Gosched()
		finalGoroutineCount = runtime.NumGoroutine()
		if finalGoroutineCount == initialGoroutineCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if finalGoroutineCount != initialGoroutineCount {
		log.Printf("Detected a goroutine leak (%d before != %d after), failing", initialGoroutineCount, finalGoroutineCount)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		result = 1
	} else {
		log.Printf("No goroutines appear to have leaked (%d before == %d after)", initialGoroutineCount, finalGoroutineCount)
	}

	os.Exit(result)
}
