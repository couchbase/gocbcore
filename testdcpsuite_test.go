package gocbcore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/stretchr/testify/suite"
)

type DCPTestSuite struct {
	suite.Suite

	*DCPTestConfig
	opAgent  *Agent
	dcpAgent *DCPAgent
	so       *TestStreamObserver
}

func (suite *DCPTestSuite) SupportsFeature(feature TestFeatureCode) bool {
	featureFlagValue := 0
	for _, featureFlag := range suite.FeatureFlags {
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
	case TestFeatureDCPExpiry:
		return !suite.ClusterVersion.Lower(srvVer650)
	case TestFeatureDCPDeleteTimes:
		return !suite.ClusterVersion.Lower(srvVer650)
	case TestFeatureCollections:
		return !suite.ClusterVersion.Lower(srvVer700)
	}

	panic("found unsupported feature code")
}

func (suite *DCPTestSuite) EnsureSupportsFeature(feature TestFeatureCode) {
	if !suite.SupportsFeature(feature) {
		suite.T().Skipf("Skipping test due to disabled feature code: %s", feature)
	}
}

func (suite *DCPTestSuite) SetupSuite() {
	suite.DCPTestConfig = globalDCPTestConfig
	suite.Require().NotEmpty(suite.DCPTestConfig.ConnStr, "Connection string cannot be empty for testing DCP")

	var err error
	suite.opAgent, err = suite.initAgent(suite.makeOpAgentConfig(suite.DCPTestConfig))
	suite.Require().Nil(err)

	flags := memd.DcpOpenFlagProducer

	if suite.SupportsFeature(TestFeatureDCPDeleteTimes) {
		flags |= memd.DcpOpenFlagIncludeDeleteTimes
	}

	suite.dcpAgent, err = suite.initDCPAgent(
		suite.makeDCPAgentConfig(suite.DCPTestConfig, suite.SupportsFeature(TestFeatureDCPExpiry)),
		flags,
	)
	suite.Require().Nil(err, err)

	suite.so = &TestStreamObserver{
		lock:      sync.Mutex{},
		lastSeqno: make(map[uint16]uint64),
		snapshots: make(map[uint16]SnapshotMarker),
		endWg:     sync.WaitGroup{},
	}
}

func (suite *DCPTestSuite) TearDownSuite() {
	if suite.opAgent != nil {
		suite.opAgent.Close()
		suite.opAgent = nil
	}

	if suite.dcpAgent != nil {
		suite.dcpAgent.Close()
		suite.dcpAgent = nil
	}
}

func (suite *DCPTestSuite) makeOpAgentConfig(testConfig *DCPTestConfig) AgentConfig {
	config := AgentConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.UseMutationTokens = true
	config.UseCollections = true
	config.BucketName = testConfig.BucketName

	config.Auth = testConfig.Authenticator

	if testConfig.CAProvider != nil {
		config.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func (suite *DCPTestSuite) initAgent(config AgentConfig) (*Agent, error) {
	agent, err := CreateAgent(&config)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(10*time.Second),
		WaitUntilReadyOptions{},
		func(result *WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return nil, err
	}

	err = <-ch
	if err != nil {
		return nil, err
	}

	return agent, nil
}

func (suite *DCPTestSuite) makeDCPAgentConfig(testConfig *DCPTestConfig, expiryEnabled bool) DCPAgentConfig {
	config := DCPAgentConfig{}
	config.FromConnStr(testConfig.ConnStr)

	config.UseCollections = suite.SupportsFeature(TestFeatureCollections)
	config.BucketName = testConfig.BucketName

	if expiryEnabled {
		config.UseExpiryOpcode = true
	}

	config.Auth = testConfig.Authenticator

	if testConfig.CAProvider != nil {
		config.TLSRootCAProvider = testConfig.CAProvider
	}

	return config
}

func (suite *DCPTestSuite) initDCPAgent(config DCPAgentConfig, openFlags memd.DcpOpenFlag) (*DCPAgent, error) {
	agent, err := CreateDcpAgent(&config, "test-stream", openFlags)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(10*time.Second),
		WaitUntilReadyOptions{},
		func(result *WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return nil, err
	}

	err = <-ch
	if err != nil {
		return nil, err
	}

	return agent, nil
}

func TestDCPSuite(t *testing.T) {
	if globalDCPTestConfig == nil {
		t.Skip()
	}
	suite.Run(t, new(DCPTestSuite))
}

func (suite *DCPTestSuite) runMutations(collection, scope string) (map[string]string, []string) {
	expirationsLeft := suite.NumExpirations
	deletionsLeft := int32(suite.NumDeletions)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(suite.NumMutations)

	mutations := make(map[string]string)
	var deletionKeys []string
	lock := new(sync.Mutex)

	for i := 0; i < suite.NumMutations; i++ {
		var expiry uint32
		if expirationsLeft > 0 {
			expiry = 1
			expirationsLeft--
		}

		go func(ex uint32, id int) {
			ch := make(chan error)
			op, err := suite.opAgent.Set(
				SetOptions{
					Key:            []byte(fmt.Sprintf("key-%d", id)),
					Value:          []byte(fmt.Sprintf("value-%d", id)),
					Expiry:         ex,
					ScopeName:      scope,
					CollectionName: collection,
				}, func(result *StoreResult, err error) {
					ch <- err
				},
			)
			if err != nil {
				suite.T().Logf("Canceling due to failure sending set: %v", err)
				cancel()
				return
			}

			select {
			case err := <-ch:
				if err != nil {
					suite.T().Logf("Canceling due to failure performing set: %v", err)
					cancel()
					return
				}
			case <-ctx.Done():
				op.Cancel()
				return
			}

			if expiry == 0 {
				dLeft := atomic.AddInt32(&deletionsLeft, -1)
				if dLeft >= 0 {
					lock.Lock()
					deletionKeys = append(deletionKeys, fmt.Sprintf("key-%d", id))
					lock.Unlock()
					ch = make(chan error)
					op, err := suite.opAgent.Delete(
						DeleteOptions{
							Key:            []byte(fmt.Sprintf("key-%d", id)),
							CollectionName: collection,
							ScopeName:      scope,
						}, func(result *DeleteResult, err error) {
							ch <- err
						},
					)
					if err != nil {
						suite.T().Logf("Canceling due to failure sending delete: %v", err)
						cancel()
						return
					}

					select {
					case err := <-ch:
						if err != nil {
							suite.T().Logf("Canceling due to failure performing delete: %v", err)
							cancel()
							return
						}
					case <-ctx.Done():
						op.Cancel()
						return
					}
				} else {
					lock.Lock()
					mutations[fmt.Sprintf("key-%d", id)] = fmt.Sprintf("value-%d", id)
					lock.Unlock()
				}
			}
			wg.Done()
		}(expiry, i)
	}

	wgCh := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		suite.T().Fatalf("Failed to perform mutations due to %v", ctx.Err())
	case <-wgCh:
		cancel()
		// Let any expirations do their thing
		time.Sleep(5 * time.Second)
	}

	return mutations, deletionKeys
}

func (suite *DCPTestSuite) getFailoverLogs(nVB int) (map[int]FailoverEntry, error) {
	ch := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	failOverEntries := make(map[int]FailoverEntry)

	var openWg sync.WaitGroup
	openWg.Add(nVB)
	lock := sync.Mutex{}

	for i := 0; i < nVB; i++ {
		go func(vbId uint16) {
			op, err := suite.dcpAgent.GetFailoverLog(vbId, func(entries []FailoverEntry, err error) {
				for _, en := range entries {
					lock.Lock()
					failOverEntries[int(vbId)] = en
					lock.Unlock()
				}
				ch <- err
			})

			if err != nil {
				cancel()
				return
			}

			select {
			case err := <-ch:
				if err != nil {
					fmt.Printf("Error received from get failover logs: %v", err)
					cancel()
					return
				}
			case <-ctx.Done():
				op.Cancel()
				return
			}

			openWg.Done()
		}(uint16(i))
	}

	wgCh := make(chan struct{}, 1)
	go func() {
		openWg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.New("Failed to get failoverlogs")
	case <-wgCh:
		cancel()
	}

	return failOverEntries, nil
}

//Runs a dcp stream on all VBs from the last snapshot to the current seqno
func (suite *DCPTestSuite) runDCPStream() int {
	suite.so.newCounter()
	seqnos, err := suite.getCurrentSeqNos()
	suite.Require().Nil(err, err)

	suite.T().Logf("Running to seqno map: %v", seqnos)

	suite.so.endWg.Add(len(seqnos))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var openWg sync.WaitGroup
	openWg.Add(len(seqnos))

	fo, err := suite.getFailoverLogs(len(seqnos))
	suite.Require().Nil(err, err)

	//Start streaming from all VBs from the latest snapshot, until the current seqno
	for _, entry := range seqnos {
		go func(en VbSeqNoEntry) {
			ch := make(chan error)
			suite.so.lock.Lock()
			snapshot := suite.so.snapshots[en.VbID]
			suite.so.lock.Unlock()

			op, err := suite.dcpAgent.OpenStream(en.VbID, memd.DcpStreamAddFlagActiveOnly, fo[int(en.VbID)].VbUUID, SeqNo(snapshot.lastSnapEnd), en.SeqNo,
				SeqNo(snapshot.lastSnapStart), SeqNo(snapshot.lastSnapEnd), suite.so, OpenStreamOptions{}, func(entries []FailoverEntry, err error) {
					ch <- err
				},
			)
			if err != nil {
				cancel()
				return
			}

			select {
			case err := <-ch:
				if err != nil {
					suite.T().Logf("Error received from open stream: %v", err)
					cancel()
					return
				}
			case <-ctx.Done():
				op.Cancel()
				return
			}

			openWg.Done()
		}(entry)
	}

	wgCh := make(chan struct{}, 1)
	go func() {
		openWg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		suite.T().Fatal("Failed to open streams")
	case <-wgCh:
		cancel()
		// Let any expirations do their thing
		time.Sleep(5 * time.Second)
	}

	suite.T().Logf("All streams open, waiting for streams to complete")

	waitCh := make(chan struct{})
	go func() {
		suite.so.endWg.Wait()
		close(waitCh)
	}()

	select {
	case <-time.After(60 * time.Second):
		suite.T().Fatal("Timed out waiting for streams to complete")
	case <-waitCh:
	}

	suite.T().Logf("All streams complete")
	return len(seqnos)
}

func (suite *DCPTestSuite) getCurrentSeqNos() ([]VbSeqNoEntry, error) {
	h := makeTestSubHarness(suite.T())

	var seqnos []VbSeqNoEntry
	snapshot, err := suite.dcpAgent.ConfigSnapshot()
	suite.Require().Nil(err, err)

	numNodes, err := snapshot.NumServers()
	suite.Require().Nil(err, err)
	//Get all SeqNos
	for i := 1; i < numNodes+1; i++ {
		h.PushOp(suite.dcpAgent.GetVbucketSeqnos(i, memd.VbucketStateActive, GetVbucketSeqnoOptions{},
			func(entries []VbSeqNoEntry, err error) {
				h.Wrap(func() {
					if err != nil {
						h.Fatalf("GetVbucketSeqnos operation failed: %v", err)
						return
					}

					seqnos = append(seqnos, entries...)
				})
			}))
		h.Wait(0)
	}
	return seqnos, nil
}

func (suite *DCPTestSuite) TestBasic() {
	mutations, deletionKeys := suite.runMutations("", "")

	suite.runDCPStream()

	// Compaction can run and cause expirations to be hidden from us
	suite.Assert().InDelta(suite.NumMutations, len(suite.so.counter.mutations), float64(suite.NumExpirations))
	suite.Assert().Equal(suite.NumDeletions, len(suite.so.counter.deletions))
	suite.Assert().InDelta(suite.NumExpirations, len(suite.so.counter.expirations), float64(suite.NumExpirations))

	for key, val := range mutations {
		if suite.Assert().Contains(suite.so.counter.mutations, key) {
			suite.Assert().Equal(string(suite.so.counter.mutations[key].Value), val)
		}
	}

	for _, key := range deletionKeys {
		suite.Assert().Contains(suite.so.counter.deletions, key)
	}
}

func (suite *DCPTestSuite) TestScopesBasic() {
	suite.EnsureSupportsFeature(TestFeatureCollections)

	prefix := "dcp_scope_sbasic"
	scopes := suite.makeScopes(suite.NumScopes, prefix, suite.BucketName, suite.opAgent)

	nVB := suite.runDCPStream()

	pScopes := suite.getPrunedScopeManifests(prefix, scopes)
	suite.Assert().Equal(len(pScopes), len(suite.so.counter.scopes))

	for _, val := range pScopes {
		suite.Assert().Equal(nVB, suite.so.counter.scopes[val.Name])
	}

}

func (suite *DCPTestSuite) TestScopesDrops() {
	suite.EnsureSupportsFeature(TestFeatureCollections)

	//Make scopes
	prefix := "dcp_scope_sdrops"
	scopes := suite.makeScopes(suite.NumCollections, prefix, suite.BucketName, suite.opAgent)

	//Drop all scopes created in this test
	pScopes := suite.getPrunedScopeManifests(prefix, scopes)
	suite.dropScopes(pScopes, suite.BucketName, suite.opAgent)

	nVB := suite.runDCPStream()

	suite.Assert().Equal(suite.NumCollections, len(suite.so.counter.scopesDeleted))

	for _, val := range pScopes {
		suite.Assert().Equal(nVB, suite.so.counter.scopesDeleted[strconv.Itoa(int(val.UID))], fmt.Sprintf("For scope %s", val.Name))
	}

}

func (suite *DCPTestSuite) TestCollectionsBasic() {
	suite.EnsureSupportsFeature(TestFeatureCollections)

	//Make scopes
	prefix := "dcp_scope_cbasic"
	scopes := suite.makeScopes(suite.NumScopes, prefix, suite.BucketName, suite.opAgent)

	//Make NumCollections per scope
	pScopes := suite.getPrunedScopeManifests(prefix, scopes)
	lastScopeManifest := suite.makeCollections(suite.NumCollections, "dcp_collection_cbasic", pScopes, suite.BucketName, suite.opAgent)
	pScopes = suite.getPrunedScopeManifests(prefix, lastScopeManifest)

	nVB := suite.runDCPStream()

	suite.Assert().Equal(suite.NumScopes, len(suite.so.counter.scopes))
	suite.Assert().Equal(suite.NumCollections*suite.NumScopes, len(suite.so.counter.collections))

	for _, val := range pScopes {
		suite.Assert().Equal(nVB, suite.so.counter.scopes[val.Name])
		for _, c := range val.Collections {
			suite.Assert().Equal(nVB, suite.so.counter.collections[strconv.Itoa(int(val.UID))+"."+c.Name])
		}
	}
}

func (suite *DCPTestSuite) TestCollectionsScopeDrop() {
	suite.EnsureSupportsFeature(TestFeatureCollections)

	//Make scopes
	prefix := "dcp_scope_csdrop"
	scopes := suite.makeScopes(suite.NumScopes, prefix, suite.BucketName, suite.opAgent)

	//Make NumCollections per scope
	pScopes := suite.getPrunedScopeManifests(prefix, scopes)
	lastScopeManifest := suite.makeCollections(suite.NumCollections, "dcp_collection_csdrop", pScopes, suite.BucketName, suite.opAgent)
	pScopes = suite.getPrunedScopeManifests(prefix, lastScopeManifest)

	//Drop all scopes created in this test, implicitly dropping all the collections
	suite.dropScopes(pScopes, suite.BucketName, suite.opAgent)

	nVB := suite.runDCPStream()

	suite.Assert().Equal(suite.NumScopes, len(suite.so.counter.scopesDeleted))
	suite.Assert().Equal(suite.NumCollections*suite.NumScopes, len(suite.so.counter.collectionsDeleted))

	for _, s := range pScopes {
		suite.Assert().Equal(nVB, suite.so.counter.scopesDeleted[strconv.Itoa(int(s.UID))], fmt.Sprintf("For scope %s", s.Name))
		for _, c := range s.Collections {
			suite.Assert().Equal(nVB, suite.so.counter.collectionsDeleted[strconv.Itoa(int(s.UID))+"."+strconv.Itoa(int(c.UID))])
		}
	}
}

func (suite *DCPTestSuite) TestCollectionsDrop() {
	suite.EnsureSupportsFeature(TestFeatureCollections)

	//Make scopes
	prefix := "dcp_scope_ccdrop"
	scopes := suite.makeScopes(suite.NumScopes, prefix, suite.BucketName, suite.opAgent)

	//Make NumCollections per scope
	pScopes := suite.getPrunedScopeManifests(prefix, scopes)
	lastScopeManifest := suite.makeCollections(suite.NumCollections, "dcp_collection_ccdrop", pScopes, suite.BucketName, suite.opAgent)
	pScopes = suite.getPrunedScopeManifests(prefix, lastScopeManifest)

	//Drop all collections created in this test
	suite.dropCollections(pScopes, suite.BucketName, suite.opAgent)

	nVB := suite.runDCPStream()

	suite.Assert().Equal(suite.NumScopes, len(suite.so.counter.scopes))
	suite.Assert().Equal(suite.NumCollections*suite.NumScopes, len(suite.so.counter.collectionsDeleted))

	for _, s := range pScopes {
		suite.Assert().Equal(nVB, suite.so.counter.scopes[s.Name])
		for _, c := range s.Collections {
			suite.Assert().Equal(nVB, suite.so.counter.collectionsDeleted[strconv.Itoa(int(s.UID))+"."+strconv.Itoa(int(c.UID))])
		}
	}
}

func (suite *DCPTestSuite) TestMutationsCollection() {
	suite.EnsureSupportsFeature(TestFeatureCollections)

	//Make scopes
	sPrefix := "dcp_scope_mut"
	cPrefix := "dcp_collection_mut"
	scopes := suite.makeScopes(suite.NumScopes, sPrefix, suite.BucketName, suite.opAgent)

	//Make NumCollections per scope
	pScopes := suite.getPrunedScopeManifests(sPrefix, scopes)
	lastScopeManifest := suite.makeCollections(suite.NumCollections, cPrefix, pScopes, suite.BucketName, suite.opAgent)
	suite.getPrunedScopeManifests(sPrefix, lastScopeManifest)
	time.Sleep(5 * time.Second) //Needed to ensure collection ready before performing mutations.
	mutations, deletionKeys := suite.runMutations(cPrefix+"0", sPrefix+"0")

	suite.runDCPStream()

	// Compaction can run and cause expirations to be hidden from us
	suite.Assert().InDelta(suite.NumMutations, len(suite.so.counter.mutations), float64(suite.NumExpirations))
	suite.Assert().Equal(suite.NumDeletions, len(suite.so.counter.deletions))
	suite.Assert().InDelta(suite.NumExpirations, len(suite.so.counter.expirations), float64(suite.NumExpirations))

	for key, val := range mutations {
		if suite.Assert().Contains(suite.so.counter.mutations, key) {
			suite.Assert().Equal(string(suite.so.counter.mutations[key].Value), val)
		}
	}

	for _, key := range deletionKeys {
		suite.Assert().Contains(suite.so.counter.deletions, key)
	}
}

func (suite *DCPTestSuite) makeScopes(n int, prefix, bucketName string, agent *Agent) []ManifestScope {
	var scopes []string
	var err error
	var m *Manifest
	for i := 0; i < n; i++ {
		s := prefix + strconv.Itoa(i)
		scopes = append(scopes, s)
		m, err = testCreateScope(s, bucketName, agent)
		suite.Require().Nil(err, err)
	}
	return m.Scopes
}

//Return only the scope manifests with the provided prefix name
func (suite *DCPTestSuite) getPrunedScopeManifests(prefix string, sm []ManifestScope) []ManifestScope {
	var prunedScopes []ManifestScope
	for _, s := range sm {
		match, err := regexp.Match(prefix+"+", []byte(s.Name))
		suite.Require().Nil(err, err)
		if s.Name != "_default" && match {
			prunedScopes = append(prunedScopes, s)
		}
	}
	return prunedScopes
}

func (suite *DCPTestSuite) dropScopes(scopes []ManifestScope, bucketName string, agent *Agent) {
	for _, s := range scopes {
		_, err := testDeleteScope(s.Name, bucketName, agent, true)
		suite.Require().Nil(err, err)
	}
}

func (suite *DCPTestSuite) dropCollections(scopes []ManifestScope, bucketName string, agent *Agent) {
	for _, s := range scopes {
		for _, c := range s.Collections {
			_, err := testDeleteCollection(c.Name, s.Name, bucketName, agent, true)
			suite.Require().Nil(err, err)
		}
	}
}

func (suite *DCPTestSuite) makeCollections(n int, prefix string, scopes []ManifestScope, bucketName string, agent *Agent) []ManifestScope {
	var m *Manifest
	var err error
	for _, s := range scopes {
		for i := 0; i < n; i++ {
			c := prefix + strconv.Itoa(i)
			m, err = testCreateCollection(c, s.Name, bucketName, agent)
			suite.Require().Nil(err, err)
		}
	}

	return m.Scopes
}
