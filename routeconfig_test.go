package gocbcore

import (
	"encoding/json"
	"io/ioutil"
	"testing"
)

func loadConfigFromFile(t *testing.T, filename string) (cfg *cfgBucket) {
	s, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err.Error())
	}
	rawCfg, err := parseConfig(s, "localhost")
	if err != nil {
		t.Fatal(err.Error())
	}

	cfg = rawCfg
	return
}

func getConfig(t *testing.T, filename string) (cfg *routeConfig) {
	cfgBk := loadConfigFromFile(t, filename)
	cfg = buildRouteConfig(cfgBk, false, "default", false)
	return
}

func TestGoodConfig(t *testing.T) {
	// Parsing an old 2.x config
	cfg := getConfig(t, "testdata/full_25.json")
	if cfg.bktType != bktTypeCouchbase {
		t.Fatal("Wrong bucket type!")
	}
	if cfg.vbMap.NumReplicas() != 1 {
		t.Fatal("Expected 2 replicas!")
	}
}

func TestBadConfig(t *testing.T) {
	// Has an empty vBucket map
	cfg := getConfig(t, "testdata/bad.json")
	if cfg.IsValid() {
		t.Fatal("Config without vbuckets should be invalid!")
	}
}

// TODO(mnunberg): Should test ketama separately from memcached config testing.
func TestKetama(t *testing.T) {
	cfg := getConfig(t, "testdata/memd_4node.config.json")
	if cfg.bktType != bktTypeMemcached {
		t.Fatal("Wrong bucket type!")
	}
	if len(cfg.kvServerList) != 4 {
		t.Fatal("Expected 4 nodes!")
	}
	if len(cfg.ketamaMap.entries) != 160*4 {
		t.Fatal("Bad length for ketama map")
	}

	type routeEnt struct {
		Hash  uint32 `json:"hash"`
		Index uint32 `json:"index"`
	}
	exp := make(map[string]routeEnt)
	rawExp, err := ioutil.ReadFile("testdata/memd_4node.exp.json")
	if err != nil {
		t.Fatalf("Couldn't open expected results! %v", err)
	}

	if err = json.Unmarshal(rawExp, &exp); err != nil {
		t.Fatalf("Bad JSON in expected output")
	}

	for k, expCur := range exp {
		hash := ketamaHash([]byte(k))
		if hash != expCur.Hash {
			t.Fatalf("Bad hash for %s. Expected %d but got %d", k, expCur.Hash, hash)
		}

		index, _ := cfg.ketamaMap.NodeByKey([]byte(k))
		if index != int(expCur.Index) {
			t.Fatalf("Bad index for %s (hash=%d). Expected %v but got %d", k, expCur.Hash, expCur.Index, index)
		}
	}
}

func TestNodeExtConfig(t *testing.T) {
	// Scenario when a node is in nodesext but not nodes
	cfg := getConfig(t, "testdata/map_node_present_nodesext_missing_nodes.json")

	if len(cfg.kvServerList) != 3 {
		t.Fatalf("Expected 3 kv nodes, got %d", len(cfg.kvServerList))
	}
}
