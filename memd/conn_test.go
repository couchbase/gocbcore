package memd

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func testPktRoundTrip(t *testing.T, pkt *Packet, features []HelloFeature) {
	t.Helper()

	// Create a buffer and connection for testing
	buf := &bytes.Buffer{}
	conn := NewConn(buf)

	// Enable the specific features
	for _, feature := range features {
		conn.EnableFeature(feature)
	}

	// Write our packet to the connection
	err := conn.WritePacket(pkt)
	if err != nil {
		t.Fatalf("packet writing failed: %s", err)
	}

	// Read the packet back
	pktOut, _, err := conn.ReadPacket()
	if err != nil {
		t.Fatalf("packet reading failed: %s", err)
	}

	// Check that the packet matched like we expect
	if !reflect.DeepEqual(pkt, pktOut) {
		t.Errorf("packets did not match after roundtrip\n"+
			"EXP: %+v\nGOT: %+v",
			pkt, pktOut)
		t.Logf("EXP DURLVL: %+v\nGOT DURLVL: %+v", pkt.DurabilityLevelFrame, pktOut.DurabilityLevelFrame)
		t.Logf("EXP DURATM: %+v\nGOT DURATM: %+v", pkt.DurabilityTimeoutFrame, pktOut.DurabilityTimeoutFrame)
		t.Logf("EXP STRMID: %+v\nGOT STRMID: %+v", pkt.StreamIDFrame, pktOut.StreamIDFrame)
		t.Logf("EXP OTRCTX: %+v\nGOT OTRCTX: %+v", pkt.OpenTracingFrame, pktOut.OpenTracingFrame)
		t.Logf("EXP SRVDUR: %+v\nGOT SRVDUR: %+v", pkt.ServerDurationFrame, pktOut.ServerDurationFrame)
		t.FailNow()
	}
}

var noFeatures = []HelloFeature{}

var allFeatures = []HelloFeature{
	FeatureDatatype,
	FeatureTLS,
	FeatureTCPNoDelay,
	FeatureSeqNo,
	FeatureTCPDelay,
	FeatureXattr,
	FeatureXerror,
	FeatureSelectBucket,
	FeatureSnappy,
	FeatureJSON,
	FeatureDuplex,
	FeatureClusterMapNotif,
	FeatureUnorderedExec,
	FeatureDurations,
	FeatureAltRequests,
	FeatureSyncReplication,
	FeatureCollections,
	FeatureOpenTracing,
}

func TestPktRtBasicReq(t *testing.T) {
	testPktRoundTrip(t, &Packet{
		Magic:    CmdMagicReq,
		Command:  CmdGetErrorMap,
		Datatype: 0x22,
		Vbucket:  0x9f9e,
		Opaque:   0x87654321,
		Cas:      0x7654321076543210,
		Key:      []byte("Hello"),
		Extras:   []byte("I am some data which is longer?"),
		Value:    []byte("World"),
	}, noFeatures)
}

func TestPktRtBasicRes(t *testing.T) {
	testPktRoundTrip(t, &Packet{
		Magic:    CmdMagicRes,
		Command:  CmdGetErrorMap,
		Datatype: 0x22,
		Status:   StatusBusy,
		Opaque:   0x87654321,
		Cas:      0x7654321076543210,
		Key:      []byte("Hello"),
		Extras:   []byte("I am some data which is longer?"),
		Value:    []byte("World"),
	}, noFeatures)
}

func TestPktRtBasicReqExt(t *testing.T) {
	testPktRoundTrip(t, &Packet{
		Magic:        CmdMagicReq,
		Command:      CmdGAT,
		Datatype:     0x22,
		Vbucket:      0x9f9e,
		Opaque:       0x87654321,
		Cas:          0x7654321076543210,
		CollectionID: 99,
		Key:          []byte("Hello"),
		Extras:       []byte("I am some data which is longer?"),
		Value:        []byte("World"),
		BarrierFrame: &BarrierFrame{},
		DurabilityLevelFrame: &DurabilityLevelFrame{
			DurabilityLevel: DurabilityLevelPersistToMajority,
		},
		DurabilityTimeoutFrame: &DurabilityTimeoutFrame{
			DurabilityTimeout: 2 * time.Second,
		},
		StreamIDFrame: &StreamIDFrame{
			StreamID: 0xe1f8,
		},
		OpenTracingFrame: &OpenTracingFrame{
			TraceContext: []byte("This is some data longer than 15bytes"),
		},
	}, allFeatures)
}

func TestPktRtBasicResExt(t *testing.T) {
	testPktRoundTrip(t, &Packet{
		Magic:        CmdMagicRes,
		Command:      CmdGAT,
		Datatype:     0x22,
		Status:       StatusBusy,
		Opaque:       0x87654321,
		Cas:          0x7654321076543210,
		CollectionID: 99,
		Key:          []byte("Hello"),
		Extras:       []byte("I am some data which is longer?"),
		Value:        []byte("World"),
		ServerDurationFrame: &ServerDurationFrame{
			ServerDuration: 119973 * time.Microsecond,
		},
	}, allFeatures)
}
