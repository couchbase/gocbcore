package memd

import (
	"strings"
	"testing"
	"time"
)

func TestPacketString(t *testing.T) {
	pak := &Packet{
		Magic:        CmdMagicReq,
		Command:      CmdGetErrorMap,
		Datatype:     0x22,
		Vbucket:      0x9f9e,
		Opaque:       0x87654321,
		Cas:          0x7654321076543210,
		CollectionID: 0x08,
		Key:          []byte("Hello"),
		Extras:       []byte("I am some data which is longer?"),
		Value:        []byte("World"),
	}

	toStr := pak.String()
	expected := "memd.Packet{Magic:0x80(CmdMagicReq), Command:0xfe(CMD_GETERRORMAP), Datatype:0x22, Status:0x0000(success), " +
		"Vbucket:40862(0x9f9e), Opaque:0x87654321, Cas: 0x7654321076543210, CollectionID:8(0x00000008), Barrier:false" +
		"\nKey:\n" +
		"   0  48 65 6C 6C 6F                                    Hello           " +
		"\nValue:\n" +
		"   0  57 6F 72 6C 64                                    World           " +
		"\nExtras:\n" +
		"   0  49 20 61 6D 20 73 6F 6D  65 20 64 61 74 61 20 77  I am some data w\n" +
		"  16  68 69 63 68 20 69 73 20  6C 6F 6E 67 65 72 3F     hich is longer? \n" +
		"}"
	if !strings.EqualFold(expected, toStr) {
		t.Fatalf("Expected packet string value of \n%v\n did not match actual \n%v", expected, toStr)
	}
}

func TestPacketStringFramingExtras(t *testing.T) {
	pak := &Packet{
		Magic:        CmdMagicReq,
		Command:      CmdGetErrorMap,
		Datatype:     0x22,
		Vbucket:      0x9f9e,
		Opaque:       0x87654321,
		Cas:          0x7654321076543210,
		CollectionID: 0x08,
		Key:          []byte("Hello"),
		Value:        []byte("World"),
		DurabilityLevelFrame: &DurabilityLevelFrame{
			DurabilityLevel: DurabilityLevelPersistToMajority,
		},
		DurabilityTimeoutFrame: &DurabilityTimeoutFrame{
			DurabilityTimeout: 10 * time.Second,
		},
		StreamIDFrame: &StreamIDFrame{
			StreamID: 0x05,
		},
		OpenTracingFrame: &OpenTracingFrame{
			TraceContext: []byte("This is some data longer than 15bytes"),
		},
		ServerDurationFrame: &ServerDurationFrame{
			ServerDuration: 1 * time.Millisecond,
		},
		UserImpersonationFrame: &UserImpersonationFrame{
			User: []byte("system"),
		},
		PreserveExpiryFrame: &PreserveExpiryFrame{},
	}

	toStr := pak.String()
	expected := "memd.Packet{Magic:0x80(CmdMagicReq), Command:0xfe(CMD_GETERRORMAP), Datatype:0x22, Status:0x0000(success), " +
		"Vbucket:40862(0x9f9e), Opaque:0x87654321, Cas: 0x7654321076543210, CollectionID:8(0x00000008), Barrier:false" +
		"\nKey:\n" +
		"   0  48 65 6C 6C 6F                                    Hello           " +
		"\nValue:\n" +
		"   0  57 6F 72 6C 64                                    World           " +
		"\nExtras:\n" +
		"\nDurability Level: 0x03" +
		"\nDurability Level Timeout: 10s" +
		"\nStreamID: 0x05" +
		"\nTrace Context:\n" +
		"   0  54 68 69 73 20 69 73 20  73 6F 6D 65 20 64 61 74  This is some dat\n" +
		"  16  61 20 6C 6F 6E 67 65 72  20 74 68 61 6E 20 31 35  a longer than 15\n" +
		"  32  62 79 74 65 73                                    bytes           \n" +
		"\nServer Duration: 1ms" +
		"\nUser: system" +
		"\nPreserve Expiry: true" +
		"}"

	if !strings.EqualFold(expected, toStr) {
		t.Fatalf("Expected packet string value of \n%s\n did not match actual \n%s", expected, toStr)
	}
}
