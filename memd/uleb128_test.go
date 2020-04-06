package memd

import (
	"bytes"
	"testing"
)

func testULEB128_32(t *testing.T, v uint32, eb []byte) {
	t.Helper()

	buf := AppendULEB128_32(nil, v)
	bufLen := len(buf)

	if bytes.Compare(buf, eb) != 0 {
		t.Fatalf("failed to encode: %+v != %+v", buf, eb)
	}

	// add some garbage to the end for fun
	buf = append(buf, 0xFF, 0x88, 0x00)

	x, n, err := DecodeULEB128_32(buf)
	if err != nil {
		t.Fatalf("failed to decode: %s", err)
	}

	if n != bufLen {
		t.Fatalf("wrong number of decoded bytes")
	}

	if x != v {
		t.Fatalf("wrong decoded value: %d != %d", x, v)
	}
}

func TestULEB128_32_0x00000000(t *testing.T) {
	testULEB128_32(t, 0x00000000, []byte{0x00})
}

func TestULEB128_32_0x00000001(t *testing.T) {
	testULEB128_32(t, 0x00000001, []byte{0x01})
}

func TestULEB128_32_0x0000007F(t *testing.T) {
	testULEB128_32(t, 0x0000007F, []byte{0x7F})
}

func TestULEB128_32_0x00000080(t *testing.T) {
	testULEB128_32(t, 0x00000080, []byte{0x80, 0x01})
}

func TestULEB128_32_0x00000555(t *testing.T) {
	testULEB128_32(t, 0x00000555, []byte{0xD5, 0x0A})
}

func TestULEB128_32_0x00007FFF(t *testing.T) {
	testULEB128_32(t, 0x00007FFF, []byte{0xFF, 0xFF, 0x01})
}

func TestULEB128_32_0x0000BFFF(t *testing.T) {
	testULEB128_32(t, 0x0000BFFF, []byte{0xFF, 0xFF, 0x02})
}

func TestULEB128_32_0x0000FFFF(t *testing.T) {
	testULEB128_32(t, 0x0000FFFF, []byte{0xFF, 0xFF, 0x03})
}

func TestULEB128_32_0x00008000(t *testing.T) {
	testULEB128_32(t, 0x00008000, []byte{0x80, 0x80, 0x02})
}

func TestULEB128_32_0x00005555(t *testing.T) {
	testULEB128_32(t, 0x00005555, []byte{0xD5, 0xAA, 0x01})
}

func TestULEB128_32_0x0CAFEF00(t *testing.T) {
	testULEB128_32(t, 0x0CAFEF00, []byte{0x80, 0xDE, 0xBF, 0x65})
}

func TestULEB128_32_0xCAFEF00D(t *testing.T) {
	testULEB128_32(t, 0xCAFEF00D, []byte{0x8D, 0xE0, 0xFB, 0xD7, 0x0C})
}

func TestULEB128_32_0xffffffff(t *testing.T) {
	testULEB128_32(t, 0xffffffff, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F})
}

func TestULEB128_32_nil(t *testing.T) {
	_, _, err := DecodeULEB128_32([]byte{})
	if err == nil {
		t.Fatal("decoding should have failed but did not")
	}
}
