package memd

import (
	"testing"
	"time"
)

func testSrvDura16(t *testing.T, n time.Duration, e uint16) {
	t.Helper()

	x := EncodeSrvDura16(n)
	if x != e {
		t.Fatalf("encoding failed %d != %d", x, e)
	}

	y := DecodeSrvDura16(e)
	if y != n {
		t.Fatalf("decoding failed %d != %d", y, n)
	}
}

func TestSrvDura16(t *testing.T) {
	// Note that these values are specifically selected as they are
	// known to be encoded exactly with eachother (which is what
	// we do our testing to)
	testSrvDura16(t, 0*time.Microsecond, 0)
	testSrvDura16(t, 1331*time.Microsecond, 93)
	testSrvDura16(t, 20841*time.Microsecond, 452)
	testSrvDura16(t, 119973*time.Microsecond, 1236)
}
