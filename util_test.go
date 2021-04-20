package gocbcore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrimSchemePrefix(t *testing.T) {
	type test struct {
		name, address string
	}

	tests := []*test{
		{
			name:    "None",
			address: "hostname:8091",
		},
		{
			name:    "HTTP",
			address: "http://hostname:8091",
		},
		{
			name:    "HTTPS",
			address: "https://hostname:8091",
		},
		{
			name:    "Couchbase",
			address: "couchbase://hostname:8091",
		},
		{
			name:    "Couchbases",
			address: "couchbases://hostname:8091",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, "hostname:8091", trimSchemePrefix(test.address))
		})
	}
}
