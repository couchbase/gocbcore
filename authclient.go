package gocbcore

import (
	"time"
)

// TODO(brett19): Remove the Exec keyword from AuthClient

// AuthClient exposes an interface for performing authentication on a
// connected Couchbase K/V client.
type AuthClient interface {
	Address() string

	ExecSaslListMechs(deadline time.Time) ([]string, error)
	ExecSaslAuth(k, v []byte, deadline time.Time) ([]byte, error)
	ExecSaslStep(k, v []byte, deadline time.Time) ([]byte, error)
	ExecSelectBucket(b []byte, deadline time.Time) error
}
