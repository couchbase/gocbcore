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

// SaslAuthPlain performs PLAIN SASL authentication against an AuthClient.
func SaslAuthPlain(username, password string, client AuthClient, deadline time.Time) error {
	// Build PLAIN auth data
	userBuf := []byte(username)
	passBuf := []byte(password)
	authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
	authData[0] = 0
	copy(authData[1:], userBuf)
	authData[1+len(userBuf)] = 0
	copy(authData[1+len(userBuf)+1:], passBuf)

	// Execute PLAIN authentication
	_, err := client.ExecSaslAuth([]byte("PLAIN"), authData, deadline)

	return err
}

// SaslAuthBest performs SASL authentication against an AuthClient using the
// best supported authentication algorithm available on both client and server.
func SaslAuthBest(username, password string, client AuthClient, deadline time.Time) error {
	methods, err := client.ExecSaslListMechs(deadline)
	if err != nil {
		return err
	}

	logDebugf("Server SASL supports: %v", methods)

	var bestMethod string
	var bestPriority int
	for _, method := range methods {
		if bestPriority <= 1 && method == "PLAIN" {
			bestPriority = 1
			bestMethod = method
		}

		// CRAM-MD5 is intentionally disabled here as it provides a false
		// sense of security to users.  SCRAM-SHA1 should be used, or TLS
		// connection coupled with PLAIN auth would also be sufficient.
		/*
			if bestPriority <= 2 && method == "CRAM-MD5" {
				bestPriority = 2
				bestMethod = method
			}
		*/

		// TODO: Support SCRAM-SHA Methods

		/*
			if bestPriority <= 3 && method == "SCRAM-SHA1" {
				bestPriority = 3
				bestMethod = method
			}
		*/

		/*
			if bestPriority <= 4 && method == "SCRAM-SHA256" {
				bestPriority = 4
				bestMethod = method
			}
		*/

		/*
			if bestPriority <= 5 && method == "SCRAM-SHA512" {
				bestPriority = 5
				bestMethod = method
			}
		*/
	}

	logDebugf("Selected `%s` for SASL auth", bestMethod)

	if bestMethod == "PLAIN" {
		return SaslAuthPlain(username, password, client, deadline)
	}

	return ErrNoAuthMethod
}
