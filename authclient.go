package gocbcore

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"time"
)

// AuthMechanism represents a type of auth that can be performed.
type AuthMechanism string

const (
	// PlainAuthMechanism represents that PLAIN auth should be performed.
	PlainAuthMechanism = AuthMechanism("PLAIN")

	// ScramSha1AuthMechanism represents that SCRAM SHA1 auth should be performed.
	ScramSha1AuthMechanism = AuthMechanism("SCRAM_SHA1")

	// ScramSha256AuthMechanism represents that SCRAM SHA256 auth should be performed.
	ScramSha256AuthMechanism = AuthMechanism("SCRAM_SHA256")

	// ScramSha512AuthMechanism represents that SCRAM SHA512 auth should be performed.
	ScramSha512AuthMechanism = AuthMechanism("SCRAM_SHA512")
)

// AuthClient exposes an interface for performing authentication on a
// connected Couchbase K/V client.
type AuthClient interface {
	Address() string
	SupportsFeature(feature HelloFeature) bool

	SaslListMechs(deadline time.Time) (chan SaslListMechsCompleted, error)
	SaslAuth(k, v []byte, deadline time.Time) (chan BytesAndError, error)
	SaslStep(k, v []byte, deadline time.Time) (chan BytesAndError, error)
}

// SaslListMechsCompleted is used to contain the result and/or error from a SaslListMechs operation.
type SaslListMechsCompleted struct {
	Err   error
	Mechs []AuthMechanism
}

// SaslAuthPlain performs PLAIN SASL authentication against an AuthClient.
func SaslAuthPlain(username, password string, client AuthClient, deadline time.Time) (chan BytesAndError, error) {
	// Build PLAIN auth data
	userBuf := []byte(username)
	passBuf := []byte(password)
	authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
	authData[0] = 0
	copy(authData[1:], userBuf)
	authData[1+len(userBuf)] = 0
	copy(authData[1+len(userBuf)+1:], passBuf)

	// Execute PLAIN authentication
	completedCh, err := client.SaslAuth([]byte(PlainAuthMechanism), authData, deadline)
	if err != nil {
		return nil, err
	}

	return completedCh, nil
}

func saslAuthScram(saslName []byte, newHash func() hash.Hash, username, password string, client AuthClient,
	deadline time.Time) (chan BytesAndError, chan bool, error) {
	scramMgr := newScramClient(newHash, username, password)

	// Perform the initial SASL step
	completedCh := make(chan BytesAndError, 1)
	continueCh := make(chan bool, 1)
	scramMgr.Step(nil)
	saslCh, err := client.SaslAuth(saslName, scramMgr.Out(), deadline)
	if err != nil {
		return nil, nil, err
	}

	go func() {
		authResult := <-saslCh
		if authResult.Err != nil && !IsErrorStatus(authResult.Err, StatusAuthContinue) {
			continueCh <- false
			completedCh <- BytesAndError{
				Err: authResult.Err,
			}
			return
		}

		if !scramMgr.Step(authResult.Bytes) {
			err = scramMgr.Err()
			if err != nil {
				continueCh <- false
				completedCh <- BytesAndError{
					Err: err,
				}
				return
			}

			logErrorf("Local auth client finished before server accepted auth")
			continueCh <- false
			completedCh <- BytesAndError{}
			return
		}

		stepCh, err := client.SaslStep(saslName, scramMgr.Out(), deadline)
		if err != nil {
			continueCh <- false
			completedCh <- BytesAndError{
				Err: err,
			}
			return
		}

		continueCh <- true

		stepResult := <-stepCh
		if stepResult.Err != nil {
			completedCh <- BytesAndError{
				Err: stepResult.Err,
			}
			return
		}

		completedCh <- BytesAndError{
			Bytes: stepResult.Bytes,
		}
	}()

	return completedCh, continueCh, nil
}

// SaslAuthScramSha1 performs SCRAM-SHA1 SASL authentication against an AuthClient.
func SaslAuthScramSha1(username, password string, client AuthClient, deadline time.Time) (completedCh chan BytesAndError,
	continueCh chan bool, err error) {
	return saslAuthScram([]byte("SCRAM-SHA1"), sha1.New, username, password, client, deadline)
}

// SaslAuthScramSha256 performs SCRAM-SHA256 SASL authentication against an AuthClient.
func SaslAuthScramSha256(username, password string, client AuthClient, deadline time.Time) (completedCh chan BytesAndError,
	continueCh chan bool, err error) {
	return saslAuthScram([]byte("SCRAM-SHA256"), sha256.New, username, password, client, deadline)
}

// SaslAuthScramSha512 performs SCRAM-SHA512 SASL authentication against an AuthClient.
func SaslAuthScramSha512(username, password string, client AuthClient, deadline time.Time) (completedCh chan BytesAndError,
	continueCh chan bool, err error) {
	return saslAuthScram([]byte("SCRAM-SHA512"), sha512.New, username, password, client, deadline)
}

func saslMethod(method AuthMechanism, username, password string, client AuthClient, deadline time.Time) (completedCh chan BytesAndError,
	continueCh chan bool, err error) {
	switch method {
	case PlainAuthMechanism:
		ch, err := SaslAuthPlain(username, password, client, deadline)
		if err != nil {
			return nil, nil, err
		}
		return ch, nil, nil
	case ScramSha1AuthMechanism:
		return SaslAuthScramSha1(username, password, client, deadline)
	case ScramSha256AuthMechanism:
		return SaslAuthScramSha256(username, password, client, deadline)
	case ScramSha512AuthMechanism:
		return SaslAuthScramSha512(username, password, client, deadline)
	default:
		return nil, nil, ErrNoAuthMethod
	}
}

func saslBestMethod(methods []AuthMechanism) AuthMechanism {
	var bestMethod AuthMechanism
	var bestPriority int
	for _, method := range methods {
		if bestPriority <= 1 && method == PlainAuthMechanism {
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

		if bestPriority <= 3 && method == ScramSha1AuthMechanism {
			bestPriority = 3
			bestMethod = method
		}

		if bestPriority <= 4 && method == ScramSha256AuthMechanism {
			bestPriority = 4
			bestMethod = method
		}

		if bestPriority <= 5 && method == ScramSha512AuthMechanism {
			bestPriority = 5
			bestMethod = method
		}
	}

	return bestMethod
}
