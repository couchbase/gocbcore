package gocbcore

import (
	"crypto/tls"
	"fmt"
)

// UserPassPair represents a username and password pair.
type UserPassPair struct {
	Username string
	Password string
}

// AuthCredsRequest represents an authentication details request from the agent.
type AuthCredsRequest struct {
	Service  ServiceType
	Endpoint string
}

// AuthCertRequest represents a certificate details request from the agent.
type AuthCertRequest struct {
	Service  ServiceType
	Endpoint string
}

// AuthProvider is an interface to allow the agent to fetch authentication
// credentials on-demand from the application.
type AuthProvider interface {
	SupportsTLS() bool
	SupportsNonTLS() bool
	Certificate(req AuthCertRequest) (*tls.Certificate, error)
	Credentials(req AuthCredsRequest) ([]UserPassPair, error)
}

// UNCOMMITTED: This API may change in the future.
type JWT = string

// UNCOMMITTED: This API may change in the future.
type AuthProviderJWT interface {
	AuthMechanismProvider
	JWT(req AuthCredsRequest) (JWT, error)

	// ContainsJWT allows Authenticator implementations to implement AuthProviderJWT whilst not actually
	// providing JWTs. This is primarily useful for composite authenticators that are used for multiple auth
	// methods.
	ContainsJWT() bool
}

type AuthMechanismProvider interface {
	DefaultAuthMechanisms(tlsEnabled bool) []AuthMechanism
}

type authCreds struct {
	UserPass UserPassPair
	JWT      JWT
}

func (k *authCreds) ContainsCreds() bool {
	return k.IsJWT() || k.IsUserPass()
}

func (k *authCreds) IsJWT() bool {
	return len(k.JWT) > 0
}

func (k *authCreds) IsUserPass() bool {
	return len(k.UserPass.Username) > 0 && len(k.UserPass.Password) > 0
}

func getKvAuthCreds(auth AuthProvider, endpoint string) (*authCreds, error) {
	if a, ok := auth.(AuthProviderJWT); ok {
		if a.ContainsJWT() {
			jwt, err := a.JWT(AuthCredsRequest{
				Service:  MemdService,
				Endpoint: endpoint,
			})
			if err != nil {
				return nil, err
			}

			if len(jwt) == 0 {
				return nil, errInvalidCredentials
			}

			return &authCreds{
				JWT: jwt,
			}, nil
		}
	}

	creds, err := auth.Credentials(AuthCredsRequest{
		Service:  MemdService,
		Endpoint: endpoint,
	})
	if err != nil {
		return nil, err
	}

	if len(creds) != 1 {
		return nil, errInvalidCredentials
	}

	return &authCreds{
		UserPass: creds[0],
	}, nil
}

// PasswordAuthProvider provides a standard AuthProvider implementation
// for use with a standard username/password pair (for example, RBAC).
type PasswordAuthProvider struct {
	Username string
	Password string
}

// SupportsNonTLS specifies whether this authenticator supports non-TLS connections.
func (auth PasswordAuthProvider) SupportsNonTLS() bool {
	return true
}

// SupportsTLS specifies whether this authenticator supports TLS connections.
func (auth PasswordAuthProvider) SupportsTLS() bool {
	return true
}

// Certificate directly returns a certificate chain to present for the connection.
func (auth PasswordAuthProvider) Certificate(req AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

// Credentials directly returns the username/password from the provider.
func (auth PasswordAuthProvider) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return []UserPassPair{{
		Username: auth.Username,
		Password: auth.Password,
	}}, nil
}

func (auth PasswordAuthProvider) String() string {
	return fmt.Sprintf("%p", &auth)
}

func (auth PasswordAuthProvider) DefaultAuthMechanisms(tlsEnabled bool) []AuthMechanism {
	if tlsEnabled {
		return []AuthMechanism{ScramSha512AuthMechanism, ScramSha256AuthMechanism, ScramSha1AuthMechanism}
	}

	return []AuthMechanism{PlainAuthMechanism}
}

// UNCOMMITTED: This API may change in the future.
type JWTAuthProvider struct {
	Token string
}

func (j JWTAuthProvider) SupportsTLS() bool {
	return true
}

func (j JWTAuthProvider) SupportsNonTLS() bool {
	return false
}

func (j JWTAuthProvider) Certificate(req AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

func (j JWTAuthProvider) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return nil, nil
}

func (j JWTAuthProvider) JWT(req AuthCredsRequest) (string, error) {
	return j.Token, nil
}

func (j JWTAuthProvider) ContainsJWT() bool {
	return true
}

func (j JWTAuthProvider) DefaultAuthMechanisms(_tlsEnabled bool) []AuthMechanism {
	return []AuthMechanism{OAuthBearerAuthMechanism}
}
