// Package security provides HMAC-based worker authentication and TLS helpers.
package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"
)

// Token auth
// TokenSigner issues and verifies HMAC-SHA256 signed tokens for worker auth.
type TokenSigner struct {
	secret []byte
}

func NewTokenSigner(secret string) *TokenSigner {
	return &TokenSigner{secret: []byte(secret)}
}

// Issue creates a signed token for the given workerID valid for `ttl`.
// Format: "<workerID>.<expireUnix>.<signature>"
func (s *TokenSigner) Issue(workerID string, ttl time.Duration) string {
	expire := time.Now().Add(ttl).Unix()
	payload := fmt.Sprintf("%s.%d", workerID, expire)
	sig := s.sign(payload)
	return fmt.Sprintf("%s.%s", payload, sig)
}

// Verify checks that the token is valid and not expired.
// Returns the worker ID encoded in the token.
func (s *TokenSigner) Verify(token string) (string, error) {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return "", fmt.Errorf("malformed token")
	}
	workerID, expireStr, sig := parts[0], parts[1], parts[2]

	payload := workerID + "." + expireStr
	expected := s.sign(payload)
	if !hmac.Equal([]byte(sig), []byte(expected)) {
		return "", fmt.Errorf("invalid token signature")
	}

	var expireUnix int64
	if _, err := fmt.Sscanf(expireStr, "%d", &expireUnix); err != nil {
		return "", fmt.Errorf("bad token expiry")
	}
	if time.Now().Unix() > expireUnix {
		return "", fmt.Errorf("token expired")
	}
	return workerID, nil
}

func (s *TokenSigner) sign(payload string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// GenerateSecret creates a cryptographically random hex secret.
func GenerateSecret(bytes int) (string, error) {
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// TLS helpers
// LoadServerTLS loads a server TLS config with optional mutual TLS.
// If caFile is non-empty, client certificates are required and verified.
func LoadServerTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server cert/key: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
	}
	return cfg, nil
}

// LoadClientTLS loads a client TLS config.
// If certFile/keyFile are non-empty, a client certificate is used (mTLS).
func LoadClientTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		cfg.RootCAs = pool
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
