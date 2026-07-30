// Package token provides cryptographically secure random token generation
// using crypto/rand. It is used to generate one-time registration tokens
// and session tokens for the forgery proxy.
//
// Registration tokens: 32 bytes → 64 hex chars (search space 2^256)
// Session tokens: 16 bytes → 32 hex chars (short-lived, lower entropy acceptable)
package token

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// Generate returns 32 bytes of crypto/rand as a hex-encoded string (64 hex characters).
// It is used to generate one-time registration tokens with a search space of 2^256.
//
// NEVER uses math/rand — only crypto/rand.Read.
func Generate() (string, error) {
	raw, err := GenerateBytes(32)
	if err != nil {
		return "", fmt.Errorf("token.Generate: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// GenerateBytes returns n bytes of cryptographically secure random data.
// It is primarily used for generating session tokens (typically GenerateBytes(16)
// which yields 32 hex characters when encoded).
//
// NEVER uses math/rand — only crypto/rand.Read.
func GenerateBytes(n int) ([]byte, error) {
	if n <= 0 {
		return nil, fmt.Errorf("token.GenerateBytes: n must be > 0, got %d", n)
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("token.GenerateBytes: %w", err)
	}
	return b, nil
}
