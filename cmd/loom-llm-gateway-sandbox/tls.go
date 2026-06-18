// TLS server config. Loaded from disk at startup; rotation is via
// SIGTERM + restart (the worker manages the singleton's lifecycle
// — PR-B2 — and cycles it when the cert changes).

package main

import (
	"crypto/tls"
	"fmt"
)

// loadServerTLS reads the cert + key files and returns a tls.Config
// suitable for http.Server. Pinned to TLS 1.2+ with the
// modern-compatibility cipher suite list — anything weaker is a
// regulatory finding waiting to happen.
//
// Cipher list mirrors the Mozilla "intermediate" profile circa
// 2024; deliberately conservative because the egress chain handles
// every team's traffic and a cipher downgrade is non-localized
// blast radius.
func loadServerTLS(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load keypair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		// Go's default cipher list is fine for TLS 1.3 (the suite
		// selection is hard-coded). For 1.2 we restrict to AEAD
		// ciphers — no CBC, no SHA-1.
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
		PreferServerCipherSuites: true,
	}, nil
}
