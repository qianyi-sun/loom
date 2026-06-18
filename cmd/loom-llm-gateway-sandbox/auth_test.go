// Step-JWT verification tests. Mirrors loom.auth.verify_step_jwt's
// Python tests at tests/unit/test_auth.py; the two MUST agree on
// wire format. If you change one, change the other.

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var testSigningKey = []byte("test-signing-key-do-not-use-in-prod")

func mintTestJWT(t *testing.T, mutate func(c jwt.MapClaims)) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss":      "loom-control-plane",
		"sub":      "step-session",
		"team_id":  "00000000-0000-0000-0000-000000000001",
		"trial_id": "00000000-0000-0000-0000-000000000002",
		"step_id":  "main",
		"exp":      time.Now().Add(5 * time.Minute).Unix(),
		"iat":      time.Now().Unix(),
		"scopes":   []string{"llm:call"},
	}
	if mutate != nil {
		mutate(claims)
	}
	body, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).
		SignedString(testSigningKey)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return stepJWTPrefix + body
}

func TestVerifyStepJWT_HappyPath(t *testing.T) {
	tok := mintTestJWT(t, nil)
	claims, err := verifyStepJWT(tok, testSigningKey)
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if claims.TeamID != "00000000-0000-0000-0000-000000000001" {
		t.Errorf("team_id wrong: %q", claims.TeamID)
	}
	if claims.TrialID != "00000000-0000-0000-0000-000000000002" {
		t.Errorf("trial_id wrong: %q", claims.TrialID)
	}
	if claims.StepID != "main" {
		t.Errorf("step_id wrong: %q", claims.StepID)
	}
}

func TestVerifyStepJWT_WithProviderConnectionID(t *testing.T) {
	tok := mintTestJWT(t, func(c jwt.MapClaims) {
		c["provider_connection_id"] = "00000000-0000-0000-0000-000000000099"
	})
	claims, err := verifyStepJWT(tok, testSigningKey)
	if err != nil {
		t.Fatalf("ok: %v", err)
	}
	if claims.ProviderConnectionID != "00000000-0000-0000-0000-000000000099" {
		t.Errorf("provider_connection_id wrong: %q", claims.ProviderConnectionID)
	}
}

func TestVerifyStepJWT_MissingPrefix(t *testing.T) {
	tok := mintTestJWT(t, nil)
	stripped := strings.TrimPrefix(tok, stepJWTPrefix)
	_, err := verifyStepJWT(stripped, testSigningKey)
	if err == nil {
		t.Fatal("expected rejection of token without loom_step_ prefix")
	}
	if !strings.Contains(err.Error(), "prefix") {
		t.Errorf("error should mention prefix: %v", err)
	}
}

func TestVerifyStepJWT_Expired(t *testing.T) {
	tok := mintTestJWT(t, func(c jwt.MapClaims) {
		c["exp"] = time.Now().Add(-1 * time.Minute).Unix()
	})
	_, err := verifyStepJWT(tok, testSigningKey)
	if err == nil {
		t.Fatal("expected expired rejection")
	}
}

func TestVerifyStepJWT_WrongSignature(t *testing.T) {
	tok := mintTestJWT(t, nil)
	_, err := verifyStepJWT(tok, []byte("wrong-key"))
	if err == nil {
		t.Fatal("expected signature-mismatch rejection")
	}
}

func TestVerifyStepJWT_RejectsAlgNone(t *testing.T) {
	// Classic JWT confusion: token claims `alg: none` to bypass
	// signature verification. The verifier MUST reject.
	claims := jwt.MapClaims{
		"iss":      "loom-control-plane",
		"team_id":  "x",
		"trial_id": "y",
		"step_id":  "main",
		"exp":      time.Now().Add(5 * time.Minute).Unix(),
	}
	body, err := jwt.NewWithClaims(jwt.SigningMethodNone, claims).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none: %v", err)
	}
	tok := stepJWTPrefix + body
	_, err = verifyStepJWT(tok, testSigningKey)
	if err == nil {
		t.Fatal("expected alg=none rejection")
	}
}

func TestVerifyStepJWT_MissingRequiredClaim(t *testing.T) {
	tok := mintTestJWT(t, func(c jwt.MapClaims) {
		delete(c, "team_id")
	})
	_, err := verifyStepJWT(tok, testSigningKey)
	if err == nil {
		t.Fatal("expected missing-claim rejection")
	}
	if !strings.Contains(err.Error(), "team_id") {
		t.Errorf("error should mention team_id: %v", err)
	}
}

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name string
		auth string
		api  string
		want string
	}{
		{"authorization bearer", "Bearer abc", "", "abc"},
		{"authorization with extra spaces", "  Bearer  xyz", "", " xyz"},
		{"x-api-key fallback", "", "loom_step_xyz", "loom_step_xyz"},
		{"authorization preferred over x-api-key", "Bearer A", "B", "A"},
		{"neither", "", "", ""},
		{"authorization without Bearer prefix", "raw", "", "raw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractBearerToken(tt.auth, tt.api)
			if got != tt.want {
				t.Errorf("got %q want %q", got, tt.want)
			}
		})
	}
}
