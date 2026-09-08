package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const uploadTestRepository = "loom-task-image-attempts/x86_64/22222222-2222-4222-8222-222222222222/task"

// Models Task 6 ownership: Next closes a predecessor only on successful renewal;
// Close releases the remaining credential. Tokens really alias locked memory.
type uploadTestSource struct {
	t                             *testing.T
	origin, repository, component string
	issued                        []*RegistryCredential
	closes                        map[*RegistryCredential]int
	rotate                        bool
	fail                          bool
	switchRepository              bool
	onUploadSucceeded             func(context.Context, UploadedManifest, *RegistryCredential) error
	uploadSucceededCalls          int
	uploadSucceededCredential     *RegistryCredential
	uploadSucceededLive           bool
	uploadSucceededManifest       UploadedManifest
	events                        []string
}

func (s *uploadTestSource) Next(ctx context.Context, prev *RegistryCredential) (*RegistryCredential, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.fail {
		return nil, errors.New("source error PRIVATE-TOKEN")
	}
	if prev != nil && (len(s.issued) == 0 || s.issued[len(s.issued)-1] != prev || prev.secret.closed) {
		s.t.Error("wrong predecessor")
	}
	token := []byte(fmt.Sprintf("token-%d.private.signature", len(s.issued)+1))
	fd := createMemfdFixture(s.t, "registry-upload-test", token, requiredMemfdSeals, true)
	secret, err := NewSecretBuffer(fd, 4096)
	if err != nil {
		s.t.Fatal(err)
	}
	lifetime := 40 * time.Second
	if s.rotate && prev == nil {
		lifetime = 16500 * time.Millisecond
	}
	repo := s.repository
	if repo == "" {
		repo = uploadTestRepository
	}
	component := s.component
	if component == "" {
		component = "task"
	}
	if s.switchRepository && prev != nil {
		repo = strings.Replace(repo, "/task", "/other", 1)
	}
	c := &RegistryCredential{secret: secret, BearerToken: secret.data, ID: fmt.Sprintf("credential-%d", len(s.issued)+1), Generation: len(s.issued) + 1, RegistryOrigin: s.origin, RegistryService: "test-registry", Repository: repo, Component: component, CPUArch: "x86_64", Platform: "linux/amd64", AttemptID: "22222222-2222-4222-8222-222222222222", ExpiresAt: time.Now().Add(lifetime)}
	if prev != nil {
		s.Close(prev)
	}
	s.issued = append(s.issued, c)
	s.events = append(s.events, "next:"+c.ID)
	return c, nil
}
func (s *uploadTestSource) Close(c *RegistryCredential) {
	if c == nil {
		return
	}
	if s.closes == nil {
		s.closes = map[*RegistryCredential]int{}
	}
	s.closes[c]++
	s.events = append(s.events, "close:"+c.ID)
	c.Close()
}
func (s *uploadTestSource) UploadSucceeded(ctx context.Context, manifest UploadedManifest, c *RegistryCredential) error {
	s.uploadSucceededCalls++
	s.uploadSucceededCredential = c
	s.uploadSucceededManifest = manifest
	s.uploadSucceededLive = c != nil && c.secret != nil && !c.secret.closed && len(c.BearerToken) > 0
	if c != nil {
		s.events = append(s.events, "callback:"+c.ID)
	}
	if s.onUploadSucceeded != nil {
		return s.onUploadSucceeded(ctx, manifest, c)
	}
	return nil
}
func (s *uploadTestSource) checkClosed() {
	s.t.Helper()
	for _, c := range s.issued {
		if s.closes[c] != 1 || !c.secret.closed || c.BearerToken != nil {
			s.t.Fatal("credential lifecycle mismatch")
		}
		for _, b := range c.secret.data {
			if b != 0 {
				s.t.Fatal("secret not zeroed")
			}
		}
	}
}

type uploadRoundTripFunc func(*http.Request) (*http.Response, error)

func (f uploadRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRegistryUploadRequestUsesOnlyPlaceholderAuthorizationHeader(t *testing.T) {
	source := &uploadTestSource{t: t, origin: "https://registry.test"}
	credential, err := source.Next(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close(credential)
	tokenText := "token-1.private.signature"
	if !bytes.Equal(credential.BearerToken, []byte(tokenText)) {
		t.Fatal("fixture token changed")
	}
	origin, err := url.Parse("https://registry.test")
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse("https://registry.test/v2/repository/tags/list")
	if err != nil {
		t.Fatal(err)
	}
	var captured *http.Request
	session := registryUploadSession{
		policy:     RegistryUploadPolicy{origin: *origin},
		credential: credential,
		client: &http.Client{Transport: uploadRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			captured = r
			auth := r.Header.Get("Authorization")
			if strings.Contains(auth, tokenText) {
				t.Fatalf("high-level request retained bearer token in Authorization header: %q", auth)
			}
			if auth != registryAuthorizationPlaceholder {
				t.Fatalf("Authorization header = %q, want fixed placeholder", auth)
			}
			return &http.Response{
				StatusCode:    http.StatusNoContent,
				ProtoMajor:    1,
				Header:        http.Header{"Content-Length": []string{"0"}},
				Body:          io.NopCloser(strings.NewReader("")),
				ContentLength: 0,
			}, nil
		})},
	}
	if _, err := session.request(context.Background(), "GET", target, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if captured == nil {
		t.Fatal("request was not sent")
	}
	if strings.Contains(captured.Header.Get("Authorization"), tokenText) {
		t.Fatal("bearer token retained after request")
	}
}

func TestRegistryAuthorizationConnInjectsBearerAcrossFragmentedHeaders(t *testing.T) {
	token := []byte("writer-token.private.signature")
	underlying := &uploadWriteOnlyConn{maxWrite: 3}
	conn := newRegistryAuthorizationConn(underlying, token)
	request := []byte("PUT /v2/repository/manifests/sha256:abc HTTP/1.1\r\nHost: registry.test\r\nAuthorization: " + registryAuthorizationPlaceholder + "\r\nContent-Type: application/vnd.oci.image.manifest.v1+json\r\n\r\n{}")
	for start := 0; start < len(request); {
		end := start + 7
		if end > len(request) {
			end = len(request)
		}
		n, err := conn.Write(request[start:end])
		if err != nil {
			t.Fatalf("fragment write error = %v", err)
		}
		if n != end-start {
			t.Fatalf("fragment write n = %d, want %d", n, end-start)
		}
		start = end
	}
	got := underlying.String()
	if !strings.Contains(got, "Authorization: Bearer writer-token.private.signature\r\n") {
		t.Fatalf("Authorization header was not injected from locked bytes:\n%s", got)
	}
	if strings.Contains(got, registryAuthorizationPlaceholder) {
		t.Fatal("placeholder reached the underlying connection")
	}
	if !strings.HasSuffix(got, "\r\n\r\n{}") {
		t.Fatalf("body was not forwarded after header injection:\n%s", got)
	}
	if conn.token != nil || len(conn.header) != 0 {
		t.Fatal("authorization writer retained secret/header scratch after success")
	}
}

func TestRegistryAuthorizationConnRejectsInvalidPlaceholder(t *testing.T) {
	validLine := "Authorization: " + registryAuthorizationPlaceholder + "\r\n"
	for _, tc := range []struct {
		name    string
		request []byte
	}{
		{name: "missing", request: []byte("GET /v2/ HTTP/1.1\r\nHost: registry.test\r\n\r\n")},
		{name: "duplicate", request: []byte("GET /v2/ HTTP/1.1\r\nHost: registry.test\r\n" + validLine + "X-Other: " + registryAuthorizationPlaceholder + "\r\n\r\n")},
		{name: "wrong header", request: []byte("GET /v2/ HTTP/1.1\r\nHost: registry.test\r\nX-Authorization: " + registryAuthorizationPlaceholder + "\r\n\r\n")},
		{name: "oversized", request: append([]byte("GET /v2/ HTTP/1.1\r\nHost: registry.test\r\nX-Fill: "), append(bytes.Repeat([]byte("a"), registryResponseBytes), []byte("\r\n"+validLine+"\r\n")...)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			underlying := &uploadWriteOnlyConn{}
			conn := newRegistryAuthorizationConn(underlying, []byte("writer-token.private.signature"))
			if _, err := conn.Write(tc.request); err == nil {
				t.Fatal("accepted invalid Authorization placeholder")
			} else if strings.Contains(err.Error(), "writer-token") {
				t.Fatal("writer error leaked bearer token")
			}
			if underlying.Len() != 0 {
				t.Fatal("invalid header was written to the underlying connection")
			}
			if conn.token != nil || len(conn.header) != 0 {
				t.Fatal("authorization writer retained scratch after rejection")
			}
		})
	}
}

func TestRegistryAuthorizationConnCleansUpAfterFailedOrAbortedWrite(t *testing.T) {
	t.Run("failed underlying write", func(t *testing.T) {
		underlying := &uploadWriteOnlyConn{maxWrite: 5, failAt: 80}
		conn := newRegistryAuthorizationConn(underlying, []byte("writer-token.private.signature"))
		request := []byte("PATCH /v2/repository/blobs/uploads/id HTTP/1.1\r\nHost: registry.test\r\nAuthorization: " + registryAuthorizationPlaceholder + "\r\n\r\npayload")
		if _, err := conn.Write(request); err == nil {
			t.Fatal("write succeeded after underlying failure")
		} else if strings.Contains(err.Error(), "writer-token") || strings.Contains(err.Error(), "PRIVATE-TOKEN") {
			t.Fatal("underlying write error leaked")
		}
		if conn.token != nil || len(conn.header) != 0 {
			t.Fatal("authorization writer retained scratch after failed write")
		}
	})
	t.Run("close before header complete", func(t *testing.T) {
		underlying := &uploadWriteOnlyConn{}
		conn := newRegistryAuthorizationConn(underlying, []byte("writer-token.private.signature"))
		if n, err := conn.Write([]byte("GET /v2/ HTTP/1.1\r\nAuthorization: ")); err != nil || n == 0 {
			t.Fatalf("partial header write = %d, %v", n, err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if conn.token != nil || len(conn.header) != 0 {
			t.Fatal("authorization writer retained scratch after close")
		}
	})
}

type uploadWriteOnlyConn struct {
	bytes.Buffer
	maxWrite int
	failAt   int
	closed   bool
}

func (c *uploadWriteOnlyConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *uploadWriteOnlyConn) Write(p []byte) (int, error) {
	if c.failAt > 0 && c.Buffer.Len() >= c.failAt {
		return 0, errors.New("underlying PRIVATE-TOKEN write failure")
	}
	limit := len(p)
	if c.maxWrite > 0 && limit > c.maxWrite {
		limit = c.maxWrite
	}
	if c.failAt > 0 && c.Buffer.Len()+limit > c.failAt {
		limit = c.failAt - c.Buffer.Len()
	}
	if limit > 0 {
		_, _ = c.Buffer.Write(p[:limit])
	}
	if c.failAt > 0 && c.Buffer.Len() >= c.failAt {
		return limit, errors.New("underlying PRIVATE-TOKEN write failure")
	}
	return limit, nil
}
func (c *uploadWriteOnlyConn) Close() error                     { c.closed = true; return nil }
func (c *uploadWriteOnlyConn) LocalAddr() net.Addr              { return uploadTestAddr("local") }
func (c *uploadWriteOnlyConn) RemoteAddr() net.Addr             { return uploadTestAddr("remote") }
func (c *uploadWriteOnlyConn) SetDeadline(time.Time) error      { return nil }
func (c *uploadWriteOnlyConn) SetReadDeadline(time.Time) error  { return nil }
func (c *uploadWriteOnlyConn) SetWriteDeadline(time.Time) error { return nil }

type uploadTestAddr string

func (a uploadTestAddr) Network() string { return string(a) }
func (a uploadTestAddr) String() string  { return string(a) }

func TestRegistryUploadSuccessCallbackUsesLiveManifestCredential(t *testing.T) {
	for _, tc := range []string{"success", "callback error", "manifest failure"} {
		t.Run(tc, func(t *testing.T) {
			out, f := newUploadFixture(t, 0)
			if tc == "manifest failure" {
				f.faultMethod = "PUT"
				f.fault = "wrong digest"
			}
			var first sync.Once
			s, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				f.ServeHTTP(w, r)
				first.Do(func() {
					time.Sleep(700 * time.Millisecond)
					f.mu.Lock()
					f.rotated = true
					f.mu.Unlock()
				})
			}))
			source := &uploadTestSource{t: t, origin: s.URL, rotate: true}
			if tc == "callback error" {
				source.onUploadSucceeded = func(context.Context, UploadedManifest, *RegistryCredential) error {
					return errors.New("PRIVATE-TOKEN callback failure")
				}
			}
			manifest, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source)
			source.checkClosed()
			if tc == "manifest failure" {
				if err == nil {
					t.Fatal("accepted failed manifest acknowledgement")
				}
				if source.uploadSucceededCalls != 0 {
					t.Fatal("success callback ran before exact manifest acknowledgement")
				}
				return
			}
			if tc == "callback error" {
				if err == nil {
					t.Fatal("upload succeeded after success callback failure")
				}
				if strings.Contains(err.Error(), "PRIVATE-TOKEN") {
					t.Fatal("callback error leaked")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if source.uploadSucceededCalls != 1 {
				t.Fatalf("success callback calls = %d, want 1", source.uploadSucceededCalls)
			}
			if len(source.issued) != 2 {
				t.Fatalf("issued credentials = %d, want manifest renewal", len(source.issued))
			}
			finalCredential := source.issued[1]
			if source.uploadSucceededCredential != finalCredential {
				t.Fatal("success callback did not receive credential used for manifest PUT")
			}
			if !source.uploadSucceededLive {
				t.Fatal("success callback credential was not live and owned")
			}
			want := UploadedManifest{Repository: uploadTestRepository, Digest: out.TopLevelDigest, MediaType: out.ManifestMediaType, Size: out.ManifestSize}
			if source.uploadSucceededManifest != want {
				t.Fatalf("callback manifest = %#v, want %#v", source.uploadSucceededManifest, want)
			}
			if tc == "success" && manifest != want {
				t.Fatalf("upload manifest = %#v, want %#v", manifest, want)
			}
			wantEvents := []string{"next:credential-1", "close:credential-1", "next:credential-2", "callback:credential-2", "close:credential-2"}
			if strings.Join(source.events, "|") != strings.Join(wantEvents, "|") {
				t.Fatalf("credential events = %v, want %v", source.events, wantEvents)
			}
		})
	}
}

type uploadRegistry struct {
	mu                                 sync.Mutex
	t                                  *testing.T
	repository                         string
	blobs                              map[string][]byte
	current                            []byte
	posts, patches, queries, manifests int
	patchSizes                         []int
	queryURL                           string
	lastUploadURL                      string
	manifest                           []byte
	manifestType                       string
	manifestDigest                     string
	present, ambiguous, rotated        bool
	ambiguousLarge                     bool
	uncertainPatchIndex                int
	faultMethod, fault                 string
}

func (f *uploadRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.TLS == nil || r.TLS.Version != tls.VersionTLS13 {
		f.t.Error("not TLS 1.3")
	}
	token := "Bearer token-1.private.signature"
	if f.rotated {
		token = "Bearer token-2.private.signature"
	}
	if r.Header.Get("Authorization") != token {
		f.t.Error("wrong bearer token")
		w.WriteHeader(401)
		return
	}
	prefix := "/v2/" + f.repository
	if !strings.HasPrefix(r.URL.Path, prefix+"/") {
		f.t.Error("cross component request")
		w.WriteHeader(403)
		return
	}
	if r.Method == f.faultMethod {
		if r.Method == "POST" {
			f.posts++
			f.current = nil
			f.lastUploadURL = prefix + "/blobs/uploads/id?state=opaque"
			w.Header().Set("Location", f.lastUploadURL)
			w.Header().Set("Range", "0-0")
		}
		switch f.fault {
		case "timeout":
			<-r.Context().Done()
			return
		case "redirect":
			w.Header().Set("Location", "https://example.invalid/stolen")
			w.WriteHeader(307)
			return
		case "body":
			w.WriteHeader(202)
			_, _ = w.Write(bytes.Repeat([]byte("x"), 65537))
			return
		case "headers":
			w.Header().Set("Server", strings.Repeat("x", 65537))
			w.WriteHeader(202)
			return
		case "multiple location":
			w.Header().Add("Location", prefix+"/blobs/uploads/id")
			w.Header().Add("Location", prefix+"/blobs/uploads/id")
			w.WriteHeader(202)
			return
		case "multiple range":
			w.Header().Set("Location", prefix+"/blobs/uploads/id")
			w.Header().Add("Range", "0-0")
			w.Header().Add("Range", "0-0")
			w.WriteHeader(202)
			return
		case "range", "overflow range", "negative range":
			v := "1-3"
			if f.fault == "overflow range" {
				v = "0-9223372036854775808"
			}
			if f.fault == "negative range" {
				v = "0--1"
			}
			w.Header().Set("Location", prefix+"/blobs/uploads/id")
			w.Header().Set("Range", v)
			w.WriteHeader(202)
			return
		case "cross origin", "cross repository", "escape", "encoded escape", "query escape", "userinfo", "fragment":
			loc := prefix + "/blobs/uploads/id"
			switch f.fault {
			case "cross origin":
				loc = "https://example.invalid" + loc
			case "cross repository":
				loc = "/v2/other/blobs/uploads/id"
			case "escape":
				loc = prefix + "/blobs/uploads/../../manifests/id"
			case "encoded escape":
				loc = prefix + "/blobs/uploads/%2e%2e/%2e%2e/manifests/id"
			case "query escape":
				loc += "?digest=sha256:evil"
			case "userinfo":
				loc = "https://user:pass@" + r.Host + loc
			case "fragment":
				loc += "#secret"
			}
			w.Header().Set("Location", loc)
			w.Header().Set("Range", "0-0")
			w.WriteHeader(202)
			return
		case "missing digest", "wrong digest", "multiple digest":
			if f.fault != "missing digest" {
				w.Header().Set("Docker-Content-Digest", "sha256:"+strings.Repeat("a", 64))
			}
			if f.fault == "multiple digest" {
				w.Header().Add("Docker-Content-Digest", f.manifestDigest)
			}
			w.WriteHeader(201)
			return
		default:
			code, err := strconv.Atoi(f.fault)
			if err == nil {
				w.WriteHeader(code)
				_, _ = io.WriteString(w, "PRIVATE-TOKEN response body")
				return
			}
		}
	}
	switch {
	case r.Method == "HEAD":
		digest := strings.TrimPrefix(r.URL.Path, prefix+"/blobs/")
		b, ok := f.blobs[digest]
		if !ok && !f.present {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.WriteHeader(200)
	case r.Method == "POST" && r.URL.Path == prefix+"/blobs/uploads/":
		f.posts++
		f.current = nil
		f.lastUploadURL = prefix + "/blobs/uploads/id?state=opaque"
		w.Header().Set("Location", f.lastUploadURL)
		w.Header().Set("Range", "0-0")
		w.WriteHeader(202)
	case r.Method == "PATCH":
		if r.URL.RequestURI() != f.lastUploadURL {
			f.t.Error("PATCH did not use exact location")
		}
		b, err := io.ReadAll(io.LimitReader(r.Body, (4<<20)+1))
		if err != nil {
			f.t.Error(err)
		}
		if len(b) > 4<<20 {
			f.t.Error("unbounded chunk")
		}
		expected := fmt.Sprintf("%d-%d", len(f.current), len(f.current)+len(b)-1)
		if r.Header.Get("Content-Range") != expected || r.Header.Get("Content-Type") != "application/octet-stream" {
			f.t.Error("wrong chunk metadata")
		}
		f.patches++
		f.patchSizes = append(f.patchSizes, len(b))
		if f.ambiguous && f.queries == 0 && (!f.ambiguousLarge || len(b) == 4<<20) {
			f.uncertainPatchIndex = len(f.patchSizes) - 1
			f.current = append(f.current, b[:len(b)/2]...)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				f.t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		f.current = append(f.current, b...)
		w.Header().Set("Location", f.lastUploadURL)
		w.Header().Set("Range", fmt.Sprintf("0-%d", len(f.current)-1))
		w.WriteHeader(202)
	case r.Method == "GET":
		f.queries++
		f.queryURL = r.URL.RequestURI()
		if f.queryURL != f.lastUploadURL {
			f.t.Error("query changed upload URL")
		}
		w.Header().Set("Location", f.lastUploadURL)
		w.Header().Set("Range", fmt.Sprintf("0-%d", len(f.current)-1))
		w.WriteHeader(204)
	case r.Method == "PUT" && strings.Contains(r.URL.Path, "/blobs/uploads/"):
		digest := r.URL.Query().Get("digest")
		if digest != "sha256:"+sha256Hex(f.current) {
			f.t.Error("blob bytes differ from local digest")
		}
		if r.URL.Query().Get("state") != "opaque" {
			f.t.Error("lost upload state")
		}
		f.blobs[digest] = append([]byte(nil), f.current...)
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Location", prefix+"/blobs/"+digest)
		w.WriteHeader(201)
	case r.Method == "PUT" && strings.Contains(r.URL.Path, "/manifests/"):
		f.manifests++
		b, err := io.ReadAll(io.LimitReader(r.Body, (4<<20)+1))
		if err != nil {
			f.t.Error(err)
		}
		if !bytes.Equal(b, f.manifest) || r.Header.Get("Content-Type") != f.manifestType || r.URL.Path != prefix+"/manifests/"+f.manifestDigest {
			f.t.Error("manifest bytes, type or digest path changed")
		}
		w.Header().Set("Docker-Content-Digest", f.manifestDigest)
		w.WriteHeader(201)
	default:
		f.t.Errorf("unexpected registry request %s", r.Method)
		w.WriteHeader(500)
	}
}

func newUploadFixture(t *testing.T, layerSize int) (OCIOutput, *uploadRegistry) {
	t.Helper()
	layer := make([]byte, layerSize)
	for i := range layer {
		layer[i] = byte(i*31 + 7)
	}
	p, _ := writeOCILayoutTar(t, "amd64", func(f *ociLayoutFixture) {
		if layerSize > 0 {
			f.manifest.Layers = []ociDescriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: "sha256:" + sha256Hex(layer), Size: int64(len(layer))}}
			f.extraEntries = append(f.extraEntries, tarEntry{name: "blobs/sha256/" + sha256Hex(layer), body: layer})
		}
	})
	out, err := ValidateOCIOutput(p, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	registry := &uploadRegistry{t: t, repository: uploadTestRepository, blobs: map[string][]byte{}, manifestType: "application/vnd.oci.image.manifest.v1+json", manifestDigest: out.TopLevelDigest}
	rewriteOCITar(t, p, func(name string, b []byte) []byte {
		if name == "blobs/sha256/"+strings.TrimPrefix(out.TopLevelDigest, "sha256:") {
			registry.manifest = append([]byte(nil), b...)
		}
		if strings.HasPrefix(name, "blobs/sha256/") {
			registry.blobs["sha256:"+strings.TrimPrefix(name, "blobs/sha256/")] = append([]byte(nil), b...)
		}
		return b
	})
	return out, registry
}
func uploadTestServer(t *testing.T, h http.Handler) (*httptest.Server, []byte) {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.TLS = &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
}
func uploadTestClient(t *testing.T, s *httptest.Server, ca []byte) *OCIRegistryUploader {
	t.Helper()
	p, err := NewRegistryUploadPolicy(s.URL, "test-registry", ca, "example.com")
	if err != nil {
		t.Fatal(err)
	}
	u, err := NewOCIRegistryUploader(p)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestRegistryUploadBehavior(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		size                       int
		present, ambiguous, rotate bool
	}{
		{name: "absent"}, {name: "present", present: true}, {name: "multi chunk", size: (8 << 20) + 123}, {name: "ambiguous PATCH", size: (4 << 20) + 123, ambiguous: true}, {name: "rotation", rotate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, f := newUploadFixture(t, tc.size)
			expected := f.blobs
			if !tc.present {
				f.blobs = map[string][]byte{}
			}
			f.present = tc.present
			f.ambiguous = tc.ambiguous
			f.ambiguousLarge = tc.ambiguous
			var first sync.Once
			s, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				f.ServeHTTP(w, r)
				if tc.rotate {
					first.Do(func() { time.Sleep(700 * time.Millisecond); f.mu.Lock(); f.rotated = true; f.mu.Unlock() })
				}
			}))
			source := &uploadTestSource{t: t, origin: s.URL, rotate: tc.rotate}
			defer source.checkClosed()
			u := uploadTestClient(t, s, ca)
			got, err := u.Upload(context.Background(), out, source)
			if err != nil {
				t.Fatal(err)
			}
			if got.Digest != out.TopLevelDigest || got.Repository != uploadTestRepository || got.Size != out.ManifestSize || got.MediaType != out.ManifestMediaType {
				t.Fatal("incorrect upload evidence")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.manifests != 1 {
				t.Fatal("manifest not published exactly once")
			}
			if tc.present && (f.posts != 0 || f.patches != 0) {
				t.Fatal("present content reuploaded")
			}
			for digest, b := range expected {
				if digest == out.TopLevelDigest {
					continue
				}
				if !bytes.Equal(f.blobs[digest], b) {
					t.Fatal("registry blob bytes differ")
				}
			}
			if tc.size > 4<<20 && f.patches < 3 {
				t.Fatal("large blob not chunked")
			}
			if tc.ambiguous && (f.queries != 1 || f.posts != 2 || len(f.patchSizes) <= f.uncertainPatchIndex+1 || f.patchSizes[f.uncertainPatchIndex] != 4<<20 || f.patchSizes[f.uncertainPatchIndex+1] != 2<<20) {
				t.Fatal("ambiguous chunk did not resume exact suffix")
			}
			if tc.rotate && len(source.issued) != 2 {
				t.Fatal("credential not rotated")
			}
		})
	}
}

func TestRegistryUploadRejectsRegistryFaults(t *testing.T) {
	cases := []struct{ method, fault string }{}
	for _, method := range []string{"HEAD", "POST", "PATCH", "GET", "PUT"} {
		for _, status := range []string{"401", "403", "404", "416", "429", "500", "503"} {
			if method == "HEAD" && status == "404" {
				continue
			}
			cases = append(cases, struct{ method, fault string }{method, status})
		}
	}
	for _, fault := range []string{"redirect", "body", "headers", "multiple location", "multiple range", "range", "overflow range", "negative range", "cross origin", "cross repository", "escape", "encoded escape", "query escape", "userinfo", "fragment"} {
		cases = append(cases, struct{ method, fault string }{"POST", fault})
	}
	for _, fault := range []string{"missing digest", "wrong digest", "multiple digest"} {
		cases = append(cases, struct{ method, fault string }{"PUT", fault})
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.fault, func(t *testing.T) {
			out, f := newUploadFixture(t, 0)
			f.blobs = map[string][]byte{}
			f.faultMethod = tc.method
			f.fault = tc.fault
			f.ambiguous = tc.method == "GET"
			s, ca := uploadTestServer(t, f)
			source := &uploadTestSource{t: t, origin: s.URL}
			defer source.checkClosed()
			_, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source)
			if err == nil {
				t.Fatal("accepted registry fault")
			}
			if len(err.Error()) > 160 || strings.Contains(err.Error(), "PRIVATE-TOKEN") || strings.Contains(err.Error(), "private.signature") {
				t.Fatal("unbounded or sensitive error")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.manifests != 0 {
				t.Fatal("manifest published after failed blob")
			}
		})
	}
}

func TestRegistryUploadPolicyAndTLS(t *testing.T) {
	out, f := newUploadFixture(t, 0)
	s, ca := uploadTestServer(t, f)
	for _, origin := range []string{"http://registry.test", "https://u:p@registry.test", s.URL + "/", s.URL + "/v2", s.URL + "?x=1", s.URL + "#f", s.URL + "?", s.URL + "#"} {
		if _, err := NewRegistryUploadPolicy(origin, "test-registry", ca, "example.com"); err == nil {
			t.Fatalf("accepted invalid origin %q", origin)
		}
	}
	for _, roots := range [][]byte{nil, {}, []byte("garbage")} {
		if _, err := NewRegistryUploadPolicy(s.URL, "test-registry", roots, "example.com"); err == nil {
			t.Fatal("accepted invalid trust")
		}
	}
	if _, err := NewRegistryUploadPolicy(s.URL, "test-registry", ca, ""); err == nil {
		t.Fatal("accepted ambient identity")
	}
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	source := &uploadTestSource{t: t, origin: s.URL}
	if _, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source); err != nil {
		t.Fatal("ambient proxy used:", err)
	}
	source.checkClosed()
	for _, tc := range []string{"wrong CA", "wrong identity", "TLS 1.2"} {
		t.Run(tc, func(t *testing.T) {
			server, roots := uploadTestServer(t, f)
			identity := "example.com"
			if tc == "wrong CA" {
				key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				cert := &x509.Certificate{SerialNumber: big.NewInt(42), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
				der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
				if err != nil {
					t.Fatal(err)
				}
				roots = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
			}
			if tc == "wrong identity" {
				identity = "wrong.example"
			}
			if tc == "TLS 1.2" {
				server.Close()
				server = httptest.NewUnstartedServer(f)
				server.Config.ErrorLog = log.New(io.Discard, "", 0)
				server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}
				server.StartTLS()
				defer server.Close()
			}
			p, err := NewRegistryUploadPolicy(server.URL, "test-registry", roots, identity)
			if err != nil {
				t.Fatal(err)
			}
			u, err := NewOCIRegistryUploader(p)
			if err != nil {
				t.Fatal(err)
			}
			source := &uploadTestSource{t: t, origin: server.URL}
			defer source.checkClosed()
			if _, err := u.Upload(context.Background(), out, source); err == nil {
				t.Fatal("accepted untrusted TLS")
			}
		})
	}
}

func TestRegistryUploadContextAndIsolation(t *testing.T) {
	for _, tc := range []string{"timeout", "source failure", "repository switch", "sidecar", "changed output"} {
		t.Run(tc, func(t *testing.T) {
			out, f := newUploadFixture(t, 0)
			f.blobs = map[string][]byte{}
			if tc == "timeout" {
				f.faultMethod = "POST"
				f.fault = "timeout"
			}
			var once sync.Once
			s, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				f.ServeHTTP(w, r)
				if tc == "repository switch" {
					once.Do(func() { time.Sleep(700 * time.Millisecond); f.mu.Lock(); f.rotated = true; f.mu.Unlock() })
				}
			}))
			source := &uploadTestSource{t: t, origin: s.URL, fail: tc == "source failure", rotate: tc == "repository switch", switchRepository: tc == "repository switch"}
			defer source.checkClosed()
			if tc == "sidecar" {
				source.component = "sidecar:db"
				source.repository = "loom-task-image-attempts/x86_64/22222222-2222-4222-8222-222222222222/sidecar-sha256-" + sha256Hex([]byte("sidecar:db"))
				f.repository = source.repository
			}
			if tc == "changed output" {
				file, err := os.OpenFile(out.Path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = file.Write([]byte("mutation"))
				_ = file.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if tc == "timeout" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 80*time.Millisecond)
			}
			defer cancel()
			start := time.Now()
			_, err := uploadTestClient(t, s, ca).Upload(ctx, out, source)
			if tc == "sidecar" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted invalid upload")
			}
			if strings.Contains(err.Error(), "PRIVATE-TOKEN") {
				t.Fatal("source leaked token")
			}
			if tc == "timeout" && time.Since(start) > time.Second {
				t.Fatal("context timeout ignored")
			}
		})
	}
}

func TestRegistryUploadWireHeadersAndManifestEvidence(t *testing.T) {
	for _, fault := range []string{"duplicate length", "informational", "unexpected header", "folded header", "manifest wrong digest", "manifest missing digest", "manifest multiple digest", "HEAD missing digest", "HEAD wrong size", "PATCH wrong range", "GET ahead", "GET behind", "GET changed location", "GET malformed range"} {
		t.Run(fault, func(t *testing.T) {
			out, f := newUploadFixture(t, (4<<20)+10)
			if strings.HasPrefix(fault, "PATCH") || strings.HasPrefix(fault, "GET") {
				f.blobs = map[string][]byte{}
			}
			f.ambiguous = strings.HasPrefix(fault, "GET")
			s, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				manifest := r.Method == "PUT" && strings.Contains(r.URL.Path, "/manifests/")
				switch {
				case (fault == "duplicate length" || fault == "informational" || fault == "folded header") && r.Method == "HEAD":
					conn, rw, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					if fault == "informational" {
						_, _ = rw.WriteString("HTTP/1.1 103 Early Hints\r\n\r\n")
					}
					digest := strings.TrimPrefix(r.URL.Path, "/v2/"+f.repository+"/blobs/")
					length := strconv.Itoa(len(f.blobs[digest]))
					head := "Docker-Content-Digest: " + digest + "\r\nContent-Length: " + length + "\r\n"
					if fault == "duplicate length" {
						head += "Content-Length: " + length + "\r\n"
					}
					if fault == "folded header" {
						head += "Server: test\r\n folded\r\n"
					}
					_, _ = rw.WriteString("HTTP/1.1 200 OK\r\n" + head + "\r\n")
					_ = rw.Flush()
					return
				case fault == "unexpected header":
					w.Header().Set("X-Unexpected", "value")
					w.WriteHeader(404)
					return
				case strings.HasPrefix(fault, "manifest") && manifest:
					if fault != "manifest missing digest" {
						w.Header().Set("Docker-Content-Digest", "sha256:"+strings.Repeat("a", 64))
					}
					if fault == "manifest multiple digest" {
						w.Header().Add("Docker-Content-Digest", out.TopLevelDigest)
					}
					w.WriteHeader(201)
					return
				case fault == "HEAD missing digest" && r.Method == "HEAD":
					w.Header().Set("Content-Length", "0")
					w.WriteHeader(200)
					return
				case fault == "HEAD wrong size" && r.Method == "HEAD":
					w.Header().Set("Docker-Content-Digest", strings.TrimPrefix(r.URL.Path, "/v2/"+f.repository+"/blobs/"))
					w.Header().Set("Content-Length", "0")
					w.WriteHeader(200)
					return
				case fault == "PATCH wrong range" && r.Method == "PATCH":
					f.mu.Lock()
					defer f.mu.Unlock()
					w.Header().Set("Location", f.lastUploadURL)
					w.Header().Set("Range", "0-999999")
					w.WriteHeader(202)
					return
				case strings.HasPrefix(fault, "GET") && r.Method == "GET":
					f.mu.Lock()
					defer f.mu.Unlock()
					loc := f.lastUploadURL
					rg := "0-99999999"
					if fault == "GET behind" {
						rg = "0-0"
					}
					if fault == "GET changed location" {
						loc = strings.Replace(loc, "/id?", "/different?", 1)
						rg = fmt.Sprintf("0-%d", len(f.current)-1)
					}
					if fault == "GET malformed range" {
						rg = "garbage"
					}
					w.Header().Set("Location", loc)
					w.Header().Set("Range", rg)
					w.WriteHeader(204)
					return
				}
				f.ServeHTTP(w, r)
			}))
			source := &uploadTestSource{t: t, origin: s.URL}
			defer source.checkClosed()
			if _, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source); err == nil {
				t.Fatal("accepted invalid wire response")
			}
		})
	}
}

func TestRegistryUploadRecoveryBounds(t *testing.T) {
	for _, mode := range []string{"fully acknowledged", "no progress"} {
		t.Run(mode, func(t *testing.T) {
			out, f := newUploadFixture(t, 0)
			f.blobs = map[string][]byte{}
			patches, queries := 0, 0
			s, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PATCH" {
					f.mu.Lock()
					defer f.mu.Unlock()
					patches++
					if r.URL.RequestURI() != f.lastUploadURL {
						t.Error("changed retry location")
					}
					if patches == 1 {
						b, err := io.ReadAll(io.LimitReader(r.Body, (4<<20)+1))
						if err != nil {
							t.Error(err)
						}
						if mode == "no progress" {
							b = b[:len(b)/2]
						}
						f.current = append(f.current, b...)
					}
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				if r.Method == "GET" {
					f.mu.Lock()
					queries++
					f.mu.Unlock()
				}
				f.ServeHTTP(w, r)
			}))
			source := &uploadTestSource{t: t, origin: s.URL}
			defer source.checkClosed()
			_, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source)
			f.mu.Lock()
			defer f.mu.Unlock()
			if mode == "fully acknowledged" {
				if err != nil {
					t.Fatal(err)
				}
				if patches != 1 || queries != 1 {
					t.Fatal("replayed acknowledged bytes")
				}
			} else {
				if err == nil || patches != 4 || queries != 3 || f.posts != 1 {
					t.Fatal("unbounded recovery or blind restart")
				}
			}
		})
	}
}

func TestRegistryUploadFixedRequestDeadline(t *testing.T) {
	out, f := newUploadFixture(t, 0)
	f.faultMethod = "HEAD"
	f.fault = "timeout"
	s, ca := uploadTestServer(t, f)
	source := &uploadTestSource{t: t, origin: s.URL}
	defer source.checkClosed()
	start := time.Now()
	_, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source)
	elapsed := time.Since(start)
	if err == nil || elapsed < 14*time.Second || elapsed > 18*time.Second {
		t.Fatalf("15-second request bound failed: %v %s", err, elapsed)
	}
}

func TestRegistryUploadRejectsEncodedTokenLocation(t *testing.T) {
	out, f := newUploadFixture(t, 0)
	f.blobs = map[string][]byte{}
	s, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.Header().Set("Location", "/v2/"+f.repository+"/blobs/uploads/id?state=%74oken-1.private.signature")
			w.Header().Set("Range", "0-0")
			w.WriteHeader(202)
			return
		}
		if r.Method == "PATCH" {
			t.Error("token placed in an upload URL")
		}
		f.ServeHTTP(w, r)
	}))
	source := &uploadTestSource{t: t, origin: s.URL}
	defer source.checkClosed()
	if _, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source); err == nil {
		t.Fatal("accepted token-bearing location")
	}
}

func TestRegistryUploadResponseBodyBoundary(t *testing.T) {
	for _, size := range []int{65536, 65537} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			out, f := newUploadFixture(t, 0)
			f.blobs = map[string][]byte{}
			s, ca := uploadTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				f.ServeHTTP(w, r)
				if r.Method == "POST" {
					_, _ = w.Write(bytes.Repeat([]byte("x"), size))
				}
			}))
			source := &uploadTestSource{t: t, origin: s.URL}
			defer source.checkClosed()
			_, err := uploadTestClient(t, s, ca).Upload(context.Background(), out, source)
			if (err == nil) != (size == 65536) {
				t.Fatalf("incorrect body budget enforcement: %v", err)
			}
		})
	}
}
