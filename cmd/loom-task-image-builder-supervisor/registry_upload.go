package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	registryChunkBytes     = 4 << 20
	registryResponseBytes  = 64 << 10
	registryRequestTimeout = 15 * time.Second
	registryRenewBefore    = registryRequestTimeout + time.Second
	registryMaxRecoveries  = 3
)

// RegistryUploadPolicy can only be constructed from independently trusted
// origin/service and explicit PEM certificates. Accepting PEM instead of an
// arbitrary CertPool prevents a caller from supplying ambient system roots.
// Fields are private so there are no proxy, redirect or TLS downgrade options.
type RegistryUploadPolicy struct {
	origin              url.URL
	service, serverName string
	roots               *x509.CertPool
	chunkBytes          int
	requestTimeout      time.Duration
	responseBytes       int64
	minTLS, maxTLS      uint16
}

func NewRegistryUploadPolicy(origin, service string, caPEM []byte, serverName string) (RegistryUploadPolicy, error) {
	bad := func() (RegistryUploadPolicy, error) {
		return RegistryUploadPolicy{}, errors.New("registry upload policy invalid")
	}
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(origin, "#") || u.Opaque != "" || u.String() != origin || len(origin) > 2048 || !registryIdentityPattern.MatchString(service) || serverName == "" || len(serverName) > 253 || strings.ContainsAny(serverName, "/@?#:\\ \t\r\n") || len(caPEM) == 0 || len(caPEM) > registryResponseBytes {
		return bad()
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return bad()
		}
	}
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(caPEM)) > 0 {
		caPEM = bytes.TrimSpace(caPEM)
		if !bytes.HasPrefix(caPEM, []byte("-----BEGIN CERTIFICATE-----")) {
			return bad()
		}
		block, rest := pem.Decode(caPEM)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return bad()
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return bad()
		}
		roots.AddCert(cert)
		count++
		caPEM = rest
	}
	if count == 0 {
		return bad()
	}
	return RegistryUploadPolicy{origin: *u, service: service, serverName: serverName, roots: roots, chunkBytes: registryChunkBytes, requestTimeout: registryRequestTimeout, responseBytes: registryResponseBytes, minTLS: tls.VersionTLS13, maxTLS: tls.VersionTLS13}, nil
}

type OCIRegistryUploader struct{ policy RegistryUploadPolicy }

func NewOCIRegistryUploader(policy RegistryUploadPolicy) (*OCIRegistryUploader, error) {
	if policy.roots == nil || len(policy.roots.Subjects()) == 0 || policy.serverName == "" || policy.origin.Scheme != "https" || policy.chunkBytes != registryChunkBytes || policy.requestTimeout != registryRequestTimeout || policy.responseBytes != registryResponseBytes || policy.minTLS != tls.VersionTLS13 || policy.maxTLS != tls.VersionTLS13 {
		return nil, errors.New("registry upload policy invalid")
	}
	policy.roots = policy.roots.Clone()
	return &OCIRegistryUploader{policy: policy}, nil
}

// Task 8 binds this interface to an exact immutable BuiltComponentSet/component.
// Next(nil) obtains the initial credential. Successful Next(predecessor) owns
// closing that predecessor, as PublicationCredentialSource.Next already does.
// On failure Next must leave predecessor owned by the caller. Close releases
// the final (or rejected successor) credential exactly once. Calls are serial.
type RegistryUploadCredentialSource interface {
	Next(context.Context, *RegistryCredential) (*RegistryCredential, error)
	Close(*RegistryCredential)
}

// UploadedManifest is upload acknowledgement evidence only. It does not change
// materialization state, verify registry bytes, or assert readiness.
type UploadedManifest struct {
	Repository, Digest, MediaType string
	Size                          int64
}

type registryUploadSession struct {
	policy                          RegistryUploadPolicy
	client                          *http.Client
	source                          RegistryUploadCredentialSource
	credential                      *RegistryCredential
	repository, component, platform string
}

func (u *OCIRegistryUploader) Upload(ctx context.Context, output OCIOutput, source RegistryUploadCredentialSource) (UploadedManifest, error) {
	if u == nil || source == nil || ctx == nil {
		return UploadedManifest{}, errors.New("registry upload input invalid")
	}
	file, err := os.Open(output.Path)
	if err != nil {
		return UploadedManifest{}, errors.New("registry upload OCI unavailable")
	}
	defer file.Close()
	checked, layout, err := scanOCIFile(ctx, file, output.Path, output.OS+"/"+output.Architecture)
	if err != nil || checked != output {
		return UploadedManifest{}, errors.New("registry upload OCI evidence mismatch")
	}
	transport := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: registryRequestTimeout, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:     &tls.Config{RootCAs: u.policy.roots.Clone(), ServerName: u.policy.serverName, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13},
		TLSHandshakeTimeout: registryRequestTimeout, ResponseHeaderTimeout: registryRequestTimeout,
		DisableCompression: true, MaxResponseHeaderBytes: registryResponseBytes,
		MaxIdleConns: 1, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1, IdleConnTimeout: registryRequestTimeout,
		ForceAttemptHTTP2: false, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	// A fresh connection per request lets the wire guard reject duplicate and
	// informational headers before net/http normalizes them. No idle sockets or
	// automatic transport replay survive a request.
	transport.DisableKeepAlives = true
	transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, registryRequestTimeout)
		defer cancel()
		conn, err := (&net.Dialer{Timeout: registryRequestTimeout}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		secured := tls.Client(conn, transport.TLSClientConfig)
		if err := secured.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		if secured.ConnectionState().Version != tls.VersionTLS13 {
			secured.Close()
			return nil, errRegistryWireHeaders
		}
		return &registryWireConn{Conn: secured, reader: bufio.NewReaderSize(secured, registryResponseBytes+1)}, nil
	}
	defer transport.CloseIdleConnections()
	s := registryUploadSession{policy: u.policy, client: &http.Client{Transport: transport, Timeout: registryRequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, source: source, platform: output.OS + "/" + output.Architecture}
	defer func() {
		if s.credential != nil {
			source.Close(s.credential)
		}
	}()
	initialCtx, cancel := context.WithTimeout(ctx, registryRequestTimeout)
	err = s.ensureCredential(initialCtx)
	cancel()
	if err != nil {
		return UploadedManifest{}, err
	}
	chunk := make([]byte, registryChunkBytes)
	descriptors := append([]ociDescriptor{layout.manifest.Config}, layout.manifest.Layers...)
	uploaded := map[string]bool{}
	for _, d := range descriptors {
		if uploaded[d.Digest] {
			continue
		}
		entry, err := descriptorEntry(layout.entries, d)
		if err != nil {
			return UploadedManifest{}, errors.New("registry upload OCI descriptor invalid")
		}
		if err := s.uploadBlob(ctx, file, entry, d, chunk); err != nil {
			return UploadedManifest{}, err
		}
		uploaded[d.Digest] = true
	}
	manifest, err := readOCIJSON(file, layout.entries, "blobs/sha256/"+strings.TrimPrefix(output.TopLevelDigest, "sha256:"))
	if err != nil {
		return UploadedManifest{}, errors.New("registry upload manifest changed")
	}
	target := s.contentURL("manifests", output.TopLevelDigest)
	resp, err := s.request(ctx, "PUT", target, manifest, output.ManifestMediaType, "")
	if err != nil {
		return UploadedManifest{}, err
	}
	if resp.StatusCode != http.StatusCreated || resp.Header.Get("Docker-Content-Digest") != output.TopLevelDigest || !s.validContentHeaders(resp, target) {
		return UploadedManifest{}, errors.New("registry upload manifest acknowledgement invalid")
	}
	return UploadedManifest{Repository: s.repository, Digest: output.TopLevelDigest, MediaType: output.ManifestMediaType, Size: output.ManifestSize}, nil
}

func (s *registryUploadSession) ensureCredential(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.New("registry upload canceled")
	}
	if s.credential != nil && time.Until(s.credential.ExpiresAt) > registryRenewBefore {
		return nil
	}
	previous := s.credential
	next, err := s.source.Next(ctx, previous)
	if err != nil {
		if next != nil && next != previous {
			s.source.Close(next)
		}
		return errors.New("registry upload credential unavailable")
	}
	// A successful renewal has already released previous through the source.
	s.credential = next
	if next == nil || next == previous || next.secret == nil || next.secret.closed || len(next.BearerToken) == 0 || len(next.BearerToken) > registryResponseBytes || time.Until(next.ExpiresAt) <= registryRenewBefore || next.RegistryOrigin != s.policy.origin.String() || next.RegistryService != s.policy.service || next.Platform != s.platform || !bearerTokenPattern.Match(next.BearerToken) {
		return errors.New("registry upload credential invalid")
	}
	repository, err := publicationRepository(next.CPUArch, next.AttemptID, next.Component)
	if err != nil || repository != next.Repository {
		return errors.New("registry upload repository invalid")
	}
	if s.repository != "" && (next.Repository != s.repository || next.Component != s.component) {
		return errors.New("registry upload credential binding changed")
	}
	s.repository = next.Repository
	s.component = next.Component
	return nil
}

func (s *registryUploadSession) repositoryPath() string {
	segments := strings.Split(s.repository, "/")
	for i := range segments {
		segments[i] = url.PathEscape(segments[i])
	}
	return "/v2/" + strings.Join(segments, "/")
}
func (s *registryUploadSession) contentURL(kind, digest string) *url.URL {
	u := s.policy.origin
	u.Path = s.repositoryPath() + "/" + kind + "/" + digest
	return &u
}

var errRegistryTransport = errors.New("registry upload transport failed")

// request owns each response body, discards at most budget+1 bytes, and removes
// the temporary Go string header before returning. No net/http error is exposed:
// those errors may include attacker-controlled URLs or response text.
func (s *registryUploadSession) request(ctx context.Context, method string, target *url.URL, body []byte, mediaType, contentRange string) (*http.Response, error) {
	requestCtx, cancel := context.WithTimeout(ctx, registryRequestTimeout)
	defer cancel()
	if err := s.ensureCredential(requestCtx); err != nil {
		return nil, err
	}
	if target == nil || target.Scheme != s.policy.origin.Scheme || target.Host != s.policy.origin.Host || target.User != nil || target.Fragment != "" || bytes.Contains([]byte(target.String()), s.credential.BearerToken) {
		return nil, errors.New("registry upload request URL invalid")
	}
	req, err := http.NewRequestWithContext(requestCtx, method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("registry upload request invalid")
	}
	// Disable replay of request bodies, including net/http's optional retries.
	req.GetBody = nil
	if mediaType != "" {
		req.Header.Set("Content-Type", mediaType)
	}
	if contentRange != "" {
		req.Header.Set("Content-Range", contentRange)
	}
	authorization := make([]byte, 7+len(s.credential.BearerToken))
	copy(authorization, "Bearer ")
	copy(authorization[7:], s.credential.BearerToken)
	req.Header.Set("Authorization", string(authorization))
	zeroBytes(authorization)
	defer req.Header.Del("Authorization")
	resp, err := s.client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if errors.Is(err, errRegistryWireHeaders) {
			return nil, errRegistryWireHeaders
		}
		return nil, errRegistryTransport
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 || !validRegistryHeaders(resp.Header) || resp.Uncompressed || len(resp.Trailer) != 0 {
		return nil, errors.New("registry upload response headers invalid")
	}
	if method != "HEAD" && resp.ContentLength > registryResponseBytes {
		return nil, errors.New("registry upload response body exceeded")
	}
	n, err := io.Copy(io.Discard, io.LimitReader(resp.Body, registryResponseBytes+1))
	if n > registryResponseBytes || len(resp.Trailer) != 0 {
		return nil, errors.New("registry upload response body invalid")
	}
	if err != nil {
		return nil, errRegistryTransport
	}
	resp.Request = nil
	return resp, nil
}

func validRegistryHeaders(headers http.Header) bool {
	total := 0
	for key, values := range headers {
		switch key {
		case "Date", "Server", "Content-Length", "Content-Type", "Docker-Distribution-Api-Version", "Docker-Upload-Uuid", "Docker-Content-Digest", "Location", "Range", "Connection":
		default:
			return false
		}
		if len(values) != 1 || values[0] == "" {
			return false
		}
		total += len(key) + len(values[0]) + 4
		if total > registryResponseBytes || strings.ContainsAny(values[0], "\r\n\x00") {
			return false
		}
	}
	if v := headers.Get("Docker-Distribution-Api-Version"); v != "" && v != "registry/2.0" {
		return false
	}
	if v := headers.Get("Docker-Content-Digest"); v != "" {
		if _, err := parseSHA256Descriptor(v); err != nil {
			return false
		}
	}
	if v := headers.Get("Content-Length"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 || strconv.FormatInt(n, 10) != v {
			return false
		}
	}
	return true
}

var uploadIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,255}$`)

func (s *registryUploadSession) uploadLocation(base *url.URL, raw string) (*url.URL, error) {
	fail := func() (*url.URL, error) { return nil, errors.New("registry upload location invalid") }
	if raw == "" || len(raw) > registryResponseBytes || strings.ContainsAny(raw, "\\\r\n\t #") {
		return fail()
	}
	ref, err := url.Parse(raw)
	if err != nil || ref.User != nil || ref.Opaque != "" || ref.Fragment != "" || ref.RawPath != "" {
		return fail()
	}
	// Reject dot segments before resolution as well as checking the resolved path.
	for _, segment := range strings.Split(ref.Path, "/") {
		if segment == "." || segment == ".." {
			return fail()
		}
	}
	resolved := base.ResolveReference(ref)
	prefix := s.repositoryPath() + "/blobs/uploads/"
	if resolved.Scheme != s.policy.origin.Scheme || resolved.Host != s.policy.origin.Host || resolved.User != nil || resolved.RawPath != "" || path.Clean(resolved.Path) != resolved.Path || !strings.HasPrefix(resolved.Path, prefix) || !uploadIDPattern.MatchString(strings.TrimPrefix(resolved.Path, prefix)) {
		return fail()
	}
	query, err := url.ParseQuery(resolved.RawQuery)
	if err != nil {
		return fail()
	}
	for key, values := range query {
		if s.credential != nil && (bytes.Contains([]byte(key), s.credential.BearerToken) || len(values) == 1 && bytes.Contains([]byte(values[0]), s.credential.BearerToken)) {
			return fail()
		}
		if key == "" || key == "digest" || len(values) != 1 {
			return fail()
		}
	}
	if s.credential != nil && bytes.Contains([]byte(resolved.String()), s.credential.BearerToken) {
		return fail()
	}
	return resolved, nil
}

func registryAcknowledgedRange(value string) (int64, error) {
	end, ok := strings.CutPrefix(value, "0-")
	if !ok || end == "" {
		return 0, errors.New("registry upload range invalid")
	}
	n, err := strconv.ParseInt(end, 10, 64)
	if err != nil || n < 0 || n >= maxOCITarBytes || strconv.FormatInt(n, 10) != end {
		return 0, errors.New("registry upload range invalid")
	}
	return n + 1, nil
}
func (s *registryUploadSession) validContentHeaders(resp *http.Response, target *url.URL) bool {
	if resp.Header.Get("Range") != "" {
		return false
	}
	if raw := resp.Header.Get("Location"); raw != "" {
		ref, err := url.Parse(raw)
		if err != nil || ref.User != nil || ref.RawPath != "" || ref.RawQuery != "" || ref.ForceQuery || ref.Fragment != "" || strings.Contains(raw, "#") {
			return false
		}
		if resolved := target.ResolveReference(ref); resolved.String() != target.String() {
			return false
		}
	}
	return true
}

func (s *registryUploadSession) uploadBlob(ctx context.Context, file *os.File, entry ociEntry, d ociDescriptor, chunk []byte) error {
	target := s.contentURL("blobs", d.Digest)
	resp, err := s.request(ctx, "HEAD", target, nil, "", "")
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		if resp.Header.Get("Docker-Content-Digest") != d.Digest || resp.ContentLength != d.Size || !s.validContentHeaders(resp, target) {
			return errors.New("registry upload HEAD evidence invalid")
		}
		return nil
	}
	if resp.StatusCode != http.StatusNotFound || resp.Header.Get("Location") != "" || resp.Header.Get("Range") != "" || resp.Header.Get("Docker-Content-Digest") != "" {
		return errors.New("registry upload HEAD rejected")
	}
	start := s.policy.origin
	start.Path = s.repositoryPath() + "/blobs/uploads/"
	resp, err = s.request(ctx, "POST", &start, nil, "", "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted || resp.Header.Get("Range") != "0-0" || resp.Header.Get("Docker-Content-Digest") != "" {
		return errors.New("registry upload initiation rejected")
	}
	location, err := s.uploadLocation(&start, resp.Header.Get("Location"))
	if err != nil {
		return err
	}
	reader := io.NewSectionReader(file, entry.offset, entry.size)
	hash := sha256.New()
	var offset int64
	for offset < entry.size {
		size := int64(len(chunk))
		if remaining := entry.size - offset; remaining < size {
			size = remaining
		}
		retained := chunk[:int(size)]
		if _, err := io.ReadFull(reader, retained); err != nil {
			return errors.New("registry upload blob read failed")
		}
		_, _ = hash.Write(retained)
		end := offset + size
		acknowledged := offset
		recoveries := 0
		for acknowledged < end {
			resp, err = s.request(ctx, "PATCH", location, retained[int(acknowledged-offset):], "application/octet-stream", fmt.Sprintf("%d-%d", acknowledged, end-1))
			if errors.Is(err, errRegistryTransport) {
				recoveries++
				if recoveries > registryMaxRecoveries {
					return errors.New("registry upload recovery exhausted")
				}
				// Never issue another POST. Query the exact URL used for the uncertain PATCH.
				resp, err = s.request(ctx, "GET", location, nil, "", "")
				if err != nil {
					return err
				}
				if resp.StatusCode != http.StatusNoContent || resp.Header.Get("Docker-Content-Digest") != "" {
					return errors.New("registry upload recovery rejected")
				}
				next, err := s.uploadLocation(location, resp.Header.Get("Location"))
				if err != nil || next.String() != location.String() {
					return errors.New("registry upload recovery location changed")
				}
				count, err := registryAcknowledgedRange(resp.Header.Get("Range"))
				// Distribution's empty 0-0 range cannot prove whether byte zero was stored.
				if err != nil || count < acknowledged || count > end || (acknowledged == 0 && count == 1) {
					return errors.New("registry upload recovery range invalid")
				}
				acknowledged = count
				continue
			}
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusAccepted || resp.Header.Get("Docker-Content-Digest") != "" {
				return errors.New("registry upload PATCH rejected")
			}
			next, err := s.uploadLocation(location, resp.Header.Get("Location"))
			if err != nil {
				return err
			}
			count, err := registryAcknowledgedRange(resp.Header.Get("Range"))
			if err != nil || count != end {
				return errors.New("registry upload PATCH range invalid")
			}
			acknowledged = count
			location = next
		}
		offset = end
	}
	if hex.EncodeToString(hash.Sum(nil)) != entry.digest {
		return errors.New("registry upload blob changed")
	}
	finish := *location
	query := finish.Query()
	query.Set("digest", d.Digest)
	finish.RawQuery = query.Encode()
	resp, err = s.request(ctx, "PUT", &finish, nil, "application/octet-stream", "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated || resp.Header.Get("Docker-Content-Digest") != d.Digest || !s.validContentHeaders(resp, target) {
		return errors.New("registry upload blob acknowledgement invalid")
	}
	return nil
}

var errRegistryWireHeaders = errors.New("registry upload wire headers invalid")

// net/http collapses equal Content-Length fields and accepts folded and interim
// headers. Validate the original bounded HTTP/1 header block before replaying it
// to that parser. Each connection serves exactly one request.
type registryWireConn struct {
	net.Conn
	reader  *bufio.Reader
	head    *bytes.Reader
	checked bool
}

func (c *registryWireConn) Read(p []byte) (int, error) {
	if !c.checked {
		c.checked = true
		header := make([]byte, 0, 4096)
		seen := map[string]bool{}
		for lineNumber := 0; ; lineNumber++ {
			line, err := c.reader.ReadSlice('\n')
			if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
				return 0, err
			}
			if err != nil || len(header)+len(line) > registryResponseBytes || !bytes.HasSuffix(line, []byte("\r\n")) {
				return 0, errRegistryWireHeaders
			}
			header = append(header, line...)
			if lineNumber == 0 {
				fields := bytes.SplitN(line, []byte(" "), 3)
				if len(fields) != 3 || string(fields[0]) != "HTTP/1.1" || len(fields[1]) != 3 {
					return 0, errRegistryWireHeaders
				}
				code, err := strconv.Atoi(string(fields[1]))
				if err != nil || code < 200 || code > 599 {
					return 0, errRegistryWireHeaders
				}
			} else {
				if bytes.Equal(line, []byte("\r\n")) {
					break
				}
				key, _, ok := bytes.Cut(line, []byte(":"))
				if !ok || len(key) == 0 {
					return 0, errRegistryWireHeaders
				}
				for _, b := range key {
					if !(b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-') {
						return 0, errRegistryWireHeaders
					}
				}
				canonical := strings.ToLower(string(key))
				if seen[canonical] {
					return 0, errRegistryWireHeaders
				}
				seen[canonical] = true
			}
		}
		c.head = bytes.NewReader(header)
	}
	if c.head != nil {
		n, err := c.head.Read(p)
		if err != io.EOF {
			return n, err
		}
		c.head = nil
	}
	return c.reader.Read(p)
}
