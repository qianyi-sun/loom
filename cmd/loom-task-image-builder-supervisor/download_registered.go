package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type DownloadedRegisteredBundle struct {
	RelativeRoot     string // Cleanup label only; not execution authority.
	ManifestSHA256   string
	TotalBytes       int64
	ExpiresAt        time.Time
	parentFD, rootFD int
}

// DupDirectoryFD transfers independent ownership of the verified input root.
// Execution must pass this descriptor into the context-sending process; neither
// a cached absolute path nor RelativeRoot is a verified execution handoff.
// The caller must finish and join context consumers before Close removes the
// owned files; duplicating a descriptor does not extend that cleanup lifetime.
func (b *DownloadedRegisteredBundle) DupDirectoryFD() (int, error) {
	if b == nil || b.rootFD < 0 {
		return -1, errors.New("registered bundle context unavailable")
	}
	return fcntlInt(b.rootFD, syscall.F_DUPFD_CLOEXEC, 0)
}

func (b *DownloadedRegisteredBundle) Close() error {
	if b == nil || b.rootFD < 0 {
		return nil
	}
	root, parent := b.rootFD, b.parentFD
	b.rootFD, b.parentFD = -1, -1
	// Reuse the pinned-descriptor private-tree cleanup owner. A renamed root
	// cannot redirect recursive cleanup into its replacement; ambiguity surfaces.
	if err := errors.Join(cleanupBuildCapture(parent, root, b.RelativeRoot), syscall.Close(parent)); err != nil {
		return errors.Join(errCleanupAmbiguous, err)
	}
	return nil
}

func createRegisteredBundleDirectory(jobFD int) (*DownloadedRegisteredBundle, error) {
	id, err := newUUID()
	if err != nil {
		return nil, err
	}
	parent, err := syscall.Dup(jobFD)
	if err != nil {
		return nil, err
	}
	syscall.CloseOnExec(parent)
	name := ".bundle-" + id
	if err := syscall.Mkdirat(parent, name, 0o700); err != nil {
		syscall.Close(parent)
		return nil, err
	}
	root, err := syscall.Openat(parent, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		syscall.Close(parent)
		// No pinned root: leave cleanup to the allocation owner, never remove an
		// unproven possibly replaced pathname.
		return nil, errors.Join(errCleanupAmbiguous, errors.New("registered bundle directory could not be pinned"))
	}
	bundle := &DownloadedRegisteredBundle{RelativeRoot: name, parentFD: parent, rootFD: root}
	if err := validateBuildCaptureDirectory(root); err != nil {
		return nil, errors.Join(err, bundle.Close())
	}
	return bundle, nil
}

type registeredDownloadBudget struct {
	ctx               context.Context
	clock             func() time.Time
	previous, expires time.Time
}

func (b *registeredDownloadBudget) check() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	now := b.clock()
	if now.Before(b.previous) || !now.Before(b.expires) {
		return errors.New("registered bundle authorization expired or clock regressed")
	}
	b.previous = now
	return nil
}

type registeredBudgetReader struct {
	io.Reader
	budget *registeredDownloadBudget
}

func (r registeredBudgetReader) Read(payload []byte) (int, error) {
	if err := r.budget.check(); err != nil {
		return 0, err
	}
	n, err := r.Reader.Read(payload)
	if checkErr := r.budget.check(); checkErr != nil {
		return 0, checkErr
	}
	return n, err
}

type registeredIdleConn struct{ net.Conn }

func (c registeredIdleConn) Read(payload []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return 0, err
	}
	return c.Conn.Read(payload)
}

func registeredBundleHTTPClient(trust BundleDownloadTrust) (*http.Client, *http.Transport, error) {
	if _, err := registeredBundleOrigin(trust); err != nil {
		return nil, nil, err
	}
	if trust.Roots == nil {
		return nil, nil, errors.New("registered bundle trusted CA unavailable")
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &http.Transport{
		Proxy: nil, DisableCompression: true, DisableKeepAlives: true, MaxConnsPerHost: 1,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: trust.Roots.Clone()},
		TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second, MaxResponseHeaderBytes: 32 * 1024,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			connection, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return registeredIdleConn{connection}, nil
		},
	}
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("registered bundle redirects forbidden")
	}}, transport, nil
}

func DownloadRegisteredBundle(ctx context.Context, secret *SecretBuffer, jobFD int, plan RegisteredBundlePlan, session BundleSessionBinding, trust BundleDownloadTrust, clock func() time.Time) (_ *DownloadedRegisteredBundle, err error) {
	started := time.Now()
	if secret == nil || secret.closed || clock == nil {
		return nil, errors.New("registered bundle input unavailable")
	}
	if _, err := validateDirectoryDescriptor(jobFD); err != nil {
		return nil, err
	}
	observed := clock()
	capability, err := parseRegisteredBundleCapability(secret.data, plan, session, trust, observed)
	if err != nil {
		return nil, err
	}
	client, transport, err := registeredBundleHTTPClient(trust)
	if err != nil {
		return nil, err
	}
	defer transport.CloseIdleConnections()
	// A monotonic timeout limits the whole operation independently of wall-clock
	// progress. Per-I/O checks additionally reject forward jumps and regression.
	ctx, cancel := context.WithDeadline(ctx, started.Add(capability.ExpiresAt.Sub(observed)))
	defer cancel()
	budget := &registeredDownloadBudget{ctx: ctx, clock: clock, previous: observed, expires: capability.ExpiresAt}
	if err := budget.check(); err != nil {
		return nil, err
	}
	bundle, err := createRegisteredBundleDirectory(jobFD)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, bundle.Close())
		}
	}()
	for _, object := range capability.Objects {
		if err := budget.check(); err != nil {
			return nil, err
		}
		if err := downloadRegisteredObject(client, bundle.rootFD, object, budget); err != nil {
			return nil, err
		}
	}
	if err := budget.check(); err != nil {
		return nil, err
	}
	if err := fsyncDirectory(bundle.rootFD); err != nil {
		return nil, err
	}
	if err := validateBuildCapturePath(bundle.parentFD, bundle.rootFD, bundle.RelativeRoot); err != nil {
		return nil, err
	}
	// Kernel filesystem I/O cannot be interrupted by context. Recheck after it;
	// never return acceptance or allow execution using an expired result.
	if err := budget.check(); err != nil {
		return nil, err
	}
	bundle.ManifestSHA256, bundle.TotalBytes, bundle.ExpiresAt = capability.ManifestSHA256, capability.TotalBytes, capability.ExpiresAt
	return bundle, nil
}

func downloadRegisteredObject(client *http.Client, rootFD int, object RegisteredBundleObject, budget *registeredDownloadBudget) error {
	trace := &httptrace.ClientTrace{Got1xxResponse: func(int, textproto.MIMEHeader) error {
		return errors.New("registered bundle informational response forbidden")
	}}
	ctx := httptrace.WithClientTrace(budget.ctx, trace)
	// Preserve the already validated absolute signed URL byte-for-byte, including
	// RawPath and RawQuery. Never synthesize a URL from decoded path components.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, object.URL, nil)
	if err != nil {
		return errors.New("registered bundle request invalid")
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("registered bundle request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Uncompressed ||
		(response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity") ||
		(response.ContentLength >= 0 && response.ContentLength != object.SizeBytes) {
		return errors.New("registered bundle response invalid")
	}
	if err := budget.check(); err != nil {
		return err
	}
	if err := createBundleParentDirectories(rootFD, filepath.Dir(object.RelativePath)); err != nil {
		return err
	}
	mode := uint32(0o644)
	if object.Mode == "0755" {
		mode = 0o755
	}
	fd, err := createBundleFile(rootFD, object.RelativePath, mode)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), object.RelativePath)
	defer file.Close()
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), registeredBudgetReader{Reader: io.LimitReader(response.Body, object.SizeBytes+1), budget: budget})
	if copyErr != nil || written != object.SizeBytes || fmt.Sprintf("%x", hash.Sum(nil)) != object.SHA256 {
		return errors.New("registered bundle file content mismatch")
	}
	if err := budget.check(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return errors.New("registered bundle file sync failed")
	}
	var stat syscall.Stat_t
	if syscall.Fstat(fd, &stat) != nil || stat.Mode&syscall.S_IFMT != syscall.S_IFREG || stat.Nlink != 1 ||
		stat.Uid != uint32(os.Geteuid()) || stat.Size != object.SizeBytes || stat.Mode&0o7777 != mode {
		return errors.New("registered bundle file metadata changed")
	}
	namedFD, err := openFileBeneath(rootFD, object.RelativePath)
	if err != nil {
		return errors.New("registered bundle file path changed")
	}
	var named syscall.Stat_t
	statErr := syscall.Fstat(namedFD, &named)
	closeErr := syscall.Close(namedFD)
	if statErr != nil || closeErr != nil || stat.Dev != named.Dev || stat.Ino != named.Ino {
		return errors.New("registered bundle file path changed")
	}
	if file.Close() != nil || response.Body.Close() != nil {
		return errors.New("registered bundle file close failed")
	}
	return budget.check()
}
