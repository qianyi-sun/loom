package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

const (
	localSchema        = "loom.task-image-builder-guard-local/v1"
	memfdCloexec       = 0x0001
	memfdAllowSealing  = 0x0002
	fAddSeals          = 1033
	fGetSeals          = 1034
	fSealSeal          = 0x0001
	fSealShrink        = 0x0002
	fSealGrow          = 0x0004
	fSealWrite         = 0x0008
	requiredMemfdSeals = fSealSeal | fSealShrink | fSealGrow | fSealWrite
	prSetDumpable      = 4
	prGetDumpable      = 3
)

type SecretBuffer struct {
	data   []byte
	closed bool
}

func NewSecretBuffer(fd int, maximum int) (_ *SecretBuffer, err error) {
	if fd < 0 || maximum <= 0 {
		if fd >= 0 {
			syscall.Close(fd)
		}
		return nil, errors.New("secret descriptor invalid")
	}
	defer func() {
		if closeErr := syscall.Close(fd); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	if err := setSecretProcessProtections(); err != nil {
		return nil, err
	}
	if err := validateSealedMemfd(fd); err != nil {
		return nil, err
	}
	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		return nil, err
	}
	if statValue.Size <= 0 || statValue.Size > int64(maximum) {
		return nil, errors.New("secret payload invalid")
	}
	data := make([]byte, int(statValue.Size))
	if err := syscall.Mlock(data); err != nil {
		zeroBytes(data)
		return nil, err
	}
	n, err := syscall.Pread(fd, data, 0)
	if err != nil {
		zeroBytes(data)
		_ = syscall.Munlock(data)
		return nil, err
	}
	if n != len(data) {
		zeroBytes(data)
		_ = syscall.Munlock(data)
		return nil, errors.New("secret payload changed")
	}
	return &SecretBuffer{data: data}, nil
}

func (b *SecretBuffer) Close() {
	if b == nil || b.closed {
		return
	}
	b.closed = true
	if len(b.data) != 0 {
		zeroBytes(b.data)
		_ = syscall.Munlock(b.data)
	}
}

func (b *SecretBuffer) Format(state fmt.State, verb rune) {
	io.WriteString(state, "<secret>")
}

func (b *SecretBuffer) cloneSealedMemfd(name string, maximum int) (int, error) {
	if b == nil || b.closed || len(b.data) == 0 {
		return -1, errors.New("secret unavailable")
	}
	return createSealedMemfd(name, b.data, maximum)
}

func validateSealedMemfd(fd int) error {
	flags, err := fcntlInt(fd, syscall.F_GETFD, 0)
	if err != nil {
		return err
	}
	if flags&syscall.FD_CLOEXEC == 0 {
		return errors.New("secret descriptor missing cloexec")
	}
	var statValue syscall.Stat_t
	if err := syscall.Fstat(fd, &statValue); err != nil {
		return err
	}
	if statValue.Nlink != 0 || statValue.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return errors.New("secret descriptor must be anonymous regular file")
	}
	seals, err := fcntlInt(fd, fGetSeals, 0)
	if err != nil {
		return err
	}
	if seals != requiredMemfdSeals {
		return errors.New("secret descriptor seals invalid")
	}
	return nil
}

func setSecretProcessProtections() error {
	if _, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, uintptr(prSetDumpable), 0, 0, 0, 0, 0); errno != 0 {
		return errno
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_CORE, &limit); err != nil {
		return err
	}
	limit.Cur = 0
	limit.Max = 0
	return syscall.Setrlimit(syscall.RLIMIT_CORE, &limit)
}

func dumpableState() (bool, error) {
	value, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, uintptr(prGetDumpable), 0, 0, 0, 0, 0)
	if errno != 0 {
		return false, errno
	}
	return value != 0, nil
}

func coreDumpsDisabled() bool {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_CORE, &limit); err != nil {
		return false
	}
	return limit.Cur == 0 && limit.Max == 0
}

func createSealedMemfd(name string, payload []byte, maximum int) (int, error) {
	if name == "" || len(payload) == 0 || len(payload) > maximum {
		return -1, errors.New("secret payload invalid")
	}
	fd, err := memfdCreate(name, memfdCloexec|memfdAllowSealing)
	if err != nil {
		return -1, err
	}
	complete := false
	defer func() {
		if !complete {
			syscall.Close(fd)
		}
	}()
	written := 0
	for written < len(payload) {
		n, err := syscall.Write(fd, payload[written:])
		if err != nil {
			return -1, err
		}
		if n <= 0 {
			return -1, errors.New("memfd write failed")
		}
		written += n
	}
	if err := syscall.Fsync(fd); err != nil {
		return -1, err
	}
	if _, err := fcntlInt(fd, fAddSeals, requiredMemfdSeals); err != nil {
		return -1, err
	}
	if err := validateSealedMemfd(fd); err != nil {
		return -1, err
	}
	complete = true
	return fd, nil
}

func memfdCreate(name string, flags int) (int, error) {
	pointer, err := syscall.BytePtrFromString(name)
	if err != nil {
		return -1, err
	}
	fd, _, errno := syscall.Syscall(memfdCreateTrap(), uintptr(unsafe.Pointer(pointer)), uintptr(flags), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

func zeroBytes(data []byte) {
	for index := range data {
		data[index] = 0
	}
	runtime.KeepAlive(data)
}

func createMemfdFixture(t interface {
	Helper()
	Fatalf(string, ...any)
}, name string, payload []byte, seals int, cloexec bool) int {
	t.Helper()
	flags := memfdAllowSealing
	if cloexec {
		flags |= memfdCloexec
	}
	fd, err := memfdCreate(name, flags)
	if err != nil {
		t.Fatalf("memfdCreate() error = %v", err)
	}
	if _, err := syscall.Write(fd, payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if seals != 0 {
		if _, err := fcntlInt(fd, fAddSeals, seals); err != nil {
			t.Fatalf("FcntlInt(F_ADD_SEALS) error = %v", err)
		}
	}
	return fd
}

func countOpenFileDescriptors(t interface {
	Helper()
	Fatalf(string, ...any)
}) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("ReadDir(/proc/self/fd) error = %v", err)
	}
	return len(entries)
}

func dupFileDescriptor(t interface {
	Helper()
	Fatalf(string, ...any)
}, fd int) int {
	t.Helper()
	duplicated, err := syscall.Dup(fd)
	if err != nil {
		t.Fatalf("Dup() error = %v", err)
	}
	if _, err := fcntlInt(duplicated, syscall.F_SETFD, syscall.FD_CLOEXEC); err != nil {
		t.Fatalf("FcntlInt(F_SETFD) error = %v", err)
	}
	return duplicated
}

func runtimeSessionArch() string {
	return runtime.GOARCH
}

func secretSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func fcntlInt(fd int, cmd int, arg int) (int, error) {
	value, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
	if errno != 0 {
		return 0, errno
	}
	return int(value), nil
}

func memfdCreateTrap() uintptr {
	switch runtime.GOARCH {
	case "amd64":
		return 319
	case "arm64":
		return 279
	default:
		panic("memfd_create unsupported on this architecture")
	}
}
