package main

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"testing"
	"unsafe"
)

func secretPageLocked(t *testing.T, address uintptr) bool {
	t.Helper()
	payload, err := os.ReadFile("/proc/self/smaps")
	if err != nil { t.Fatal(err) }
	selected := false
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 1 && strings.Contains(fields[0], "-") {
			ends := strings.SplitN(fields[0], "-", 2)
			low, e1 := strconv.ParseUint(ends[0], 16, 64)
			high, e2 := strconv.ParseUint(ends[1], 16, 64)
			if e1 == nil && e2 == nil { selected = uint64(address) >= low && uint64(address) < high }
		}
		if selected && strings.HasPrefix(line, "VmFlags:") {
			for _, flag := range fields[1:] { if flag == "lo" { return true } }
			return false
		}
	}
	return false
}

func TestSecretMappingsKeepSurvivingSnapshotsLocked(t *testing.T) {
	// Small session-sized payloads previously shared a Go heap page. Closing
	// one mlocked slice then unlocked another; Linux locks are not refcounted.
	var live []*SecretBuffer
	defer func(){ for _, b := range live { b.Close() } }()
	pages := map[uintptr]bool{}
	for i := 0; i < 16; i++ {
		payload := bytes.Repeat([]byte{'x'}, 512)
		fd := createMemfdFixture(t, "mapping-isolation", payload, requiredMemfdSeals, true)
		b, err := NewSecretBuffer(fd, 4096)
		if err != nil { t.Fatal(err) }
		live = append(live, b)
		address := uintptr(unsafe.Pointer(&b.data[0]))
		page := address / uintptr(os.Getpagesize())
		if pages[page] { t.Fatal("independently owned secrets share a page") }
		pages[page] = true
		if !secretPageLocked(t, address) { t.Fatal("live secret not locked") }
	}
	for i := 0; i < len(live); i += 2 { live[i].Close() }
	for i := 1; i < len(live); i += 2 {
		if !secretPageLocked(t, uintptr(unsafe.Pointer(&live[i].data[0]))) { t.Fatal("closing predecessor unlocked live secret") }
		if !bytes.Equal(live[i].data, bytes.Repeat([]byte{'x'},512)) { t.Fatal("surviving secret damaged") }
	}
}
