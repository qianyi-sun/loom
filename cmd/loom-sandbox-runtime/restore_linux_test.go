//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const restoreStage = ".loom-restore-0123456789abcdef0123456789abcdef"

func restoreRequest(t *testing.T, root, stage string) int {
	t.Helper()
	data, _ := json.Marshal(map[string]string{"root": root, "stage": stage})
	response, err := testClient(t).Post("http://sandbox/restore-directory", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestRestoreDirectoryPromotesLinksAndPreservesRootMetadata(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, restoreStage)
	if err := os.Mkdir(stage, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "deleted"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(stage, "content")
	if err := os.WriteFile(file, []byte("new"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(file, filepath.Join(stage, "hard")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("content", filepath.Join(stage, "link")); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1_700_000_000, 123456789)
	if err := os.Chtimes(stage, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if status := restoreRequest(t, root, restoreStage); status != http.StatusNoContent {
		t.Fatalf("restore status %d", status)
	}
	if data, err := os.ReadFile(filepath.Join(root, "link")); err != nil || string(data) != "new" {
		t.Fatalf("restored link: %q %v", data, err)
	}
	first, _ := os.Stat(filepath.Join(root, "content"))
	second, _ := os.Stat(filepath.Join(root, "hard"))
	if first == nil || second == nil || !os.SameFile(first, second) || first.Mode().Perm() != 0640 {
		t.Fatal("hardlink identity or file mode lost")
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0750 || !info.ModTime().Equal(stamp) {
		t.Fatalf("root metadata lost: %v %v", info, err)
	}
	for _, name := range []string{"deleted", restoreStage} {
		if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("unexpected remaining entry %s: %v", name, err)
		}
	}
}

func TestRestoreDirectoryRejectsInvalidStageBeforeDeletingBaseline(t *testing.T) {
	for _, kind := range []string{"missing", "file", "symlink", "self-collision", "invalid-name"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			baseline := filepath.Join(root, "baseline")
			if err := os.WriteFile(baseline, []byte("unchanged"), 0600); err != nil {
				t.Fatal(err)
			}
			name := restoreStage
			stage := filepath.Join(root, name)
			switch kind {
			case "file":
				if err := os.WriteFile(stage, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(t.TempDir(), stage); err != nil {
					t.Fatal(err)
				}
			case "self-collision":
				if err := os.Mkdir(stage, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(stage, name), 0700); err != nil {
					t.Fatal(err)
				}
			case "invalid-name":
				name = "../outside"
			}
			if status := restoreRequest(t, root, name); status == http.StatusNoContent {
				t.Fatal("invalid stage was accepted")
			}
			if data, err := os.ReadFile(baseline); err != nil || string(data) != "unchanged" {
				t.Fatalf("baseline changed: %q %v", data, err)
			}
		})
	}
}

func TestRestoreDirectoryRejectsSymlinkAncestorsAndProtectedRoots(t *testing.T) {
	parent := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(outside, restoreStage), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(parent, "link")); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"/", "/tmp", "/proc", "/proc/self", "/opt", "/opt/verifier", "/loom", "/tests", parent + "/../escape", filepath.Join(parent, "link")} {
		if status := restoreRequest(t, root, restoreStage); status == http.StatusNoContent {
			t.Fatalf("unsafe root accepted: %s", root)
		}
	}
	if _, err := os.Stat(filepath.Join(outside, restoreStage)); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreDirectoryDoesNotTraverseOldSymlinkLeaves(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "keep"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "old-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, restoreStage), 0700); err != nil {
		t.Fatal(err)
	}
	if status := restoreRequest(t, root, restoreStage); status != http.StatusNoContent {
		t.Fatalf("status %d", status)
	}
	if data, err := os.ReadFile(filepath.Join(outside, "keep")); err != nil || string(data) != "private" {
		t.Fatalf("outside changed: %q %v", data, err)
	}
}

func TestRestoreDirectoryPreservesRootOwnership(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("ownership change requires root")
	}
	root := t.TempDir()
	stage := filepath.Join(root, restoreStage)
	if err := os.Mkdir(stage, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(stage, 1234, 1235); err != nil {
		t.Fatal(err)
	}
	if status := restoreRequest(t, root, restoreStage); status != http.StatusNoContent {
		t.Fatalf("status %d", status)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	if stat.Uid != 1234 || stat.Gid != 1235 {
		t.Fatalf("ownership %d:%d", stat.Uid, stat.Gid)
	}
}

func TestRestoreDirectoryDoesNotRequireWritableParent(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("requires unprivileged permission enforcement")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "owned")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, restoreStage), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0700) })
	if status := restoreRequest(t, root, restoreStage); status != http.StatusNoContent {
		t.Fatalf("status %d", status)
	}
}

func TestRestoreDirectoryReadOnlyStageRetainsFinalPermissions(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("requires unprivileged permission enforcement")
	}
	root := t.TempDir()
	stage := filepath.Join(root, restoreStage)
	if err := os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "content"), []byte("read only"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stage, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(root, 0700); os.Chmod(stage, 0700) })
	if err := replaceDirectory(root, restoreStage); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0555 {
		t.Fatalf("mode changed: %v %v", info, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "content")); err != nil || string(data) != "read only" {
		t.Fatalf("content changed: %q %v", data, err)
	}
}

func TestRestoreDirectoryOnlyRequiresSearchableAncestors(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("requires unprivileged permission enforcement")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "owned")
	stage := filepath.Join(root, restoreStage)
	if err := os.MkdirAll(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "content"), []byte("restored"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0111); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0700) })
	if err := replaceDirectory(root, restoreStage); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "content")); err != nil || string(data) != "restored" {
		t.Fatalf("content changed: %q %v", data, err)
	}
}
