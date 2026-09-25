package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNetworkFilesAreIndependentCopiesForEachPrivateSandbox(t *testing.T) {
	source, root := t.TempDir(), t.TempDir()
	roles := []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}, {RoleName: "verifier-sandbox", PrivateSandbox: true}}
	for _, name := range []string{"hosts", "resolv.conf"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte("original "+name+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, role := range roles {
		if err := os.Mkdir(filepath.Join(root, role.RoleName), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := materializeNetworkFiles(roles, root, source); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hosts", "resolv.conf"} {
		path := filepath.Join(root, "task-sandbox", "network", name)
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o644 {
			t.Fatalf("unexpected private file mode: %v %v", info, err)
		}
		if err := os.WriteFile(path, []byte("task mutation"), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, other := range []string{filepath.Join(source, name), filepath.Join(root, "verifier-sandbox", "network", name)} {
			body, err := os.ReadFile(other)
			if err != nil || string(body) != "original "+name+"\n" {
				t.Fatalf("task changed another container's %s: %q %v", other, body, err)
			}
		}
	}
	if err := materializeNetworkFiles(roles, root, source); err == nil {
		t.Fatal("repeated initialization overwrote task state")
	}
}

func TestNetworkFileInitializationRejectsPreexistingOrLinkedDestinations(t *testing.T) {
	for _, mutation := range []string{"root-link", "role-link", "network-link", "network-existing", "missing-role"} {
		t.Run(mutation, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			role := filepath.Join(root, "task-sandbox")
			var err error
			switch mutation {
			case "root-link":
				link := filepath.Join(t.TempDir(), "root")
				err = os.Symlink(root, link)
				root = link
			case "role-link":
				err = os.Symlink(outside, role)
			case "network-link", "network-existing":
				if err = os.Mkdir(role, 0o755); err == nil {
					if mutation == "network-link" {
						err = os.Symlink(outside, filepath.Join(role, "network"))
					} else {
						err = os.Mkdir(filepath.Join(role, "network"), 0o755)
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := materializeNetworkFiles([]sidecar{{RoleName: "task-sandbox", PrivateSandbox: true}}, root, "/etc"); err == nil {
				t.Fatal("unsafe or missing private volume accepted")
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Fatalf("initialization modified linked directory: %v %v", entries, err)
			}
		})
	}
	if err := materializeNetworkFiles([]sidecar{{RoleName: "fixture"}}, "/absent", "/absent"); err != nil {
		t.Fatalf("ordinary sidecar unexpectedly requires private network files: %v", err)
	}
}
