package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateSandboxIdentityStrictRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name string
		uid, gid int64
		home string
		private bool
		valid bool
	}{
		{"root", 0, 0, "/root", true, true},
		{"numeric", 1001, 1002, "/home/miles", true, true},
		{"negative", -1, 0, "/root", true, false},
		{"traversal", 0, 0, "/root/../tests", true, false},
		{"protected", 0, 0, "/loom/private", true, false},
		{"ordinary sidecar", 0, 0, "/root", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, _ := json.Marshal(isolatedControllerPlan())
			var raw map[string]any
			if err := json.Unmarshal(body, &raw); err != nil { t.Fatal(err) }
			sidecar := raw["sidecars"].([]any)[0].(map[string]any)
			sidecar["identity"] = map[string]any{"run_as_user": test.uid, "run_as_group": test.gid, "home": test.home}
			if !test.private { sidecar["private_sandbox"] = false; sidecar["role_name"] = "database" }
			body, _ = json.Marshal(raw)
			path := filepath.Join(t.TempDir(), "plan.json")
			if err := os.WriteFile(path, body, 0600); err != nil { t.Fatal(err) }
			loaded, err := loadPlan(path)
			if (err == nil) != test.valid { t.Fatalf("valid=%v, got %v", test.valid, err) }
			if err == nil {
				got, err := json.Marshal(loaded)
				if err != nil { t.Fatal(err) }
				var roundTrip map[string]any
				if err := json.Unmarshal(got, &roundTrip); err != nil { t.Fatal(err) }
				identity := roundTrip["sidecars"].([]any)[0].(map[string]any)["identity"].(map[string]any)
				if identity["run_as_user"] != float64(test.uid) || identity["home"] != test.home { t.Fatal("identity changed") }
			}
		})
	}
}
