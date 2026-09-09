package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOrchestratorJoinsBuildWrapperBeforeAllocationCleanup(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false:"joined", true:"timeout"}[timeout], func(t *testing.T) {
			h := newOrchestratorHarness(t)
			started, release, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
			h.executor.blockBuild = func(context.Context, string) (OCIOutput, error) {
				close(started)
				<-release
				close(joined)
				return OCIOutput{}, context.Canceled
			}
			o := h.orchestrator()
			o.CleanupGrace = 100*time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- o.Run(ctx) }()
			<-started
			cancel()
			<-h.executor.closed()
			if timeout {
				select {
				case err := <-done:
					if !errors.Is(err, errCleanupAmbiguous) { t.Error("unjoined build did not report cleanup ambiguity") }
				case <-time.After(time.Second): t.Error("join timeout did not return")
				}
				for _, event := range h.events {
					if event == "caps_close" || event == "finish" { t.Errorf("unjoined build reported/released cleanup: %s", event) }
				}
				close(release)
			} else {
				select { case <-done: t.Error("orchestrator returned before build joined"); case <-time.After(20*time.Millisecond): }
				close(release)
				select { case <-done: case <-time.After(time.Second): t.Error("joined build did not return") }
			}
			<-joined
		})
	}
}
