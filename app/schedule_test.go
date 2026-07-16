package app

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bjaus/flow"
	"github.com/stretchr/testify/require"
)

func scheduledApp(t *testing.T, triggers []Trigger) (*App, *Stores, chan time.Time) {
	t.Helper()
	stores, err := SQLite(filepath.Join(t.TempDir(), "flow.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.Close()) })
	a, err := New(Config{Store: stores, Provider: FakeProvider(nil), Listen: "127.0.0.1:0", DrainTimeout: 2 * time.Second, Triggers: triggers})
	require.NoError(t, err)
	tick := make(chan time.Time)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a.clock = func() time.Time { return base }
	a.timer = func(time.Duration) <-chan time.Time { return tick }
	return a, stores, tick
}

func doubleWorkflow() AnyWorkflow {
	return flow.Define("double", "doubles", flow.Do("double", func(_ context.Context, in int) (int, error) { return in * 2, nil }))
}

func waitTriggerRuns(t *testing.T, a *App, workflow string, n int) []*Run {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := a.ListRuns(context.Background(), RunFilter{Workflow: workflow})
		require.NoError(t, err)
		if len(runs) >= n {
			return runs
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d runs of %q", n, workflow)
	return nil
}

func waitEvent(t *testing.T, events <-chan Event, kind EventKind) Event {
	t.Helper()
	deadline := time.After(4 * time.Second)
	for {
		select {
		case e := <-events:
			if e.Kind == kind {
				return e
			}
		case <-deadline:
			t.Fatalf("expected %s event", kind)
		}
	}
}

func TestTriggerValidationAtNew(t *testing.T) {
	_, err := New(Config{Triggers: []Trigger{{Workflow: "double", Spec: "not a cron spec"}}})
	require.ErrorContains(t, err, "invalid cron spec")
	_, err = New(Config{Triggers: []Trigger{{Workflow: ""}}})
	require.ErrorContains(t, err, "workflow is required")
	_, err = New(Config{Triggers: []Trigger{{Name: "n", Workflow: "a", Spec: "* * * * *"}, {Name: "n", Workflow: "b", Spec: "* * * * *"}}})
	require.ErrorContains(t, err, "duplicate trigger name")
}

func TestTriggerValidationAtServe(t *testing.T) {
	a, _, _ := scheduledApp(t, []Trigger{{Workflow: "missing", Spec: "* * * * *"}})
	err := a.Serve(context.Background())
	require.ErrorContains(t, err, `workflow "missing" is not registered`)

	a, _, _ = scheduledApp(t, []Trigger{{Workflow: "double", Spec: "* * * * *", Input: json.RawMessage(`"nope"`)}})
	require.NoError(t, a.Register(doubleWorkflow()))
	err = a.Serve(context.Background())
	require.ErrorContains(t, err, "input does not decode")
}

func TestScheduledTriggerEnqueuesAttributedRun(t *testing.T) {
	a, s, tick := scheduledApp(t, []Trigger{{Name: "nightly", Workflow: "double", Spec: "0 3 * * *", Input: json.RawMessage(`21`)}})
	require.NoError(t, a.Register(doubleWorkflow()))
	serve(t, a)
	tick <- time.Time{}
	runs := waitTriggerRuns(t, a, "double", 1)
	require.Equal(t, "nightly", runs[0].Trigger)
	run := waitRun(t, s, runs[0].ID, StatusSucceeded)
	require.JSONEq(t, `42`, string(run.Result))
}

func TestScheduledTriggerSkipsWhilePreviousRunActive(t *testing.T) {
	release := make(chan struct{})
	a, s, tick := scheduledApp(t, []Trigger{{Name: "hourly", Workflow: "slow", Spec: "0 * * * *", Input: json.RawMessage(`1`)}})
	wf := flow.Define("slow", "blocks until released", flow.Do("wait", func(ctx context.Context, in int) (int, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}))
	require.NoError(t, a.Register(wf))
	events, cancel := s.Events.Subscribe("")
	defer cancel()
	serve(t, a)
	tick <- time.Time{}
	first := waitTriggerRuns(t, a, "slow", 1)[0]
	waitRun(t, s, first.ID, StatusRunning)
	tick <- time.Time{}
	e := waitEvent(t, events, EventTriggerSkipped)
	require.JSONEq(t, `{"trigger":"hourly","workflow":"slow","reason":"previous run is still active"}`, string(e.Data))
	close(release)
	waitRun(t, s, first.ID, StatusSucceeded)
	tick <- time.Time{}
	runs := waitTriggerRuns(t, a, "slow", 2)
	require.Len(t, runs, 2)
}

func TestScheduledTriggerSuppressedWhileDraining(t *testing.T) {
	a, s, _ := scheduledApp(t, []Trigger{{Name: "nightly", Workflow: "double", Spec: "0 3 * * *"}})
	require.NoError(t, a.Register(doubleWorkflow()))
	events, cancel := s.Events.Subscribe("")
	defer cancel()
	a.draining.Store(true)
	a.fire(context.Background(), a.schedules[0].trigger)
	e := waitEvent(t, events, EventTriggerSkipped)
	require.JSONEq(t, `{"trigger":"nightly","workflow":"double","reason":"daemon is draining"}`, string(e.Data))
	runs, err := a.ListRuns(context.Background(), RunFilter{Workflow: "double"})
	require.NoError(t, err)
	require.Empty(t, runs)
}
