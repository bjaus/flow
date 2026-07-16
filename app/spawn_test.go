package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bjaus/flow"
	"github.com/stretchr/testify/require"
)

// gatedChildWorkflow suspends at a human gate, so a parent awaiting it stays
// visibly parked until an operator decides.
func gatedChildWorkflow() AnyWorkflow {
	return flow.Define("child-gated", "waits at a gate", flow.Then(
		flow.Do("double", func(_ context.Context, in int) (int, error) { return in * 2, nil }),
		flow.Human("approve", func(v int, _ flow.Decision) int { return v }, func(int) string { return "approve?" }),
	))
}

// awaitingParent spawns one gated child and awaits it, adding 1 to its result.
func awaitingParent() AnyWorkflow {
	return flow.Define("parent", "spawns child", flow.Do("compose", func(ctx context.Context, in int) (int, error) {
		res, err := SpawnAwait(ctx, "child-gated", in)
		if err != nil {
			return 0, err
		}
		var v int
		if err := json.Unmarshal(res, &v); err != nil {
			return 0, err
		}
		return v + 1, nil
	}))
}

func waitChild(t *testing.T, s *Stores, parentID string, status Status) *Run {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		children, err := s.Runs.List(context.Background(), RunFilter{ParentID: parentID, Status: status})
		require.NoError(t, err)
		if len(children) == 1 {
			return children[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no child of %s reached %s", parentID, status)
	return nil
}

func TestAwaitSuspendsParentAndResumesWithChildResult(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(gatedChildWorkflow()))
	require.NoError(t, a.Register(awaitingParent()))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "parent", json.RawMessage(`21`))
	require.NoError(t, err)
	// The parent suspends durably instead of holding the worker: it is
	// visibly awaiting_child, pointing at the gated child, with no gate
	// prompt of its own (it must not look like a human gate).
	parent := waitRun(t, s, id, StatusAwaitingChild)
	child := waitChild(t, s, id, StatusAwaitingReview)
	require.Equal(t, child.ID, parent.WaitingOn)
	require.Empty(t, parent.GatePrompt)
	require.NotEmpty(t, parent.InterruptID)
	require.Equal(t, "child-gated", child.Workflow)
	require.Equal(t, id, child.ParentID)
	// Deciding the child completes it; the daemon re-enqueues the parent,
	// which resumes at Await with the child's result.
	require.NoError(t, a.Decide(context.Background(), child.ID, Decision{Approved: true}))
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `43`, string(run.Result))
	require.Empty(t, run.WaitingOn)
	require.Equal(t, StatusSucceeded, waitRun(t, s, child.ID, StatusSucceeded).Status)
	// A run that is not a child never matches a parent filter.
	orphans, err := s.Runs.List(context.Background(), RunFilter{ParentID: child.ID})
	require.NoError(t, err)
	require.Empty(t, orphans)
}

func TestAwaitSurvivesRestartWhileParentAwaits(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(gatedChildWorkflow()))
	require.NoError(t, a.Register(awaitingParent()))
	stop := serve(t, a)
	id, err := a.Trigger(context.Background(), "parent", json.RawMessage(`21`))
	require.NoError(t, err)
	waitRun(t, s, id, StatusAwaitingChild)
	child := waitChild(t, s, id, StatusAwaitingReview)
	stop()
	// A new App over the same store: the parent must stay awaiting_child
	// through the restart and resume once the child completes here.
	b, err := New(Config{Store: s, Provider: FakeProvider(nil), Listen: "127.0.0.1:0", DrainTimeout: 2 * time.Second})
	require.NoError(t, err)
	require.NoError(t, b.Register(gatedChildWorkflow()))
	require.NoError(t, b.Register(awaitingParent()))
	serve(t, b)
	require.Equal(t, StatusAwaitingChild, waitRun(t, s, id, StatusAwaitingChild).Status)
	require.NoError(t, b.Decide(context.Background(), child.ID, Decision{Approved: true}))
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `43`, string(run.Result))
}

func TestAwaitRecoveryResumesParentWhenChildFinishedWhileDown(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(gatedChildWorkflow()))
	require.NoError(t, a.Register(awaitingParent()))
	stop := serve(t, a)
	id, err := a.Trigger(context.Background(), "parent", json.RawMessage(`21`))
	require.NoError(t, err)
	waitRun(t, s, id, StatusAwaitingChild)
	child := waitChild(t, s, id, StatusAwaitingReview)
	stop()
	// The child reaches terminal while no daemon is up: recovery must
	// re-enqueue the awaiting parent immediately on boot.
	now := time.Now().UTC()
	child.Status, child.Result, child.FinishedAt = StatusSucceeded, json.RawMessage(`42`), &now
	require.NoError(t, s.Runs.Save(context.Background(), child))
	b, err := New(Config{Store: s, Provider: FakeProvider(nil), Listen: "127.0.0.1:0", DrainTimeout: 2 * time.Second})
	require.NoError(t, err)
	require.NoError(t, b.Register(gatedChildWorkflow()))
	require.NoError(t, b.Register(awaitingParent()))
	serve(t, b)
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `43`, string(run.Result))
}

func TestAwaitPropagatesChildFailure(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(flow.Define("child-boom", "fails", flow.Do("boom", func(_ context.Context, _ int) (int, error) {
		return 0, context.DeadlineExceeded
	}))))
	require.NoError(t, a.Register(flow.Define("parent-of-boom", "spawns failing child", flow.Do("compose", func(ctx context.Context, in int) (int, error) {
		_, err := SpawnAwait(ctx, "child-boom", in)
		return 0, err
	}))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "parent-of-boom", json.RawMessage(`1`))
	require.NoError(t, err)
	run := waitRun(t, s, id, StatusFailed)
	require.Contains(t, run.Error, "child run")
	require.Contains(t, run.Error, "failed")
	children, err := s.Runs.List(context.Background(), RunFilter{ParentID: id})
	require.NoError(t, err)
	require.Len(t, children, 1)
	require.Equal(t, StatusFailed, children[0].Status)
}

func TestCancelParentCancelsChildren(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(gatedChildWorkflow()))
	require.NoError(t, a.Register(flow.Define("parent-canceled", "spawns then awaits gated children", flow.Do("compose", func(ctx context.Context, in int) (int, error) {
		spawner, ok := SpawnerFrom(ctx)
		if !ok {
			return 0, context.Canceled
		}
		// One child never awaited, one awaited: both suspend at their gate.
		if _, err := spawner.Spawn(ctx, "child-gated", in); err != nil {
			return 0, err
		}
		res, err := SpawnAwait(ctx, "child-gated", in+1)
		if err != nil {
			return 0, err
		}
		var v int
		return v, json.Unmarshal(res, &v)
	}))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "parent-canceled", json.RawMessage(`7`))
	require.NoError(t, err)
	waitRun(t, s, id, StatusAwaitingChild)
	// Wait until both children suspend at their gates.
	deadline := time.Now().Add(4 * time.Second)
	for {
		children, err := s.Runs.List(context.Background(), RunFilter{ParentID: id, Status: StatusAwaitingReview})
		require.NoError(t, err)
		if len(children) == 2 {
			break
		}
		require.True(t, time.Now().Before(deadline), "gated children never reached their gates")
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, a.Cancel(context.Background(), id))
	waitRun(t, s, id, StatusCanceled)
	children, err := s.Runs.List(context.Background(), RunFilter{ParentID: id})
	require.NoError(t, err)
	require.Len(t, children, 2)
	for _, child := range children {
		require.Equal(t, StatusCanceled, waitRun(t, s, child.ID, StatusCanceled).Status)
	}
}

func TestSpawnDepthGuardTrips(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(flow.Define("recurse", "spawns itself", flow.Do("deeper", func(ctx context.Context, in int) (int, error) {
		res, err := SpawnAwait(ctx, "recurse", in+1)
		if err != nil {
			if strings.Contains(err.Error(), "max spawn depth") {
				// The depth guard tripped: report how deep this run is.
				return in, nil
			}
			// Any other error — including the Await suspension — propagates.
			return 0, err
		}
		var v int
		return v, json.Unmarshal(res, &v)
	}))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "recurse", json.RawMessage(`0`))
	require.NoError(t, err)
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `8`, string(run.Result))
}
