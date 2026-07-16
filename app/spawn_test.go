package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bjaus/flow"
	"github.com/stretchr/testify/require"
)

func childWorkflow() AnyWorkflow {
	return flow.Define("child-double", "doubles", flow.Do("double", func(_ context.Context, in int) (int, error) { return in * 2, nil }))
}

func TestSpawnAwaitDrivesChildInlineAndPersistsParentID(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(childWorkflow()))
	parent := flow.Define("parent", "spawns child", flow.Do("compose", func(ctx context.Context, in int) (int, error) {
		res, err := SpawnAwait(ctx, "child-double", in)
		if err != nil {
			return 0, err
		}
		var v int
		if err := json.Unmarshal(res, &v); err != nil {
			return 0, err
		}
		return v + 1, nil
	}))
	require.NoError(t, a.Register(parent))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "parent", json.RawMessage(`21`))
	require.NoError(t, err)
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `43`, string(run.Result))
	children, err := s.Runs.List(context.Background(), RunFilter{ParentID: id})
	require.NoError(t, err)
	require.Len(t, children, 1)
	require.Equal(t, "child-double", children[0].Workflow)
	require.Equal(t, id, children[0].ParentID)
	require.Equal(t, StatusSucceeded, children[0].Status)
	require.JSONEq(t, `42`, string(children[0].Result))
	// A run that is not a child never matches a parent filter.
	orphans, err := s.Runs.List(context.Background(), RunFilter{ParentID: children[0].ID})
	require.NoError(t, err)
	require.Empty(t, orphans)
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
	require.NoError(t, a.Register(childWorkflow()))
	require.NoError(t, a.Register(flow.Define("child-gated", "waits at a gate", flow.Then(
		flow.Do("noop", func(_ context.Context, in int) (int, error) { return in, nil }),
		flow.Human("approve", func(v int, _ flow.Decision) int { return v }, func(v int) string { return "approve?" }),
	))))
	require.NoError(t, a.Register(flow.Define("parent-canceled", "spawns then blocks on a gated child", flow.Do("compose", func(ctx context.Context, in int) (int, error) {
		s, ok := SpawnerFrom(ctx)
		if !ok {
			return 0, context.Canceled
		}
		// One child never awaited: it stays queued behind the busy worker.
		if _, err := s.Spawn(ctx, "child-double", in); err != nil {
			return 0, err
		}
		// One gated child awaited inline: Await parks at its human gate.
		res, err := SpawnAwait(ctx, "child-gated", in)
		if err != nil {
			return 0, err
		}
		var v int
		return v, json.Unmarshal(res, &v)
	}))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "parent-canceled", json.RawMessage(`7`))
	require.NoError(t, err)
	// Wait until the gated child suspends at its gate, so both children exist.
	deadline := time.Now().Add(4 * time.Second)
	for {
		children, err := s.Runs.List(context.Background(), RunFilter{ParentID: id, Status: StatusAwaitingReview})
		require.NoError(t, err)
		if len(children) == 1 {
			break
		}
		require.True(t, time.Now().Before(deadline), "gated child never reached its gate")
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
			// The depth guard tripped: report how deep this run is.
			return in, nil
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
