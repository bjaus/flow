package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bjaus/flow/app"
	"github.com/stretchr/testify/require"
)

func open(t *testing.T) *app.Stores {
	t.Helper()
	s, err := app.SQLite(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestCheckpointLifecycle(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	_, ok, err := s.Checkpoint.Get(ctx, "missing")
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, s.Checkpoint.Set(ctx, "a", []byte("one")))
	got, ok, err := s.Checkpoint.Get(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), got)
	got[0] = 'x'
	again, _, _ := s.Checkpoint.Get(ctx, "a")
	require.Equal(t, []byte("one"), again)
	require.NoError(t, s.Checkpoint.Set(ctx, "a", []byte("two")))
	got, ok, err = s.Checkpoint.Get(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("two"), got)
	require.NoError(t, s.Checkpoint.Delete(ctx, "a"))
	_, ok, err = s.Checkpoint.Get(ctx, "a")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRunsCRUDFilterAndFIFOClaim(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Minute)
	runs := []*app.Run{
		{ID: "1", Workflow: "alpha", Status: app.StatusQueued, Input: json.RawMessage(`1`), CreatedAt: now},
		{ID: "2", Workflow: "beta", Status: app.StatusQueued, Input: json.RawMessage(`2`), CreatedAt: now.Add(time.Second)},
		{ID: "3", Workflow: "alpha", Status: app.StatusSucceeded, Input: json.RawMessage(`3`), Result: json.RawMessage(`4`), CreatedAt: now.Add(2 * time.Second)},
	}
	for _, r := range runs {
		require.NoError(t, s.Runs.Save(ctx, r))
	}
	got, err := s.Runs.Get(ctx, "3")
	require.NoError(t, err)
	require.Equal(t, app.StatusSucceeded, got.Status)
	require.JSONEq(t, `4`, string(got.Result))
	filtered, err := s.Runs.List(ctx, app.RunFilter{Workflow: "alpha"})
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	filtered, err = s.Runs.List(ctx, app.RunFilter{Status: app.StatusQueued})
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, []string{filtered[0].ID, filtered[1].ID})
	claimed, err := s.Runs.Claim(ctx)
	require.NoError(t, err)
	require.Equal(t, "1", claimed.ID)
	require.Equal(t, app.StatusRunning, claimed.Status)
	require.NotNil(t, claimed.StartedAt)
	claimed, err = s.Runs.Claim(ctx)
	require.NoError(t, err)
	require.Equal(t, "2", claimed.ID)
	claimed, err = s.Runs.Claim(ctx)
	require.NoError(t, err)
	require.Nil(t, claimed)
}

func TestClaimIsAtomicAcrossCallers(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		require.NoError(t, s.Runs.Save(ctx, &app.Run{ID: string(rune('a' + i)), Workflow: "w", Status: app.StatusQueued, Input: json.RawMessage(`null`), CreatedAt: time.Now().Add(time.Duration(i) * time.Millisecond)}))
	}
	var wg sync.WaitGroup
	ids := make(chan string, 40)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, err := s.Runs.Claim(ctx)
				require.NoError(t, err)
				if r == nil {
					return
				}
				ids <- r.ID
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		require.False(t, seen[id])
		seen[id] = true
	}
	require.Len(t, seen, 20)
}

func TestEventsReplayThenLiveWithIndependentSequences(t *testing.T) {
	s := open(t)
	s.Events.Publish(app.Event{RunID: "a", Kind: app.EventRunStarted})
	s.Events.Publish(app.Event{RunID: "b", Kind: app.EventRunStarted})
	s.Events.Publish(app.Event{RunID: "a", Kind: app.EventAgentToken})
	ch, cancel := s.Events.Subscribe("a")
	defer cancel()
	first := <-ch
	second := <-ch
	require.Equal(t, int64(1), first.Seq)
	require.Equal(t, int64(2), second.Seq)
	s.Events.Publish(app.Event{RunID: "a", Kind: app.EventRunFinished})
	select {
	case live := <-ch:
		require.Equal(t, int64(3), live.Seq)
		require.Equal(t, app.EventRunFinished, live.Kind)
	case <-time.After(time.Second):
		t.Fatal("live event not delivered")
	}
	all, stop := s.Events.Subscribe("")
	defer stop()
	require.Equal(t, "a", (<-all).RunID)
	require.Equal(t, "b", (<-all).RunID)
	require.Equal(t, "a", (<-all).RunID)
}
