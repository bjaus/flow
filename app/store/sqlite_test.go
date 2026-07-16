package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bjaus/flow/app"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, err := s.Runs.Claim(ctx)
				if err != nil {
					errs <- err
					return
				}
				if r == nil {
					return
				}
				ids <- r.ID
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	seen := map[string]bool{}
	for id := range ids {
		require.False(t, seen[id])
		seen[id] = true
	}
	require.Len(t, seen, 20)
}

func TestEventsFromAnotherStoreInstanceReachLiveSubscriber(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	first, err := app.SQLite(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	second, err := app.SQLite(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()
	ch, cancel := first.Events.Subscribe("shared")
	defer cancel()
	second.Events.Publish(app.Event{RunID: "shared", Kind: app.EventRunStarted})
	select {
	case event := <-ch:
		require.Equal(t, app.EventRunStarted, event.Kind)
	case <-time.After(time.Second):
		t.Fatal("cross-instance event was not observed")
	}
}

func TestLargeEventHistoryDoesNotBlockSubscription(t *testing.T) {
	s := open(t)
	for i := 0; i < 600; i++ {
		s.Events.Publish(app.Event{RunID: "large", Kind: app.EventAgentToken})
	}
	ch, cancel := s.Events.Subscribe("large")
	defer cancel()
	for want := int64(1); want <= 600; want++ {
		require.Equal(t, want, (<-ch).Seq)
	}
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

func TestMigrationAddsTriggerColumnToExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE runs (
  id TEXT PRIMARY KEY, workflow TEXT NOT NULL, fingerprint TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
  input BLOB NOT NULL, result BLOB, error TEXT NOT NULL DEFAULT '', interrupt_id TEXT NOT NULL DEFAULT '',
  gate_prompt TEXT NOT NULL DEFAULT '', decision BLOB, cancel_pending INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL, started_at TIMESTAMP, finished_at TIMESTAMP, updated_at TIMESTAMP NOT NULL)`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	s, err := app.SQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ctx := context.Background()
	require.NoError(t, s.Runs.Save(ctx, &app.Run{ID: "1", Workflow: "w", Status: app.StatusQueued, Trigger: "nightly", Input: json.RawMessage(`1`)}))
	got, err := s.Runs.Get(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, "nightly", got.Trigger)
}
