package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"
)

// MaxSpawnDepth bounds how deeply spawned runs may nest: a run whose ancestry
// chain is already this long cannot spawn further children. It guards against
// a workflow that (directly or via intermediaries) spawns itself forever.
const MaxSpawnDepth = 8

// Spawner lets a workflow step start child runs of other registered workflows
// and await their results, so composite pipelines (a planning workflow
// spawning one implementation run per issue) are first-class. Workflows are
// compiled Go that never sees the App, so the worker injects a Spawner into
// every run's context; inside a Do step recover it with SpawnerFrom.
type Spawner interface {
	// Spawn enqueues a run of the named registered workflow. Input is
	// marshaled to JSON and validated against the workflow's input type.
	// The child records the calling run as its parent (Run.ParentID).
	Spawn(ctx context.Context, workflow string, input any) (runID string, err error)
	// Await blocks until the run is terminal and returns its result. A
	// failed or canceled child surfaces as an error.
	Await(ctx context.Context, runID string) (result json.RawMessage, err error)
}

type spawnerKey struct{}

// SpawnerFrom recovers the Spawner the worker injected into a run's context.
// It returns false outside a run (for example in a unit test that invokes the
// step function directly).
func SpawnerFrom(ctx context.Context) (Spawner, bool) {
	s, ok := ctx.Value(spawnerKey{}).(Spawner)
	return s, ok
}

// SpawnAwait spawns a child run and blocks until its result, in one call.
func SpawnAwait(ctx context.Context, workflow string, input any) (json.RawMessage, error) {
	s, ok := SpawnerFrom(ctx)
	if !ok {
		return nil, errors.New("no spawner in context: SpawnAwait must be called inside a running workflow step")
	}
	id, err := s.Spawn(ctx, workflow, input)
	if err != nil {
		return nil, err
	}
	return s.Await(ctx, id)
}

func (a *App) withSpawner(ctx context.Context, run *Run) context.Context {
	return context.WithValue(ctx, spawnerKey{}, &runSpawner{app: a, run: run})
}

// runSpawner is the Spawner bound to one executing run.
type runSpawner struct {
	app *App
	run *Run
}

func (s *runSpawner) Spawn(ctx context.Context, workflow string, input any) (string, error) {
	depth, err := s.app.spawnDepth(ctx, s.run)
	if err != nil {
		return "", fmt.Errorf("spawn %q: %w", workflow, err)
	}
	if depth+1 > MaxSpawnDepth {
		return "", fmt.Errorf("spawn %q: max spawn depth %d exceeded", workflow, MaxSpawnDepth)
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("spawn %q: marshal input: %w", workflow, err)
	}
	return s.app.enqueue(ctx, workflow, raw, "", s.run.ID)
}

// Await drives the child to a terminal state and returns its result.
//
// Deadlock: the daemon has a single worker (§6.2), and the parent occupies it
// for the whole Await — a queued child would never be claimed. Rather than
// park the parent as awaiting-child and free the worker (durable, but it needs
// a new run state, checkpointing at the Await point, and scheduler support),
// v1 drives the child inline: Await claims the queued child directly
// (RunStore.ClaimByID) and executes it re-entrantly in the parent's worker
// slot. The tradeoff is that the parent stays `running` (and holds the worker)
// while the child runs or waits at a human gate, and an inline child does not
// survive a daemon restart as a resumable parent would. If the child is not
// claimable (another worker took it, or it is awaiting review), Await polls
// until it is queued again or terminal. The seam for the heavier approach is
// exactly here: replace the inline drive with a park-and-notify without
// touching the Spawner API.
func (s *runSpawner) Await(ctx context.Context, runID string) (json.RawMessage, error) {
	for {
		r, err := s.app.runs.Get(ctx, runID)
		if err != nil {
			return nil, fmt.Errorf("await %s: %w", runID, err)
		}
		switch r.Status {
		case StatusSucceeded:
			return r.Result, nil
		case StatusFailed:
			return nil, fmt.Errorf("child run %s failed: %s", runID, r.Error)
		case StatusCanceled:
			return nil, fmt.Errorf("child run %s canceled", runID)
		case StatusNeedsMigration:
			return nil, fmt.Errorf("child run %s needs migration", runID)
		case StatusQueued:
			claimed, err := s.app.claimRun(ctx, runID)
			if err != nil {
				return nil, fmt.Errorf("await %s: %w", runID, err)
			}
			if claimed != nil {
				if err := s.app.execute(ctx, claimed); err != nil {
					return nil, fmt.Errorf("await %s: %w", runID, err)
				}
				continue
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// claimRun claims one specific queued run, preferring the store's transactional
// ClaimByID and falling back to an optimistic save for stores without it.
func (a *App) claimRun(ctx context.Context, id string) (*Run, error) {
	if c, ok := a.runs.(interface {
		ClaimByID(context.Context, string) (*Run, error)
	}); ok {
		return c.ClaimByID(ctx, id)
	}
	r, err := a.runs.Get(ctx, id)
	if err != nil || r.Status != StatusQueued {
		return nil, err
	}
	now := time.Now().UTC()
	r.Status = StatusRunning
	if r.StartedAt == nil {
		r.StartedAt = &now
	}
	return r, a.runs.Save(ctx, r)
}

// spawnDepth counts a run's ancestors by walking ParentID links.
func (a *App) spawnDepth(ctx context.Context, run *Run) (int, error) {
	depth := 0
	for id := run.ParentID; id != "" && depth <= MaxSpawnDepth; depth++ {
		r, err := a.runs.Get(ctx, id)
		if err != nil {
			return 0, err
		}
		id = r.ParentID
	}
	return depth, nil
}

// cancelChildren cancels every non-terminal child of a run; Cancel recurses,
// so canceling a parent takes its whole descendant family down with it.
func (a *App) cancelChildren(ctx context.Context, parentID string) error {
	children, err := a.runs.List(ctx, RunFilter{ParentID: parentID})
	if err != nil {
		return err
	}
	live := lo.Filter(children, func(r *Run, _ int) bool { return !r.Status.Terminal() })
	for _, child := range live {
		if err := a.Cancel(ctx, child.ID); err != nil {
			return err
		}
	}
	return nil
}
