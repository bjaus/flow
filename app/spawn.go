package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/bjaus/flow/engine"
	"github.com/cloudwego/eino/compose"
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
	// failed or canceled child surfaces as an error. When the run is not
	// yet terminal, Await suspends the parent durably (see the method's
	// doc on runSpawner): the error it returns is the engine's interrupt
	// signal and MUST propagate out of the Do step unchanged.
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
	return context.WithValue(ctx, spawnerKey{}, &runSpawner{app: a, run: run, calls: map[string]int{}})
}

// runSpawner is the Spawner bound to one executing run. It tracks how many
// times each (workflow, input) pair was spawned during this execution, so a
// step body replayed after an await-resume reuses the children it already
// created instead of enqueuing duplicates.
type runSpawner struct {
	app *App
	run *Run

	mu    sync.Mutex
	calls map[string]int
}

// awaitGate is the interrupt payload an Await suspension carries; the worker
// detects it (by type) to park the parent as awaiting_child rather than
// surfacing a human gate.
type awaitGate struct{ ChildRunID string }

// awaitOutcome is the machine decision the worker resumes an awaiting parent
// with once its child is terminal — the exact mirror of the operator decision
// a human gate resumes with.
type awaitOutcome struct {
	ChildRunID string
	Status     Status
	Result     json.RawMessage
	Error      string
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
	// A parent resumed from an await-suspension replays its Do step from the
	// top, re-executing every Spawn before the Await. Match this call
	// positionally against the children this run already has for the same
	// (workflow, input), so replay returns the existing child.
	key := workflow + "\x00" + string(raw)
	s.mu.Lock()
	nth := s.calls[key]
	s.calls[key]++
	s.mu.Unlock()
	children, err := s.app.runs.List(ctx, RunFilter{ParentID: s.run.ID, Workflow: workflow})
	if err != nil {
		return "", fmt.Errorf("spawn %q: %w", workflow, err)
	}
	matches := lo.Filter(children, func(r *Run, _ int) bool { return bytes.Equal(r.Input, raw) })
	if nth < len(matches) {
		return matches[nth].ID, nil
	}
	return s.app.enqueue(ctx, workflow, raw, "", s.run.ID)
}

// Await returns the child's result, suspending the parent durably while the
// child is still in flight.
//
// A child already terminal is answered inline. Otherwise Await interrupts the
// parent's graph exactly here (compose.StatefulInterrupt), the worker parks
// the run as awaiting_child, and the worker slot is freed for the child. When
// the child reaches a terminal state the daemon re-enqueues the parent and
// resumes it with the child's outcome (compose.ResumeWithData); the replayed
// Await consumes that outcome and returns. Failure and cancellation of the
// child surface as errors, exactly as in the inline path.
func (s *runSpawner) Await(ctx context.Context, runID string) (json.RawMessage, error) {
	// If this Await is the resume target, the daemon already folded the
	// child's terminal outcome into the resume context.
	if resumed, _, outcome := compose.GetResumeContext[awaitOutcome](ctx); resumed && outcome.ChildRunID == runID && outcome.Status.Terminal() {
		return childResult(runID, outcome.Status, outcome.Result, outcome.Error)
	}
	r, err := s.app.runs.Get(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("await %s: %w", runID, err)
	}
	if r.Status.Terminal() {
		return childResult(runID, r.Status, r.Result, r.Error)
	}
	// Suspend the parent: the engine checkpoints the run at this step and the
	// returned interrupt error must propagate out of the Do function.
	return nil, engine.Suspend(ctx, awaitGate{ChildRunID: runID})
}

// childResult maps a terminal child status to Await's result contract.
func childResult(runID string, status Status, result json.RawMessage, errText string) (json.RawMessage, error) {
	switch status {
	case StatusSucceeded:
		return result, nil
	case StatusFailed:
		return nil, fmt.Errorf("child run %s failed: %s", runID, errText)
	case StatusCanceled:
		return nil, fmt.Errorf("child run %s canceled", runID)
	default:
		return nil, fmt.Errorf("child run %s resumed while %s", runID, status)
	}
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

// resumeAwaitingParents re-enqueues every run suspended awaiting the given
// run, now that it is terminal. The resumed parent recovers the child's
// outcome in execute via compose.ResumeWithData.
func (a *App) resumeAwaitingParents(ctx context.Context, childID string) {
	waiting, err := a.runs.List(ctx, RunFilter{Status: StatusAwaitingChild})
	if err != nil {
		return
	}
	for _, parent := range lo.Filter(waiting, func(r *Run, _ int) bool { return r.WaitingOn == childID }) {
		parent.Status = StatusQueued
		if a.runs.Save(ctx, parent) == nil {
			a.events.Publish(Event{RunID: parent.ID, Kind: EventRunResumed})
			a.signal()
		}
	}
}
