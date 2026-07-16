package app

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/robfig/cron/v3"
	"github.com/samber/lo"
)

// Trigger declares a scheduled enqueue of a registered workflow: on each
// firing of Spec (a standard 5-field cron expression) the daemon enqueues a
// run of Workflow with the canned Input, exactly as POST /api/runs would.
type Trigger struct {
	// Name identifies the trigger in events and on the runs it creates.
	// Optional; defaults to "<workflow>@<spec>".
	Name string `json:"name,omitempty"`
	// Workflow is the registered workflow to enqueue.
	Workflow string `json:"workflow"`
	// Spec is the cron schedule, e.g. "*/5 * * * *".
	Spec string `json:"spec"`
	// Input is the canned JSON input for each scheduled run.
	Input json.RawMessage `json:"input,omitempty"`
}

type schedule struct {
	trigger Trigger
	cron    cron.Schedule
}

func parseTriggers(triggers []Trigger) ([]schedule, error) {
	seen := make(map[string]bool, len(triggers))
	schedules := make([]schedule, 0, len(triggers))
	for _, t := range triggers {
		if t.Workflow == "" {
			return nil, fmt.Errorf("trigger %q: workflow is required", t.Name)
		}
		if t.Name == "" {
			t.Name = t.Workflow + "@" + t.Spec
		}
		if seen[t.Name] {
			return nil, fmt.Errorf("trigger %q: duplicate trigger name", t.Name)
		}
		seen[t.Name] = true
		s, err := cron.ParseStandard(t.Spec)
		if err != nil {
			return nil, fmt.Errorf("trigger %q: invalid cron spec %q: %w", t.Name, t.Spec, err)
		}
		schedules = append(schedules, schedule{trigger: t, cron: s})
	}
	return schedules, nil
}

// validateTriggers checks every trigger against the registered workflows:
// the workflow must exist and the canned input must decode to its input type.
func (a *App) validateTriggers() error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, s := range a.schedules {
		wf := a.workflows[s.trigger.Workflow]
		if wf == nil {
			return fmt.Errorf("trigger %q: workflow %q is not registered", s.trigger.Name, s.trigger.Workflow)
		}
		input := s.trigger.Input
		if len(input) == 0 {
			input = json.RawMessage("null")
		}
		if _, err := decodeInput(input, wf.definition.In); err != nil {
			return fmt.Errorf("trigger %q: input does not decode to %s: %w", s.trigger.Name, wf.definition.In, err)
		}
	}
	return nil
}

// runScheduler fires due triggers until ctx is canceled. It waits for the
// earliest next firing across all schedules, then fires every trigger due at
// that instant.
func (a *App) runScheduler(ctx context.Context) {
	if len(a.schedules) == 0 {
		return
	}
	for {
		now := a.clock()
		due := lo.MinBy(a.schedules, func(x, min schedule) bool { return x.cron.Next(now).Before(min.cron.Next(now)) }).cron.Next(now)
		select {
		case <-ctx.Done():
			return
		case <-a.timer(due.Sub(now)):
		}
		if ctx.Err() != nil {
			return
		}
		for _, s := range a.schedules {
			if !s.cron.Next(now).After(due) {
				a.fire(ctx, s.trigger)
			}
		}
	}
}

// fire enqueues one scheduled run of t, unless the daemon is draining or the
// trigger's previous run is still queued, running, or awaiting review — in
// which case it publishes a trigger.skipped event instead (no pile-up).
func (a *App) fire(ctx context.Context, t Trigger) {
	skip := func(reason string) {
		a.events.Publish(Event{Kind: EventTriggerSkipped, Data: mustJSON(map[string]string{"trigger": t.Name, "workflow": t.Workflow, "reason": reason})})
	}
	if a.cfg.DrainOnly || a.draining.Load() {
		skip("daemon is draining")
		return
	}
	busy, err := a.triggerBusy(ctx, t)
	if err != nil {
		skip("run store error: " + err.Error())
		return
	}
	if busy {
		skip("previous run is still active")
		return
	}
	if _, err := a.enqueue(ctx, t.Workflow, t.Input, t.Name, ""); err != nil {
		skip("enqueue failed: " + err.Error())
	}
}

// triggerBusy reports whether a run created by t is still queued, running,
// awaiting review, or awaiting a child run.
func (a *App) triggerBusy(ctx context.Context, t Trigger) (bool, error) {
	for _, status := range []Status{StatusQueued, StatusRunning, StatusAwaitingReview, StatusAwaitingChild} {
		runs, err := a.runs.List(ctx, RunFilter{Workflow: t.Workflow, Status: status})
		if err != nil {
			return false, err
		}
		if lo.SomeBy(runs, func(r *Run) bool { return r.Trigger == t.Name }) {
			return true, nil
		}
	}
	return false, nil
}
