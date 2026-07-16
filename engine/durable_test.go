package engine_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"

	"github.com/cloudwego/eino/compose"
)

// Durable human-in-the-loop composes through nesting, not just on the top-level spine: a Human buried inside a
// fan-out branch or a bound sub-workflow checkpoints and resumes at the exact point, via a CompositeInterrupt.

func TestDurableHumanInBind(t *testing.T) {
	type S struct {
		In  string
		Out string
	}
	sub := flow.Then(
		flow.Do("prep", func(_ context.Context, s string) (string, error) { return "prepped:" + s, nil }),
		flow.Human("ok",
			func(v string, d flow.Decision) string {
				if d.Approved {
					return "APPROVED:" + v
				}
				return "NO:" + v
			},
			func(v string) string { return "approve " + v }),
	)
	bound := flow.Bind(sub, func(s S) string { return s.In }, func(s S, out string) S { s.Out = out; return s })
	wf := flow.Define("bindhuman", "", bound)

	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	const cp = "bind-1"
	in := S{In: "x"}
	_, err = app.Invoke(ctx, in, compose.WithCheckPointID(cp))
	if _, paused := compose.ExtractInterruptInfo(err); !paused {
		t.Fatalf("expected a pause at the human nested in the bind, got: %v", err)
	}
	rctx := compose.ResumeWithData(ctx, interruptID(t, err), flow.Decision{Approved: true})
	out, err := app.Invoke(rctx, in, compose.WithCheckPointID(cp))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out.Out != "APPROVED:prepped:x" {
		t.Fatalf("nested-in-bind durable human wrong: %+v", out)
	}
}

func TestDurableHumanInParallel(t *testing.T) {
	plain := flow.Do("a", func(_ context.Context, s string) (string, error) { return "a:" + s, nil })
	gated := flow.Then(
		flow.Do("bprep", func(_ context.Context, s string) (string, error) { return "b:" + s, nil }),
		flow.Human("bok",
			func(v string, d flow.Decision) string {
				if d.Approved {
					return v + ":ok"
				}
				return v + ":no"
			},
			func(v string) string { return "approve " + v }),
	)
	panel := flow.Reduce(flow.Parallel(plain, gated), func(rs []string) string {
		sort.Strings(rs)
		return strings.Join(rs, "|")
	})
	wf := flow.Define("parhuman", "", panel)

	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	const cp = "par-1"
	_, err = app.Invoke(ctx, "x", compose.WithCheckPointID(cp))
	if _, paused := compose.ExtractInterruptInfo(err); !paused {
		t.Fatalf("expected a pause at the human inside a parallel branch, got: %v", err)
	}
	rctx := compose.ResumeWithData(ctx, interruptID(t, err), flow.Decision{Approved: true})
	out, err := app.Invoke(rctx, "x", compose.WithCheckPointID(cp))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != "a:x|b:x:ok" {
		t.Fatalf("durable human in a parallel branch wrong: %q", out)
	}
}

// Two sibling gates suspended in the same super-step must each receive their own decision; the
// non-target gate re-interrupts instead of consuming a zero-value decision.
func TestDurableSiblingHumansGetOwnDecisions(t *testing.T) {
	gate := func(name string) flow.Step[string, string] {
		return flow.Human(name,
			func(v string, d flow.Decision) string {
				if d.Approved {
					return v + ":yes"
				}
				return v + ":no:" + d.Feedback
			},
			func(v string) string { return "gate " + name + " for " + v })
	}
	panel := flow.Reduce(flow.Parallel(gate("left"), gate("right")), func(rs []string) string {
		sort.Strings(rs)
		return strings.Join(rs, "|")
	})
	wf := flow.Define("siblinghumans", "", panel)

	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	const cp = "sib-1"
	_, err = app.Invoke(ctx, "x", compose.WithCheckPointID(cp))

	decisions := map[string]flow.Decision{
		"gate left for x":  {Approved: true},
		"gate right for x": {Approved: false, Feedback: "redo"},
	}
	var out string
	for range 3 {
		info, paused := compose.ExtractInterruptInfo(err)
		if !paused {
			break
		}
		ic := info.InterruptContexts[0]
		dec, ok := decisions[fmt.Sprint(ic.Info)]
		if !ok {
			t.Fatalf("unexpected gate prompt %q", fmt.Sprint(ic.Info))
		}
		rctx := compose.ResumeWithData(ctx, ic.ID, dec)
		out, err = app.Invoke(rctx, "x", compose.WithCheckPointID(cp))
	}
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out != "x:no:redo|x:yes" {
		t.Fatalf("decisions crossed between sibling gates: %q", out)
	}
}

// routedBatch is the state the recompile/isolation tests thread through Map-nested gates.
type routedBatch struct {
	Items   []string
	Results []string
}

// itemGate approves one item with the operator's feedback attached.
func itemGate() flow.Step[string, string] {
	return flow.Human("item",
		func(v string, d flow.Decision) string { return v + ":" + d.Feedback },
		func(v string) string { return "approve " + v })
}

// reviewEach binds Map(Human) into the batch state: every item is its own gate.
func reviewEach() flow.Step[routedBatch, routedBatch] {
	return flow.Bind(flow.Map(itemGate()),
		func(s routedBatch) []string { return s.Items },
		func(s routedBatch, outs []string) routedBatch { s.Results = outs; return s })
}

// driveGates resumes a suspended run one prompt-matched decision at a time until it completes.
func driveGates(t *testing.T, invoke func(context.Context) (routedBatch, error), err error, decisions map[string]flow.Decision) routedBatch {
	t.Helper()
	ctx := context.Background()
	var out routedBatch
	for range len(decisions) + 1 {
		info, paused := compose.ExtractInterruptInfo(err)
		if !paused {
			break
		}
		ic := info.InterruptContexts[0]
		dec, ok := decisions[fmt.Sprint(ic.Info)]
		if !ok {
			t.Fatalf("unexpected gate prompt %q", fmt.Sprint(ic.Info))
		}
		out, err = invoke(compose.ResumeWithData(ctx, ic.ID, dec))
	}
	if err != nil {
		t.Fatalf("drive gates: %v", err)
	}
	return out
}

// Daemon-shaped durability: the app worker recompiles the definition on every claim, so a checkpoint
// taken by one compiled instance must restore into a FRESHLY compiled one — including gates nested inside a
// Route case. Route cases live in a Go map; lowering them in raw iteration order handed out different node
// keys per compile, so restore failed with "channel[...] from checkpoint is not registered".
func TestDurableResumeAcrossRecompiles(t *testing.T) {
	pass := func(name string) flow.Step[routedBatch, routedBatch] {
		return flow.Do(name, func(_ context.Context, s routedBatch) (routedBatch, error) { return s, nil })
	}
	wf := flow.Define("routedbatch", "", flow.Route(func(routedBatch) string { return "review" },
		map[string]flow.Step[routedBatch, routedBatch]{
			"a": pass("case-a"), "b": pass("case-b"), "review": reviewEach(), "d": pass("case-d"), "e": pass("case-e"),
		}))

	store := &memStore{m: map[string][]byte{}}
	const cp = "recompile-1"
	in := routedBatch{Items: []string{"a", "b"}}
	invoke := func(ctx context.Context) (routedBatch, error) { // a fresh compile per invocation, like the worker
		app, err := engine.Compile(context.Background(), wf, registry(nil), store)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return app.Invoke(ctx, in, compose.WithCheckPointID(cp))
	}
	_, err := invoke(context.Background())
	out := driveGates(t, invoke, err, map[string]flow.Decision{
		"approve a": {Approved: true, Feedback: "alpha"},
		"approve b": {Approved: true, Feedback: "beta"},
	})
	if got := strings.Join(out.Results, "|"); got != "a:alpha|b:beta" {
		t.Fatalf("resume across recompiles produced wrong results: %q", got)
	}
}

// Two concurrent RUNS of the same workflow suspended at Map-nested gates must not share fan-out sub-run
// checkpoints: node keys are deterministic per definition, so without a per-run scope both runs would write
// their bind/map sub-checkpoints under identical ids and restore each other's state.
func TestConcurrentRunsIsolateFanOutSubCheckpoints(t *testing.T) {
	wf := flow.Define("scopedbatch", "", reviewEach())
	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	invoke := func(cp string, in routedBatch) func(context.Context) (routedBatch, error) {
		return func(ctx context.Context) (routedBatch, error) {
			return app.Invoke(engine.WithCheckpointScope(ctx, cp), in, compose.WithCheckPointID(cp))
		}
	}
	inA, inB := routedBatch{Items: []string{"a1", "a2"}}, routedBatch{Items: []string{"b1", "b2"}}
	invokeA, invokeB := invoke("run-A", inA), invoke("run-B", inB)

	// Suspend BOTH runs before resuming either, so their sub-run checkpoints coexist in the store.
	_, errA := invokeA(context.Background())
	_, errB := invokeB(context.Background())

	outA := driveGates(t, invokeA, errA, map[string]flow.Decision{
		"approve a1": {Approved: true, Feedback: "a-one"},
		"approve a2": {Approved: true, Feedback: "a-two"},
	})
	outB := driveGates(t, invokeB, errB, map[string]flow.Decision{
		"approve b1": {Approved: true, Feedback: "b-one"},
		"approve b2": {Approved: true, Feedback: "b-two"},
	})
	if got := strings.Join(outA.Results, "|"); got != "a1:a-one|a2:a-two" {
		t.Fatalf("run A results crossed with run B: %q", got)
	}
	if got := strings.Join(outB.Results, "|"); got != "b1:b-one|b2:b-two" {
		t.Fatalf("run B results crossed with run A: %q", got)
	}
}

// A Human apply that switches on Resolved() sees three distinct outcomes — approve, revise, reject — whether
// the operator names the outcome explicitly or a legacy client encodes it through {approved, feedback}.
func TestDurableHumanThreeWayResolvedOutcomes(t *testing.T) {
	gate := flow.Human("triage",
		func(v string, d flow.Decision) string {
			switch d.Resolved() {
			case flow.OutcomeApprove:
				return "GO:" + v
			case flow.OutcomeRevise:
				return "REDO(" + d.Feedback + "):" + v
			case flow.OutcomeReject:
				return "STOP(" + d.Feedback + "):" + v
			default:
				return "?:" + v
			}
		},
		func(v string) string { return "review " + v })
	wf := flow.Define("threeway", "", gate)

	cases := []struct {
		name string
		dec  flow.Decision
		want string
	}{
		{"explicit approve", flow.Decision{Outcome: flow.OutcomeApprove}, "GO:x"},
		{"explicit revise", flow.Decision{Outcome: flow.OutcomeRevise, Feedback: "tighten"}, "REDO(tighten):x"},
		{"explicit reject with feedback", flow.Decision{Outcome: flow.OutcomeReject, Feedback: "out of scope"}, "STOP(out of scope):x"},
		{"legacy approve", flow.Decision{Approved: true}, "GO:x"},
		{"legacy revise", flow.Decision{Feedback: "tighten"}, "REDO(tighten):x"},
		{"legacy reject", flow.Decision{}, "STOP():x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			ctx := context.Background()
			cp := "threeway-" + tc.name
			_, err = app.Invoke(ctx, "x", compose.WithCheckPointID(cp))
			if _, paused := compose.ExtractInterruptInfo(err); !paused {
				t.Fatalf("expected a pause at the gate, got: %v", err)
			}
			rctx := compose.ResumeWithData(ctx, interruptID(t, err), tc.dec)
			out, err := app.Invoke(rctx, "x", compose.WithCheckPointID(cp))
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			if out != tc.want {
				t.Fatalf("out = %q, want %q", out, tc.want)
			}
		})
	}
}
