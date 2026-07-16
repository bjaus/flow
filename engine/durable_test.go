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
