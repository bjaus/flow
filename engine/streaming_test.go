package engine_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"

	"github.com/cloudwego/eino/compose"
)

// Item 1: agents INSIDE a concurrent fan-out stream their tokens to the sink (per-branch streaming).
func TestParallelAgentStreaming(t *testing.T) {
	reg := registry(map[string]string{
		"reviewer_a": "looks correct and safe to me",
		"reviewer_b": "tests are adequate ship it",
	})
	wf := flow.Define("panel", "", flow.Reduce(
		flow.Parallel(
			flow.Agent[string, string]("reviewer_a", func(s string) string { return s }),
			flow.Agent[string, string]("reviewer_b", func(s string) string { return s }),
		),
		func(rs []string) string { return strings.Join(rs, " || ") },
	))
	app, err := engine.Compile(context.Background(), wf, reg, nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	sink := newSink()
	sr, err := app.Stream(context.Background(), "review the diff", compose.WithCallbacks(sink))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	for {
		if _, e := sr.Recv(); e == io.EOF {
			break
		} else if e != nil {
			t.Fatalf("recv: %v", e)
		}
	}
	// BOTH reviewers streamed their tokens to the sink, tagged by agent name.
	if a := sink.text("reviewer_a"); !strings.Contains(a, "looks correct and safe") {
		t.Fatalf("reviewer_a did not stream: %q", a)
	}
	if b := sink.text("reviewer_b"); !strings.Contains(b, "tests are adequate") {
		t.Fatalf("reviewer_b did not stream: %q", b)
	}
}

// Item 2: a durable human gate carrying a STRUCT value — auto-registered via the workflow's In/Out type, no
// manual registration.
type Ticket struct {
	ID       string
	Note     string
	Approved bool
}

func TestDurableStructHumanGate(t *testing.T) {
	prep := flow.Do("prep", func(_ context.Context, tk Ticket) (Ticket, error) { tk.Note = "prepared"; return tk, nil })
	approve := flow.Human("approve",
		func(tk Ticket, d flow.Decision) Ticket { tk.Approved = d.Approved; return tk },
		func(tk Ticket) string { return "approve " + tk.ID + "?" })
	ship := flow.Do("ship", func(_ context.Context, tk Ticket) (Ticket, error) { tk.Note += "+shipped"; return tk, nil })

	wf := flow.Define("ticket", "", flow.Then(flow.Then(prep, approve), ship))
	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	const cp = "t-1"
	in := Ticket{ID: "JH-9"}

	_, err = app.Invoke(ctx, in, compose.WithCheckPointID(cp))
	if _, paused := compose.ExtractInterruptInfo(err); !paused {
		t.Fatalf("expected pause at human gate, got: %v", err)
	}
	rctx := compose.ResumeWithData(ctx, interruptID(t, err), flow.Decision{Approved: true})
	out, err := app.Invoke(rctx, in, compose.WithCheckPointID(cp))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !out.Approved || out.Note != "prepared+shipped" {
		t.Fatalf("struct survived the checkpoint wrong: %+v", out)
	}
}

// Item 1 (full): an INTERMEDIATE struct (neither In nor Out) crosses the human gate — auto-registered
// reflectively from the definition, no manual Register.
type Plan struct {
	Steps    []string
	Approved bool
}

func TestDurableIntermediateStruct(t *testing.T) {
	prep := flow.Do("prep", func(_ context.Context, s string) (Plan, error) { return Plan{Steps: []string{s}}, nil })
	approve := flow.Human("approve",
		func(p Plan, d flow.Decision) Plan { p.Approved = d.Approved; return p },
		func(p Plan) string { return "approve?" })
	finalize := flow.Do("finalize", func(_ context.Context, p Plan) (string, error) {
		out := strings.Join(p.Steps, ",")
		if p.Approved {
			out += ":approved"
		}
		return out, nil
	})
	wf := flow.Define("t", "", flow.Then(flow.Then(prep, approve), finalize)) // Workflow[string,string], Plan is intermediate

	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	const cp = "p-1"
	_, err = app.Invoke(ctx, "add-retry", compose.WithCheckPointID(cp))
	if _, paused := compose.ExtractInterruptInfo(err); !paused {
		t.Fatalf("expected pause, got: %v", err)
	}
	rctx := compose.ResumeWithData(ctx, interruptID(t, err), flow.Decision{Approved: true})
	out, err := app.Invoke(rctx, "add-retry", compose.WithCheckPointID(cp))
	if err != nil {
		t.Fatalf("resume (intermediate struct not auto-registered?): %v", err)
	}
	if out != "add-retry:approved" {
		t.Fatalf("intermediate struct lost across checkpoint: %q", out)
	}
}

// Item 2: a Network with a HUMAN participant suspends mid-mesh and resumes durably at the exact turn —
// runtime membership + durable dispatcher, wired into the Network combinator.
type Mesh struct {
	Members  []string
	Turn     int
	Approved bool
}

func TestDurableNetworkHuman(t *testing.T) {
	lead := flow.Do("lead", func(_ context.Context, m Mesh) (Mesh, error) {
		switch m.Turn {
		case 0:
			m.Members = append(m.Members, "C")
			m.Turn = 1
		case 2:
			kept := m.Members[:0:0]
			for _, x := range m.Members {
				if x != "A" {
					kept = append(kept, x)
				}
			}
			m.Members = kept
			m.Turn = 3
		}
		return m, nil
	})
	consensus := flow.Human("consensus",
		func(m Mesh, d flow.Decision) Mesh { m.Approved = d.Approved; m.Turn = 2; return m },
		func(m Mesh) string { return "consensus?" })
	worker := flow.Do("worker", func(_ context.Context, m Mesh) (Mesh, error) { m.Turn = 4; return m, nil })

	wf := flow.Define("mesh", "", flow.Network("m", flow.NetworkConfig[Mesh]{
		Actors: map[string]flow.Step[Mesh, Mesh]{"lead": lead, "human": consensus, "worker": worker},
		Next: func(m Mesh) (string, bool) {
			switch {
			case m.Turn >= 4:
				return "", false
			case m.Turn == 0 || m.Turn == 2:
				return "lead", true
			case m.Turn == 1:
				return "human", true
			default:
				return "worker", true
			}
		},
		Max: 8,
	}))

	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	const cp = "mesh-1"
	in := Mesh{Members: []string{"A", "B"}}

	// runs turn 0 (spawn C), suspends at the human turn
	_, err = app.Invoke(ctx, in, compose.WithCheckPointID(cp))
	if _, paused := compose.ExtractInterruptInfo(err); !paused {
		t.Fatalf("expected mid-mesh pause, got: %v", err)
	}
	// resume: approve, continue (remove A, drain)
	rctx := compose.ResumeWithData(ctx, interruptID(t, err), flow.Decision{Approved: true})
	out, err := app.Invoke(rctx, in, compose.WithCheckPointID(cp))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(out.Members) != 2 || out.Members[0] != "B" || out.Members[1] != "C" || !out.Approved {
		t.Fatalf("durable mesh wrong: members=%v approved=%v", out.Members, out.Approved)
	}
}
