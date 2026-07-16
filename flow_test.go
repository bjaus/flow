package flow

import (
	"context"
	"testing"

	"github.com/bjaus/flow/internal/ir"
)

// The pure DSL is authored and analyzed with no backend. These tests exercise the typed builder and the
// shape analysis (Validate / AgentNames / Walk) — no execution, no tokens.

type Ticket struct{ Title string }
type Plan struct{ Tasks []string }
type Repo struct {
	Plan     Plan
	Patches  int
	Reviews  int
	Approved bool
}

func buildExample() Workflow[Repo, Repo] {
	// A typed segment: Ticket -> Plan (edges-primary), bound into a Repo state spine.
	planner := Agent[Ticket, Plan]("planner", func(t Ticket) string { return "plan: " + t.Title })

	reviewer := func(lens string) Step[Repo, Repo] {
		return Do("review:"+lens, func(_ context.Context, r Repo) (Repo, error) { r.Reviews++; return r, nil })
	}
	supervisor := Router("build", RouterConfig[Repo]{
		Participants: map[string]Step[Repo, Repo]{
			"fe": Do("fe", func(_ context.Context, r Repo) (Repo, error) { r.Patches++; return r, nil }),
			"be": Do("be", func(_ context.Context, r Repo) (Repo, error) { r.Patches++; return r, nil }),
		},
		Select: func(r Repo) string {
			if r.Patches%2 == 0 {
				return "fe"
			}
			return "be"
		},
		Done: func(r Repo) bool { return r.Patches >= 2 },
		Max:  6,
	})

	root := Seq(
		Bind(planner, func(r Repo) Ticket { return Ticket{} }, func(r Repo, p Plan) Repo { r.Plan = p; return r }),
		supervisor,
		Loop("review", Seq(reviewer("correctness"), reviewer("security")),
			StateGate(func(r Repo) bool { return r.Reviews >= 2 }), 3),
		Human("approve", func(r Repo, d Decision) Repo { r.Approved = d.Approved; return r },
			func(r Repo) string { return "approve?" }),
	)
	return Define("ship", "plan, build, review, approve", root)
}

func TestValidateClean(t *testing.T) {
	if p := buildExample().Validate(); len(p) != 0 {
		t.Fatalf("expected a clean workflow, got: %v", p)
	}
}

func TestDuplicateIDCaught(t *testing.T) {
	wf := Define("dup", "two ids collide",
		Seq(
			Do("a", func(_ context.Context, r Repo) (Repo, error) { return r, nil }).ID("same"),
			Do("b", func(_ context.Context, r Repo) (Repo, error) { return r, nil }).ID("same"),
		))
	if len(wf.Validate()) == 0 {
		t.Fatal("expected a duplicate-id problem")
	}
}

func TestAgentNames(t *testing.T) {
	names := buildExample().AgentNames()
	if len(names) != 1 || names[0] != "planner" {
		t.Fatalf("agent names = %v, want [planner]", names)
	}
}

func TestWalkCountsKinds(t *testing.T) {
	counts := map[ir.Kind]int{}
	ir.Walk(buildExample().Definition(), func(n *ir.Node) { counts[n.Kind]++ })
	for kind, want := range map[ir.Kind]int{ir.KSeq: 2, ir.KRouter: 1, ir.KLoop: 1, ir.KHuman: 1, ir.KBind: 1} {
		if counts[kind] != want {
			t.Errorf("kind %d count = %d, want %d", kind, counts[kind], want)
		}
	}
}
