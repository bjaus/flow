package engine_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"
)

func compileApp[In, Out any](t *testing.T, wf flow.Workflow[In, Out], reg engine.Registry) engine.Runnable[In, Out] {
	t.Helper()
	app, err := engine.Compile(context.Background(), wf, reg, nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return app
}

func TestRoute(t *testing.T) {
	mk := func(tag string) flow.Step[string, string] {
		return flow.Do(tag, func(_ context.Context, s string) (string, error) { return tag + ":" + s, nil })
	}
	wf := flow.Define("route", "", flow.Route(
		func(s string) string { return s },
		map[string]flow.Step[string, string]{"bug": mk("bug"), "feat": mk("feat")},
	))
	if got, _ := compileApp(t, wf, registry(nil)).Invoke(context.Background(), "bug"); got != "bug:bug" {
		t.Fatalf("route: %q", got)
	}
}

func TestLoop(t *testing.T) {
	inc := flow.Do("inc", func(_ context.Context, n int) (int, error) { return n + 1, nil })
	wf := flow.Define("loop", "", flow.Loop("count", inc, flow.StateGate(func(n int) bool { return n >= 3 }), 10))
	if got, _ := compileApp(t, wf, registry(nil)).Invoke(context.Background(), 0); got != 3 {
		t.Fatalf("loop: %d", got)
	}
}

func TestGate(t *testing.T) {
	wf := flow.Define("guard", "", flow.Then(
		flow.Guard("safe", flow.StateGate(func(s string) bool { return s != "bad" })),
		flow.Do("ok", func(_ context.Context, s string) (string, error) { return "ok:" + s, nil }),
	))
	app := compileApp(t, wf, registry(nil))
	if got, _ := app.Invoke(context.Background(), "good"); got != "ok:good" {
		t.Fatalf("gate(pass): %q", got)
	}
	if _, err := app.Invoke(context.Background(), "bad"); err == nil {
		t.Fatal("gate(fail): expected rejection")
	}
}

func TestParallelConcurrent(t *testing.T) {
	rev := func(tag string) flow.Step[string, string] {
		return flow.Do(tag, func(_ context.Context, _ string) (string, error) {
			time.Sleep(50 * time.Millisecond)
			return tag, nil
		})
	}
	wf := flow.Define("panel", "", flow.Reduce(
		flow.Parallel(rev("a"), rev("b"), rev("c")),
		func(rs []string) string { sort.Strings(rs); return strings.Join(rs, "-") },
	))
	start := time.Now()
	got, _ := compileApp(t, wf, registry(nil)).Invoke(context.Background(), "x")
	elapsed := time.Since(start)
	if got != "a-b-c" {
		t.Fatalf("parallel result: %q", got)
	}
	if elapsed > 120*time.Millisecond {
		t.Fatalf("3×50ms reviewers took %v — not concurrent", elapsed)
	}
}

func TestMap(t *testing.T) {
	dbl := flow.Do("dbl", func(_ context.Context, n int) (int, error) { return n * 2, nil })
	wf := flow.Define("map", "", flow.Reduce(
		flow.Map(dbl),
		func(ns []int) int {
			s := 0
			for _, n := range ns {
				s += n
			}
			return s
		},
	))
	if got, _ := compileApp(t, wf, registry(nil)).Invoke(context.Background(), []int{1, 2, 3}); got != 12 {
		t.Fatalf("map-reduce: %d", got)
	}
}

func TestBind(t *testing.T) {
	type St struct{ N, Doubled int }
	dbl := flow.Do("dbl", func(_ context.Context, n int) (int, error) { return n * 2, nil })
	wf := flow.Define("bind", "", flow.Bind(dbl,
		func(s St) int { return s.N },
		func(s St, d int) St { s.Doubled = d; return s }))
	if got, _ := compileApp(t, wf, registry(nil)).Invoke(context.Background(), St{N: 5}); got.Doubled != 10 {
		t.Fatalf("bind: %+v", got)
	}
}

func TestRouter(t *testing.T) {
	type B struct {
		Steps int
		Log   []string
	}
	mk := func(tag string) flow.Step[B, B] {
		return flow.Do(tag, func(_ context.Context, b B) (B, error) { b.Log = append(b.Log, tag); b.Steps++; return b, nil })
	}
	wf := flow.Define("router", "", flow.Router("dispatch", flow.RouterConfig[B]{
		Participants: map[string]flow.Step[B, B]{"x": mk("x"), "y": mk("y")},
		Select: func(b B) string {
			if b.Steps%2 == 0 {
				return "x"
			}
			return "y"
		},
		Done: func(b B) bool { return b.Steps >= 3 },
		Max:  10,
	}))
	if got, _ := compileApp(t, wf, registry(nil)).Invoke(context.Background(), B{}); len(got.Log) != 3 {
		t.Fatalf("router: %+v", got)
	}
}

// Item 4: the actor tier — a mesh whose membership changes at runtime (spawn/remove), draining cleanly.
func TestNetworkDynamicMembership(t *testing.T) {
	type Cluster struct {
		Members []string
		Log     []string
		Turn    int
	}
	lead := flow.Do("lead", func(_ context.Context, m Cluster) (Cluster, error) {
		switch m.Turn {
		case 0:
			m.Members = append(m.Members, "C") // spawn a peer at runtime
		case 2:
			kept := m.Members[:0:0]
			for _, x := range m.Members {
				if x != "A" {
					kept = append(kept, x)
				}
			}
			m.Members = kept // remove a peer at runtime
		}
		m.Turn++
		return m, nil
	})
	worker := flow.Do("worker", func(_ context.Context, m Cluster) (Cluster, error) {
		m.Log = append(m.Log, "drain")
		m.Turn++
		return m, nil
	})

	wf := flow.Define("mesh", "", flow.Network("m", flow.NetworkConfig[Cluster]{
		Actors: map[string]flow.Step[Cluster, Cluster]{"lead": lead, "worker": worker},
		Next: func(m Cluster) (string, bool) {
			if m.Turn >= 4 {
				return "", false // drain
			}
			if m.Turn == 0 || m.Turn == 2 {
				return "lead", true
			}
			return "worker", true
		},
		Max: 8,
	}))
	got, _ := compileApp(t, wf, registry(nil)).Invoke(context.Background(), Cluster{Members: []string{"A", "B"}})
	if len(got.Members) != 2 || got.Members[0] != "B" || got.Members[1] != "C" {
		t.Fatalf("runtime membership wrong: %v", got.Members)
	}
}

// agents run inside a concurrent fan-out (results collected natively).
func TestParallelAgents(t *testing.T) {
	reg := registry(map[string]string{"a": "alpha", "b": "beta"})
	wf := flow.Define("dual", "", flow.Reduce(
		flow.Parallel(
			flow.Agent[string, string]("a", func(s string) string { return s }),
			flow.Agent[string, string]("b", func(s string) string { return s }),
		),
		func(rs []string) string { sort.Strings(rs); return strings.Join(rs, "|") },
	))
	got, _ := compileApp(t, wf, reg).Invoke(context.Background(), "q")
	if !strings.Contains(got, "alpha") || !strings.Contains(got, "beta") {
		t.Fatalf("parallel agents: %q", got)
	}
}
