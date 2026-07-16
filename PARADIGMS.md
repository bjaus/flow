# Multi-agent paradigms in flow

SPEC.md's third north star says *every multi-agent paradigm is expressible*. This guide shows which
paradigms those are, the composition that expresses each one, and when to pick one over another.

The whole model fits in one sentence: **leaves do work, combinators arrange work, and every combinator
returns another `Step` — so a paradigm is just a composition, and paradigms compose with each other.**
There is no "pipeline mode" or "swarm mode" in flow; a swarm can sit inside a pipeline stage, a pipeline
inside a swarm actor. Each section below gives the shape, when to reach for it (and when not to), a
compact snippet against the real API, and how durability and human-in-the-loop interact with it.

Shared types used by the snippets:

```go
type Ticket struct{ Title, Body string }
type Draft  struct{ Text string; Round int; Approved bool }
type Review struct{ Verdict string; Notes string }
```

---

## 1. Pipeline / assembly line — `Then` / `Seq`

A fixed sequence of specialists, each transforming the artifact and handing it on: research → draft →
edit → format. The oldest and most predictable shape — each stage sees exactly one input type and owes
exactly one output type, and the Go compiler checks every hand-off.

**Reach for it** when the stages are known up front and each genuinely depends on the previous one's
output. **Avoid it** when stages don't depend on each other (that's a panel, §2) or when the number of
stages depends on the input (that's a route, §4, or a loop, §3).

```go
outline := flow.Agent[Ticket, string]("outliner", func(t Ticket) string { return "Outline: " + t.Title })
write   := flow.Agent[string, Draft]("writer", func(o string) string { return "Write from outline:\n" + o })
polish  := flow.Agent[Draft, Draft]("editor", func(d Draft) string { return "Polish:\n" + d.Text })

wf := flow.Define("pipeline", "Staged specialists", flow.Then(flow.Then(outline, write), polish))
```

Use `Seq(a, b, c, ...)` instead of nested `Then` when every stage is `Step[S, S]` — the flat, same-typed
state spine.

**Durability/HITL.** The friendliest paradigm: checkpoints land at step boundaries, and a `Human` gate
drops between any two stages (`flow.Then(write, approve)`), suspending the run durably at exactly that
seam. `examples/triage/workflow.go` is the minimal pipeline-plus-gate.

---

## 2. Parallel panel / fan-out–fan-in — `Parallel` / `Map` + `Reduce`

N independent experts examine the same input concurrently and *blind to each other*; a fold merges their
outputs afterwards. `Parallel` is the fixed-cast form; `Map` is the runtime-sized form (one body applied
to each item of a slice).

**Reach for it** for a panel of reviewers, multi-perspective analysis, or per-item batch work. The
blindness is a feature, not a limitation: because no reviewer sees another's verdict, no one anchors on
the first opinion — you get independent signals instead of groupthink, and the fold (majority vote,
strictest verdict, concatenation) is where consensus is formed, explicitly and in code. **Avoid it** when
the experts must build on each other's output (pipeline) or negotiate (router/network).

```go
reviewer := func(name string) flow.Step[Draft, Review] {
    return flow.Agent[Draft, Review](name, func(d Draft) string { return "Review this draft:\n" + d.Text })
}

panel := flow.Reduce(
    flow.Parallel(reviewer("security-reviewer"), reviewer("style-reviewer"), reviewer("factual-reviewer")),
    func(rs []Review) Review { // consensus lives here, in plain Go
        for _, r := range rs {
            if r.Verdict == "reject" {
                return r
            }
        }
        return Review{Verdict: "approve"}
    },
)
```

Note `Reduce`'s fold is `func([]Out) Out` — the merged value must have the same type as one branch
output. To fan in to a *different* type, skip `Reduce` and write `flow.Then(flow.Parallel(...),
flow.Do("merge", ...))` with a `Do[[]Review, Summary]`.

**Durability/HITL.** A `Human` inside one branch suspends the run while completed sibling outputs survive
in the checkpoint (`engine/durable_test.go`: `TestDurableHumanInParallel`). Two sibling gates suspended at
once each receive their own decision (`TestDurableSiblingHumansGetOwnDecisions`). `Map(Human)` gives one
durable gate per item (`reviewEach` in the same file). Shared `StateDo` state does **not** cross the
fan-out boundary — each branch is its own graph (§7). Concurrency test: `TestParallelConcurrent`,
`TestParallelAgents` in `engine/combinators_test.go`.

---

## 3. Generator–critic loop — `Loop`

One step (or sub-composition) drafts; a critique judges; the cycle repeats until the work passes or the
budget is spent. The single highest-leverage quality pattern: a model is a much better judge of work than
a one-shot producer of it.

**Reach for it** when quality is checkable — by a predicate, a judge agent, or a critic panel — and worth
iterating for. **Avoid it** when there is no meaningful convergence test; a loop whose gate can't say
"good enough" is just an expensive `Seq`.

```go
revise := flow.Then(
    flow.Agent[Draft, Draft]("writer", func(d Draft) string {
        return fmt.Sprintf("Revise (round %d):\n%s", d.Round, d.Text)
    }),
    flow.Agent[Draft, Draft]("critic", func(d Draft) string { return "Critique and mark Approved:\n" + d.Text }),
)

draftLoop := flow.Loop("draft-critique", revise,
    flow.StateGate(func(d Draft) bool { return d.Approved || d.Round >= 5 }), // the gate owns the bound
    5)
```

**The gate predicate owns the bound.** Be aware: `Loop`'s `max` argument is currently *advisory* — the
engine's `lowerLoop` (engine/compile.go) wires the cycle so the only exit is the gate passing; `Max` is
carried in the IR but not enforced there. So carry an attempt counter in `T` and make the gate's
predicate include the budget (`d.Approved || d.Round >= 5`, as above). That idiom is good practice anyway
— it keeps the termination condition visible in one place — but until the engine enforces `Max`, it is
also load-bearing. (Contrast: `Router`/`Network` *do* enforce their `Max` turn cap.) One more semantic
to know: the body always runs at least once — the gate is checked *after* each pass (do-while), so guard
against empty inputs before entering a loop whose body indexes into them.

The critic can itself be a blind panel: put `flow.Reduce(flow.Parallel(...), fold)` inside the loop body
via `Bind` — generator–critic and panel compose. Engine test: `TestLoop` in `engine/combinators_test.go`.

**Durability/HITL.** The loop is a native graph cycle; checkpoints land at each body-step boundary, so a
crash mid-iteration resumes inside the current round. Swap the critic agent for a `Human` step and the
same shape becomes a durable human revision loop: the operator's `Decision.Feedback` folds into the draft
and the gate checks `Approved`.

---

## 4. Complexity router / triage — `Route`

A cheap classifier looks at the input once and dispatches to exactly one of several fixed processes of
different weights: a FAQ short-circuit, a standard path, a research-heavy path. Static branching — the
branch set is fixed at authoring time, and control passes through exactly one case.

**Reach for it** whenever inputs vary widely in difficulty: paying the research-pipeline price for a
password-reset ticket is the classic waste this kills. **Avoid it** when the same participant must be
consulted repeatedly or the order of consultation is data-dependent — that's a `Router` (§5).

```go
answer := func(kind string) flow.Step[Ticket, string] {
    return flow.Agent[Ticket, string](kind, func(t Ticket) string { return t.Body })
}

triage := flow.Route(
    func(t Ticket) string { // cheap classifier: plain Go, or Then an Agent classifier in front
        if strings.Contains(t.Title, "how do I") {
            return "faq"
        }
        return "deep"
    },
    map[string]flow.Step[Ticket, string]{
        "faq":  answer("faq-bot"),
        "deep": flow.Then(answer("researcher"), flow.Agent[string, string]("writer", func(s string) string { return s })),
    },
).Default(answer("generalist"))
```

The classifier here is a pure function; to classify with a model, put an `Agent[In, Class]` in front with
`Then` and route on its typed output. Engine test: `TestRoute` in `engine/combinators_test.go`.

**Durability/HITL.** Each case is an ordinary sub-composition — gates nest freely inside a case and
resume durably, *including across daemon restarts that recompile the workflow*
(`TestDurableResumeAcrossRecompiles` in `engine/durable_test.go` suspends inside a Route case and resumes
in a freshly compiled instance).

---

## 5. Supervisor / orchestrator–workers — `Router`

A fixed cast of participants shares one state; each turn a selector (the "supervisor") reads the state
and decides who acts next, until a done-predicate or the turn cap. Unlike `Route`, which fires one branch
once, `Router` is a *turn loop*: the same participant can be consulted repeatedly, in a data-dependent
order the author never enumerated.

**Reach for it** when the *order* of work is the unknown — delegate, inspect the result, delegate again —
but the *cast* is known. **Avoid it** when one pass through a known order suffices (pipeline) or when the
cast itself changes at runtime (network, §6). The selector can be plain Go or an `Agent`-backed decision
folded into the state by a participant.

```go
type Desk struct {
    Task, Research, Text string
    Done                 bool
}

super := flow.Router("newsroom", flow.RouterConfig[Desk]{
    Participants: map[string]flow.Step[Desk, Desk]{
        "researcher": flow.Agent[Desk, Desk]("researcher", func(d Desk) string { return "Research: " + d.Task }),
        "writer":     flow.Agent[Desk, Desk]("writer", func(d Desk) string { return "Write up:\n" + d.Research }),
    },
    Select: func(d Desk) string { // the supervisor: reads state, names the next actor
        if d.Research == "" {
            return "researcher"
        }
        return "writer"
    },
    Done: func(d Desk) bool { return d.Done },
    Max:  12, // enforced: the dispatcher stops after Max turns
})
```

All participants are `Step[S, S]` over one shared state type — the state struct is the collaboration
protocol, so design its fields as the messages participants leave for each other. Engine test:
`TestRouter` in `engine/combinators_test.go`.

**Durability/HITL.** The dispatcher checkpoints the mesh state each turn. A `Human` can be a participant
directly — the supervisor selecting it suspends the run, and the decision folds into the state on resume.
A `Human` nested *inside* a participant's subtree also suspends and resumes correctly (the engine wraps
it in a composite interrupt and re-runs that participant on resume — see `lowerRouter` in
`engine/compile.go`). Note `Select`/`Done` are consulted repeatedly; keep them pure functions of `S`.

---

## 6. Dynamic network / swarm — `Network`

A mesh whose *membership* changes at runtime: actors spawn peers, retire peers, and hand off, with the
member registry living in the checkpointed state itself. `Next` reads that registry each turn and either
schedules an actor or signals drain.

**Reach for it** only when the participant set is genuinely unknowable at authoring time — a coordinator
that spawns one worker per discovered subtask, agents that recruit specialists. This is the most powerful
and least predictable shape; if you can enumerate the cast, use `Router` and keep the legibility.

```go
type Cluster struct {
    Members []string // the registry: actors mutate it to spawn/retire peers
    Backlog []string
}

mesh := flow.Network("swarm", flow.NetworkConfig[Cluster]{
    Actors: map[string]flow.Step[Cluster, Cluster]{
        "coordinator": flow.Do("coordinator", func(_ context.Context, c Cluster) (Cluster, error) {
            for range c.Backlog {
                c.Members = append(c.Members, "worker") // spawn: membership is just state
            }
            return c, nil
        }),
        "worker": flow.Agent[Cluster, Cluster]("worker", func(c Cluster) string { return "Take one backlog item" }),
    },
    Next: func(c Cluster) (string, bool) { // schedule, or (_, false) to drain
        if len(c.Backlog) == 0 {
            return "", false
        }
        if len(c.Members) == 0 {
            return "coordinator", true
        }
        return "worker", true
    },
    Max: 32,
})
```

`Next` must be a *pure* function of the state — the engine calls it separately for scheduling and for the
done-check each turn, so a side-effecting or non-deterministic `Next` will diverge. Engine test:
`TestNetworkDynamicMembership` in `engine/combinators_test.go` (spawns and retires peers, then drains).

**Durability/HITL.** Identical to `Router` — same dispatcher underneath. Because membership is ordinary
state, it rides the checkpoint: a run suspended at a human gate resumes with the same mesh, spawned
workers included.

---

## 7. Blackboard — `StateDo` (+ `Bind`)

Steps coordinate through a shared mutable value — the blackboard — instead of (or alongside) the typed
edges. In flow the primary spine *is* typed edges; the blackboard is the deliberate exception for
coordination an edge can't carry cleanly: a cache two distant steps both touch, provenance accumulated
across a spine, cross-cutting tallies.

**Reach for it** only when threading the value through every intermediate step's type would distort those
types. **Avoid it** as a default — an edge is visible in the composition and checked by the compiler; a
blackboard write is invisible until a read goes wrong.

```go
stash := flow.StateDo("stash", func(_ context.Context, t Ticket, _ func() any, set func(any)) (Ticket, error) {
    set(map[string]string{"source": t.Title}) // write the board
    return t, nil
})
cite := flow.StateDo("cite", func(_ context.Context, d Draft, get func() any, _ func(any)) (Draft, error) {
    if board, ok := get().(map[string]string); ok { // read it steps later
        d.Text += "\n\nSource: " + board["source"]
    }
    return d, nil
})
```

Two warnings. **Fan-out boundary:** the state is per-graph and is *not* shared across a `Parallel`/`Map`
boundary — each branch is its own graph with its own (nil-initialized) state, so a blackboard cannot be
used to coordinate concurrent branches; merge through `Reduce` instead. **Durability:** a workflow that
both uses state and pauses at a `Human` gate must register its state type with `engine.Register[T]()` so
the board can cross the checkpoint. Engine test: `TestSharedState` in `engine/state_test.go`.

For *structured* shared state, prefer making the state a typed spine: put your fields in a struct `S`,
compose with `Seq[S]`, and lift differently-typed steps onto the spine with `Bind` (§9). That is a
blackboard the compiler can see. `Router`/`Network` are themselves blackboard-controlled turn loops — the
shared `S` is the board and `Select`/`Next` read it.

---

## 8. Human-in-the-loop as a paradigm — `Human` + `Guard`

Humans are not an afterthought bolted onto the runtime; `Human` is a step like any other, so approval
policy is *composed into* the workflow and checkpointed durably wherever it sits.

The recurring shapes:

```go
// Approval gate: suspend, fold the operator's decision into the value.
approve := flow.Human("approve",
    func(d Draft, dec flow.Decision) Draft { d.Approved = dec.Approved; return d },
    func(d Draft) string { return "Publish this draft?\n\n" + d.Text })

// No-approval-no-action tripwire: hard-fail the run unless the gate passed.
publishGuard := flow.Guard("approved-only", flow.StateGate(func(d Draft) bool { return d.Approved }))

// Human revision loop: operator feedback drives redrafting until approval (gate owns the bound, §3).
redraft := flow.Loop("human-revision",
    flow.Then(flow.Agent[Draft, Draft]("writer", func(d Draft) string { return "Revise per feedback:\n" + d.Text }), approve),
    flow.StateGate(func(d Draft) bool { return d.Approved || d.Round >= 3 }), 3)
```

Per-item review over a batch has two forms. `flow.Map(gate)` opens one durable gate per item, resumable
in any order (`reviewEach` in `engine/durable_test.go`). For strictly *sequential* gates — item N+1 only
after item N is decided — use the **cursor loop**: keep `Items []T` and `Idx int` in the state, make the
loop body a `Human` gate for `Items[Idx]` that advances `Idx`, and gate on `Idx >= len(Items)`. The
sequencing lives in the cursor, so an operator sees exactly one pending decision at a time.

**Durability.** This paradigm is *made of* durability: every `Human` auto-registers its carried type with
the checkpoint serializer, and gates resume at their exact point whether they sit on the spine, inside a
`Parallel` branch, under a `Bind`, inside a `Route` case, or as a dispatch participant — including across
daemon restarts and with concurrent runs isolated from each other (`engine/durable_test.go`, all of it).

---

## 9. Hierarchical composition — workflows nesting workflows

Because every combinator returns a `Step`, a whole sub-workflow is a step: build it, name it, and drop it
into a larger composition. `Bind` is the seam that makes this practical when types differ — it lifts a
`Step[In, Out]` onto a state spine `S` via read/write lenses, so a sub-workflow keeps its own honest
types while the parent keeps its own state.

```go
// A reusable sub-workflow: string -> Review (itself a pipeline into a panel).
reviewFlow := flow.Then(write, panel) // Step[string, Review] from §1/§2 pieces

type Case struct {
    T      Ticket
    Rev    Review
}

parent := flow.Seq(
    flow.Bind(reviewFlow,
        func(c Case) string { return c.T.Body },      // read lens: project the sub-workflow's input
        func(c Case, r Review) Case { c.Rev = r; return c }, // write lens: fold its output back
    ),
    // ...more Step[Case, Case] stages
)
```

Share sub-compositions as ordinary Go functions returning `Step` (the way `examples/triage/workflow.go`
exports its workflow); the package system is the module system for paradigms. **Durability/HITL:** a
`Human` buried inside a bound sub-workflow suspends and resumes at its exact point
(`TestDurableHumanInBind` in `engine/durable_test.go`).

---

## Selection table

| You need | Reach for |
|---|---|
| Known stages, each consuming the previous stage's output | `Then` / `Seq` (§1) |
| N independent opinions on the same input, merged after | `Parallel` + `Reduce` (§2) |
| The same work applied to each item of a runtime-sized list | `Map` (+ `Reduce`) (§2) |
| Iterate until the work is good enough or the budget is spent | `Loop` + gate-owned bound (§3) |
| Match process weight to input difficulty, once | `Route` (+ `.Default`) (§4) |
| Data-dependent *order* of turns over a fixed cast | `Router` (§5) |
| The *cast itself* determined at runtime (spawn/retire) | `Network` (§6) |
| Coordination two distant steps share that edges can't carry | `StateDo` — sparingly (§7) |
| Structured shared state with compiler-checked access | struct spine + `Seq` + `Bind` (§7, §9) |
| A human decision, durably, anywhere in the graph | `Human` (§8) |
| Hard-fail unless a condition (e.g. approval) holds | `Guard` (§8) |
| Sequential one-at-a-time human review over a batch | cursor loop (§8) |
| Reuse a sub-workflow with different types inside a parent | `Bind` (§9) |

---

## Combining paradigms

Real workflows are rarely one paradigm; they are a spine of them. The shape of a production
content-review product, in four paradigms:

```go
type Job struct {
    T        Ticket
    D        Draft
    Rev      Review
    Idx      int      // cursor for sequential publish gates
    Sections []Draft
}

wf := flow.Define("editorial", "Triage, research, refine, publish with review",
    flow.Seq(
        // 1. Triage (§4): a cheap classifier decides how much process this ticket deserves.
        flow.Bind(triage, func(j Job) Ticket { return j.T }, func(j Job, s string) Job { j.D.Text = s; return j }),
        // 2. Research panel (§2): independent experts, blind to each other, folded by Reduce.
        flow.Bind(panel, func(j Job) Draft { return j.D }, func(j Job, r Review) Job { j.Rev = r; return j }),
        // 3. Generator–critic (§3): revise until the critic approves or the round budget is spent.
        flow.Bind(draftLoop, func(j Job) Draft { return j.D }, func(j Job, d Draft) Job { j.D = d; return j }),
        // 4. Sequential human gates (§8): one durable publish decision per section, in order.
        flow.Loop("publish-each",
            flow.Human("publish",
                func(j Job, dec flow.Decision) Job {
                    j.Sections[j.Idx].Approved = dec.Approved
                    j.Idx++
                    return j
                },
                func(j Job) string { return "Publish section?\n" + j.Sections[j.Idx].Text }),
            flow.StateGate(func(j Job) bool { return j.Idx >= len(j.Sections) }),
            64),
        // 5. Tripwire (§8): nothing ships unapproved, no matter what upstream did.
        flow.Guard("all-approved", flow.StateGate(func(j Job) bool {
            return !slices.ContainsFunc(j.Sections, func(d Draft) bool { return !d.Approved })
        })),
    ))
```

Each stage is a paradigm from this guide, lifted onto one `Job` spine with `Bind` (§9). The run is
durable end to end: it can crash during the research panel, restart, pause for days at a publish gate,
and resume with everything — including the loop cursor — intact. That is the north star in practice: not
nine features, one algebra.
