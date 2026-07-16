---
name: flow-dsl
description: The flow DSL public API — leaves, combinators, decorators, and how to author a type-safe workflow. Load when writing workflows for tests, examples, or the app runtime.
---

# Authoring workflows with the `flow` package

The DSL lives in package `flow` (module path `github.com/bjaus/flow`). There is one core type,
`flow.Step[In, Out]` — a typed unit of execution. Leaves do work; combinators arrange steps and return steps,
so composition is unbounded. Data flows along typed edges (`Out → In`); an ambient state is used only for
coordination (fan-in, loops, blackboards).

## Leaves

- `Do[In, Out](name string, fn func(context.Context, In) (Out, error)) Step[In, Out]` — a plain typed
  function; the deterministic primitive. (Call any eino component from inside a `Do` to use it in a workflow.)
- `StateDo[In, Out](name string, fn func(ctx context.Context, in In, get func() any, set func(any)) (Out, error)) Step[In, Out]`
  — like `Do` but with read/write access to the workflow's shared state, for coordination the typed edges
  can't carry. Per-graph; NOT shared across a `Parallel`/`Map` fan-out boundary. Prefer edges and `Bind` for
  linear data flow.
- `Agent[In, Out](name string, prompt func(In) string) Step[In, Out]` — an LLM step. The persona is resolved
  by name at runtime; `prompt` renders the typed input into the task; `Out` is the structured-output contract.
  If the resolved persona has tools, the Agent runs a real ReAct loop (calls tools and iterates); otherwise a
  single completion.
- `Human[T](name string, apply func(T, Decision) T, prompt func(T) string) Step[T, T]` — suspends the run for
  a person. `apply` folds the operator's answer into the value. `Decision` is
  `struct{ Approved bool; Feedback string }`. Works on the top-level spine AND nested inside a `Parallel`/
  `Map` branch, a `Bind`, or a dispatch participant (durable resume at the exact point).

## Combinators

- `Then[A, B, C](a Step[A,B], b Step[B,C]) Step[A,C]` — pairwise, fully typed sequencing.
- `Seq[S](steps ...Step[S,S]) Step[S,S]` — flat same-typed spine.
- `Parallel[In, Out](branches ...Step[In,Out]) Step[In, []Out]` — fixed-count fan-out (real concurrency).
- `Map[In, Out](each Step[In,Out]) Step[[]In, []Out]` — runtime-sized fan-out.
- `Reduce[In, Out](s Step[In, []Out], fold func([]Out) Out) Step[In, Out]` — fan-in.
- `Route[In, Out](by func(In) string, cases map[string]Step[In,Out]) Step[In, Out]` — static branch + join.
- `Loop[T](name string, body Step[T,T], until Gate[T], max int) Step[T,T]` — convergence-terminated cycle.
- `Guard[T](name string, gate Gate[T]) Step[T,T]`; build a gate with
  `StateGate[T](pred func(T) bool) Gate[T]`.
- `Bind[S, In, Out](s Step[In,Out], read func(S) In, write func(S, Out) S) Step[S,S]` — lift a typed step
  into a same-typed state spine.
- `Router[S](name string, cfg RouterConfig[S]) Step[S,S]` — dynamic dispatch over a fixed participant set.
- `Network[S](name string, cfg NetworkConfig[S]) Step[S,S]` — dynamic-membership mesh (spawn/remove peers).

```go
type RouterConfig[S any]  struct { Participants map[string]Step[S,S]; Select func(S) string; Done func(S) bool; Max int }
type NetworkConfig[S any] struct { Actors       map[string]Step[S,S]; Next   func(S) (actor string, more bool); Max int }
```

## Decorators (chainable, type-preserving)

`s.ID(string)` · `s.Retries(int)` · `s.Default(alt Step[In,Out])`.

## Entry point and analysis

`Define[In, Out](name, desc string, root Step[In,Out]) Workflow[In, Out]`. On a `Workflow`:
`Definition() *ir.Node`, `Validate() []string` (duplicate ids, edge-type mismatches — empty means valid),
`AgentNames() []string` (preflight that referenced personas exist).

## Example (self-contained)

```go
type Ticket struct{ Title string }
type Plan   struct{ Steps []string; Approved bool }

plan := flow.Agent[Ticket, Plan]("planner", func(t Ticket) string {
    return "Draft a plan for: " + t.Title
})
approve := flow.Human("approve",
    func(p Plan, d flow.Decision) Plan { p.Approved = d.Approved; return p },
    func(p Plan) string { return "Approve this plan?" })

wf := flow.Define("triage", "plan then approve", flow.Then(plan, approve)) // Workflow[Ticket, Plan]
```

To execute a workflow, compile it with the `engine` package — see the **engine-api** skill. The existing tests
in `engine/*_test.go` are runnable examples of every combinator.
