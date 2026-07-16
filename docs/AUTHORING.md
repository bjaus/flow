# Authoring reference

Import `github.com/bjaus/flow`. The core abstraction is `Step[In, Out]`: a typed unit that transforms `In`
into `Out`. Leaves perform work; combinators arrange leaves and other combinators. Every builder returns a
step, so there is no artificial nesting limit.

## Leaves

| API | Type transformation | Use |
|---|---|---|
| `Do(name, fn)` | `In → Out` | deterministic Go, ports, and escape hatches |
| `Agent(name, prompt)` | `In → Out` | persona-backed model call with typed structured output |
| `StateDo(name, fn)` | `In → Out` | graph-local ambient state, sparingly |
| `Human(name, apply, prompt)` | `T → T` | durable operator decision |

### `Do`

`Do` lifts `func(context.Context, In) (Out, error)`. Use it for validation, deterministic transforms, and
calls through injected interfaces. A returned error fails the step and participates in `.Retries` behavior.
Side effects should be idempotent because a process failure can re-enter the last incomplete step.

### `Agent`

The first argument names a persona; it is not merely a display label. The prompt function turns typed input
into the invocation-specific task, and `Out` defines the structured response type. Keep model selection,
system behavior, skills, and tool grants in persona configuration. A persona with tools runs the engine's
native model↔tool loop; one without tools performs one completion.

### `StateDo`

`StateDo` receives `get` and `set` callbacks for one graph-local `any` value. Prefer typed edges and `Bind`.
State does not cross `Parallel` or `Map` branch boundaries. If ambient state and a `Human` checkpoint coexist,
register the state type once with `engine.Register[T]()`.

### `Human`

`Human` checkpoints its carried value, emits a prompt, and resumes when a `flow.Decision` is submitted. A
decision is three-way — switch `apply` on `d.Resolved()`, which yields `flow.OutcomeApprove`,
`flow.OutcomeRevise`, or `flow.OutcomeReject` (from the explicit `Outcome` field, or derived from the legacy
`{Approved, Feedback}` pair when it is empty); `Feedback` may accompany any outcome. `apply` owns the policy
for folding that decision into `T`. Put a `Guard` after approval when a side effect must be impossible
without it.

## Combinators

| API | Input → output | Selection rule |
|---|---|---|
| `Then(a, b)` | `A → C` for `a: A→B`, `b: B→C` | typed pairwise sequence |
| `Seq(steps...)` | `S → S` | flat same-state sequence |
| `Parallel(branches...)` | `In → []Out` | fixed concurrent fan-out |
| `Map(each)` | `[]In → []Out` | runtime-sized concurrent fan-out |
| `Reduce(step, fold)` | `In → Out` | fold `[]Out` to `Out` |
| `Route(by, cases)` | `In → Out` | run one static case once |
| `Loop(name, body, gate, max)` | `T → T` | repeat a same-type body |
| `Guard(name, gate)` | `T → T` | pass or fail |
| `Bind(step, read, write)` | `S → S` | lift `In→Out` onto state `S` |
| `Router(name, config)` | `S → S` | turn-based fixed participants |
| `Network(name, config)` | `S → S` | turn-based dynamic membership |

### Fan-out and fan-in

`Parallel` uses a fixed branch list; `Map` applies one body to a runtime slice. Both execute concurrently and
collect outputs. Do not rely on completion order as a business rule—carry item IDs. `Reduce` requires the
folded result to have the branch output type. To produce a different summary type, follow the fan-out with
`Do[[]Out, Summary]`.

### Routing

`Route` chooses one case from a static map. Add `.Default(alt)` for an unknown key. If a model classifies the
request, make classification an `Agent` before the route and route over its typed result.

`Router` repeatedly calls pure `Select(S) string` and `Done(S) bool` functions over a fixed participant map.
`Network` repeatedly calls pure `Next(S) (actor, more)` over actor behaviors while membership lives in state.
Both enforce `Max`. Use static `Route` unless repeated data-dependent turns are truly needed.

### Loops and gates

A loop body runs before its exit gate is checked. Carry an attempt count in `T`, and include the budget in the
predicate even though `max` is represented in the definition. This makes termination explicit and protects
against backend differences. `StateGate` is a pure typed predicate; `Guard` fails the run if it is false.

### `Bind`

`Bind` is the main composition adapter. `read` projects a child input from parent state and `write` folds the
child output back. It avoids forcing every reusable component to accept a project-wide state struct.

## Decorators

- `.ID(id)` assigns a stable unique node handle for diagnostics and designated engine options.
- `.Retries(n)` retries a failed step.
- `.Default(step)` sets a `Route` fallback.

Builder values currently mutate their underlying definition when decorated; configure a step before reusing
it in more than one parent.

## Workflow definition and analysis

```go
wf := flow.Define("name", "Operator-facing description", root)
problems := wf.Validate()
personas := wf.AgentNames()
definition := wf.Definition() // engine/runtime integration seam
```

`Validate` checks structural errors such as duplicate IDs and empty required slots. The runtime additionally
preflights referenced personas during registration. Descriptions appear in CLI/API discovery and should state
the outcome, not repeat the name.

## Choosing state

Use, in order:

1. typed edges for direct dataflow;
2. a typed state spine plus `Bind` for a larger process;
3. `StateDo` only for truly ambient graph-local coordination;
4. an injected external port for state shared across runs or systems.

This order keeps data visible to the compiler and makes checkpoints understandable.

## Execution

For unit/in-process execution, compile with `github.com/bjaus/flow/engine`; for queueing, persistence, clients,
and human review, register with `github.com/bjaus/flow/app`. See [Runtime](RUNTIME.md).
