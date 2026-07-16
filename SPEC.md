# flow — Build Specification

**What this is.** A self-contained specification for building **flow**: a local-first, single-binary Go
platform for authoring and running long-lived agentic workflows. It is written to be read from scratch by an
engineer or a coding agent working in the `flow` repository, with no external context required.

**What is already built, and what remains.** The `flow` module — the type-safe workflow **DSL** and its
execution **engine** — is implemented and lives in this repository (`./` and `./engine`). Its code is the
source of truth for *authoring*; this document does not re-specify it, and treats it as a fixed, ready
dependency. What remains to build — and what this document specifies in full — is everything that turns an
authored workflow into a running product:

- the **`flow/app` runtime**: a durable server, pluggable stores, a model provider, a markdown-driven
  agent/skill registry, a terminal UI, and an installable web app;
- the repository's **module layout and packaging**, so the DSL is a clean public library and the runtime is
  a second module that bundles the server and its clients (terminal, CLI, and web);
- the **safe-redeployment strategy** for shipping new binaries without clobbering in-flight work;
- the world-class **API documentation** the public `flow` package needs.

**North stars, in priority order.**
1. **Type-safety.** Workflows are Go code, generic over their types; the compiler checks them.
2. **Unbounded composability.** Any workflow nests inside any other, without limit.
3. **Every multi-agent paradigm is expressible.**
4. **Durable execution + human-in-the-loop** on a single machine, no external services required.
5. **Ergonomic, idiomatic Go.** APIs that read well and fight the language as little as possible.

---

## 1. The product in one picture

flow is **one repository that ships as two Go modules and builds into one binary**:

- The **`flow` library** (`github.com/bjaus/flow`) — you author workflows in type-safe Go.
- The **`flow/app` runtime** (`github.com/bjaus/flow/app`) — you run, observe, and steer them.

A developer imports both, writes a ~10-line `main`, and runs `go build`. The resulting binary is a durable
daemon that executes workflows and serves three clients over one HTTP+SSE API: a terminal UI, a command-line
client, and an installable, mobile-friendly web app (PWA). Everything runs on one machine against local,
zero-config defaults (a SQLite store, a local model gateway); every backing technology is an interface you can
swap without touching a workflow.

```
             author (Go)                          run / observe / steer
   ┌───────────────────────────┐        ┌───────────────────────────────────────┐
   │  import "…/flow"           │        │      daemon (one process)              │──▶ TUI  (terminal client)
   │  wf := flow.Define(…)      │  ───▶  │  queue → worker → engine.Runnable      │
   │  a.Register(wf)            │        │        │            │                  │──▶ CLI  (shell / scripts)
   └───────────────────────────┘        │        ▼            ▼                  │
                                        │   RunStore   EventSink · HTTP+SSE API ──┼──▶ PWA  (browser / phone)
   import "…/flow/app"                  │   CheckpointStore                      │
   a := app.New(cfg); a.Serve(ctx)      └───────────────────────────────────────┘
```

The only thing a workflow is *not* is data: because workflows are typed Go, adding or changing one produces a
new binary (§12 makes that safe). Everything else — personas, skills, prompts, backing stores, the server, the
UIs — is either data or a versioned dependency, and updates without re-authoring anything.

---

## 2. Repository layout and modules

One repository, **two modules**. The flagship library sits at the module root so its import path is the
shortest and cleanest (`github.com/bjaus/flow` → `flow.Do`), exactly as idiomatic Go libraries do. The runtime
is a nested module so that a developer importing the DSL to author workflows never drags in the heavy runtime
dependencies (the terminal UI framework, the SQLite driver, the embedded web assets).

```
github.com/bjaus/flow                      the repository — product name: "flow"
│
├── go.mod            MODULE 1  github.com/bjaus/flow          deps: cloudwego/eino only
├── *.go              package flow    →  flow.Do, flow.Agent, flow.Human, flow.Define, …
├── engine/           package engine  →  engine.Compile(ctx, wf, reg, store) → engine.Runnable
│                                        (the execution engine; the ONLY place the backend is referenced)
│
├── app/              MODULE 2  github.com/bjaus/flow/app       deps: everything heavy
│   ├── go.mod        requires github.com/bjaus/flow
│   ├── app.go        package app     →  app.New(cfg) (*App, error); (*App).Register; (*App).Serve(ctx) error
│   ├── config.go     app.Config, convenience constructors (app.SQLite, app.Gateway, …)
│   ├── server/       daemon: queue, worker, run lifecycle, HTTP + SSE API
│   ├── store/        CheckpointStore + RunStore + EventSink (SQLite default; pluggable)
│   ├── provider/     model provider (local gateway + a fake for tests)
│   ├── agent/        loads personas + skills from configured user/project paths; explicit reload
│   ├── tui/          Bubble Tea terminal client of the API
│   ├── web/          the PWA: static assets embedded via embed.FS, served by the daemon
│   └── cmd/flowd/    reference daemon binary (the example main a user copies)
│
├── go.work           dev-time glue:  use (. ./app)
├── README.md         quickstart: the ~10-line main, `go get`, `go build`
└── examples/         runnable example workflows (also used as documentation)
```

**Import surface — no stutter anywhere.**

| You want to… | import | you write |
|---|---|---|
| author a workflow | `github.com/bjaus/flow` | `flow.Do`, `flow.Agent`, `flow.Define` |
| compile & run one in-process (e.g. a test) | `github.com/bjaus/flow/engine` | `engine.Compile(…)` → `engine.Runnable` |
| build the durable daemon + UIs | `github.com/bjaus/flow/app` | `app.New(cfg).Register(wf); a.Serve(ctx)` |

**Module boundaries and why.** The root `flow` module (the DSL plus `engine`) depends only on the engine's
backing library and is therefore a clean, minimal public package: importing it to author or unit-test
workflows pulls in nothing else. The nested `flow/app` module carries all the runtime's dependencies and
`require`s the root module. A `go.work` file ties the two together for local development so a change in the DSL
is seen immediately by the runtime without a published version. On release, the two modules are tagged
independently (`vX.Y.Z` for the DSL, `app/vX.Y.Z` for the runtime) — standard Go multi-module tagging.

**Naming conventions to preserve.** The DSL package is `flow` and its verbs read as flow-of-data/control
(`flow.Then`, `flow.Loop`, `flow.Route`). The engine package is `engine` (not named after its backing
library), so the backend choice never leaks into the public API and a second backend could appear later
without a rename. The compiled-workflow type is `engine.Runnable`, leaving the word "app" to mean only the
runtime module. The runtime package is `app`, with a small, honest surface (`New`, `Register`, `Serve`).

---

## 3. The `flow` library (already built)

This section orients a runtime builder; it is not a re-specification. The public authoring surface is:

- **One core type, `flow.Step[In, Out]`** — a typed unit of execution, `In → Out`. Leaves and combinators are
  *both* steps, which is what makes composition unbounded.
- **Leaves** (do work): `Do` (a plain typed function — and the escape hatch for calling any eino component),
  `StateDo` (a `Do` with read/write access to shared graph state, for coordination edges can't carry), `Agent`
  (an LLM step typed on input *and* output, whose persona is resolved by name at runtime; if the persona has
  tools it runs a real ReAct loop, else a single completion), `Human` (suspends the run for a person, on the
  spine or nested inside a fan-out/bind/dispatch participant).
- **Combinators** (arrange steps): `Then`/`Seq` (sequencing), `Parallel`/`Map` (fan-out), `Reduce` (fan-in),
  `Route` (static branch), `Loop` + `Gate`/`StateGate`/`Guard` (convergence-terminated cycles), `Bind` (lift a
  typed step into a same-typed state spine), `Router` and `Network` (dynamic dispatch over a runtime-chosen or
  runtime-membership set of participants).
- **Entry point**: `flow.Define(name, desc, root)` produces a `flow.Workflow[In, Out]`. `Validate`, `Walk`,
  and `AgentNames` analyze a definition without any engine.
- **The engine**: `engine.Compile(ctx, wf, registry, checkpointStore, opts...)` lowers a workflow to
  `engine.Runnable[In, Out]`, which exposes all four eino run modes — `Invoke`, `Stream` (token-by-token),
  `Collect`, `Transform` — plus `Underlying()` to embed a workflow back into a hand-written eino graph. The
  extra `opts` pass straight through to eino's compiler (a custom serializer, interrupt breakpoints, a DAG
  trigger mode), and per-node run options target a step by its `Step.ID`. The engine is built on cloudwego/eino
  and inherits, for free: real goroutine concurrency, per-step tracing spans, token streaming (including inside
  concurrent fan-outs), and **durable interrupt/checkpoint/resume** that composes through arbitrary nesting.
  `engine.Registry` is the seam that resolves a persona name to an `engine.Persona` (model, system instruction,
  and tools). In short, flow deletes eino's boilerplate without hiding any of eino's seams (§5).

**Durability contract the runtime relies on.** On resume, the engine restores checkpointed *state*, not values
in flight on an edge. An interrupting step (a human gate) checkpoints its own input and, on resume, recovers it
plus the operator's decision. A completed sibling branch's output survives a suspend inside the checkpoint. The
runtime supplies a `CheckpointStore` at compile time and resumes a run by re-invoking with the same checkpoint
id and the decision data. Every custom type that rides a checkpoint is auto-registered by the DSL (the
workflow's input/output types, and the type each `Human` step carries), so authors need no manual registration.
This durability is the engine's own: per-step checkpoints land at engine node boundaries and on-boot recovery
resumes them from the `RunStore`. flow deliberately does **not** layer a separate durable-execution framework
(such as DBOS) on top — that would duplicate the checkpoint mechanism the engine already provides.

**What remains on the `flow` side: world-class API documentation.** The library is the public face of the
product and must be documented so that both humans and coding agents can use it without reading its internals:

- A package doc comment (`doc.go`) that states the mental model in a few paragraphs and shows the smallest
  complete example.
- Every exported identifier documented in full sentences beginning with its name, per Go convention; every
  combinator's doc states its type transformation and when to reach for it versus a neighbor.
- Runnable `Example` functions (`ExampleAgent`, `ExampleThen`, `ExampleLoop`, a full `Example` workflow) that
  appear on the generated documentation and are executed by `go test`, so they cannot rot.
- A README whose first screen is the ~10-line quickstart (§13), followed by a tour of the combinators and a
  link to the examples directory.
- An `AGENTS.md` at the repo root describing the package to coding agents: the one core type, the leaf/
  combinator split, the four composition tools and when each applies, and the persona-vs-task invariant.

---

## 4. The runtime: architecture and the `app` API

The runtime is an ordinary three-tier application with exactly one non-ordinary constraint (workflows compile
in, handled in §12). A single **daemon** process owns all state and exposes one HTTP+SSE API; two **clients**
(the TUI and the PWA) render it. A developer's binary bundles the daemon and both clients.

### 4.1 The mountable `app` API

The developer's `main` constructs an `App`, registers workflows, and serves. Construction can fail, so it
returns an error; serving blocks until its context is canceled and returns an error. There is no all-in-one
"main" method — argument parsing and process lifecycle belong to the developer.

```go
package app

// New builds a runtime from a Config, applying local defaults for any port left nil. It validates the
// configuration (reachable store, resolvable agents directory, …) and returns an error rather than panicking.
func New(cfg Config) (*App, error)

// Register compiles a workflow and adds it to the runtime under its name. It returns an error for a duplicate
// name or a definition that fails validation (duplicate step ids, edge-type mismatches).
func (a *App) Register(wf AnyWorkflow) error

// Serve runs the daemon: it starts the HTTP+SSE server, the queue worker, and (optionally) resumes parked
// runs, then blocks. When ctx is canceled it performs a graceful drain (§12.1) and returns. Any fatal startup
// or runtime error is returned.
func (a *App) Serve(ctx context.Context) error
```

`AnyWorkflow` is the type-erased form of `flow.Workflow[In, Out]` (the DSL already carries erased node types
internally); `Register` recovers the concrete input/output types from the definition for compilation and for
decoding run inputs.

A canonical `main`:

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    a, err := app.New(app.Config{
        Store:    app.SQLite("flow.db"),                       // default if nil
        Provider: app.Gateway(os.Getenv("FLOW_GATEWAY_URL")),  // OpenAI-compatible endpoint
        Agents:   []string{"./company/agents"},                 // optional path override
        Skills:   []string{"./company/skills"},                 // optional path override
        Listen:   ":7788",
    })
    if err != nil {
        log.Fatalf("init: %v", err)
    }
    if err := a.Register(workflows.Triage()); err != nil {     // workflows.Triage() flow.Workflow[Ticket, Report]
        log.Fatalf("register: %v", err)
    }
    if err := a.Serve(ctx); err != nil {                       // blocks; SIGTERM → drain → return
        log.Fatalf("serve: %v", err)
    }
}
```

For the smallest possible start, a package-level convenience constructs defaults and serves in one call:

```go
// Serve builds an App with default config (SQLite ./flow.db, gateway from $FLOW_GATEWAY_URL, and the
// user/project .flow configuration), registers the given workflows, and runs until ctx is canceled.
func Serve(ctx context.Context, workflows ...AnyWorkflow) error
```

### 4.2 Clients and subcommands

The TUI and the PWA are pure clients of the runtime; neither needs workflows compiled in. The daemon exposes
**two presentation layers over one core** (the RunStore, the EventSink, and the worker): a **JSON HTTP+SSE API
under `/api`**, consumed by the TUI and any programmatic client (§6.3), and a **server-rendered htmx web app at
`/`** (§10). Both layers read the same runs and the same event stream; they differ only in representation (JSON
versus HTML fragments). The TUI is launched as a client:

```go
// TUI runs the terminal client against a daemon endpoint. It is a thin SSE/HTTP consumer.
func (a *App) TUI(ctx context.Context, endpoint string) error
```

The **command-line client is a first-class client in its own right** (§11) — for triggering, listing,
approving, and tailing runs from a shell or a script. The runtime exposes its command tree so a developer can
mount it in their own `main` or use it ready-made on the reference binary:

```go
// CLI returns the cobra command tree (serve, workflows, runs, tui) bound to this App, so a developer can mount
// the full command-line surface in their own main instead of writing one. See §11.
func (a *App) CLI() *cobra.Command
```

---

## 5. Pluggable ports

Every backing technology is an interface with a zero-config local default. Swapping one never touches a
workflow. The interfaces live in `app`; the default implementations live in sub-packages, surfaced through
convenience constructors on `app` (`app.SQLite`, `app.Gateway`, `app.FakeProvider`).

```go
// CheckpointStore persists engine checkpoints for durable interrupt/resume. Default: SQLite.
type CheckpointStore interface {
    Get(ctx context.Context, id string) (data []byte, ok bool, err error)
    Set(ctx context.Context, id string, data []byte) error
    Delete(ctx context.Context, id string) error
}

// RunStore is the durable record of runs and the work queue. Default: SQLite.
type RunStore interface {
    Save(ctx context.Context, r *Run) error
    Get(ctx context.Context, id string) (*Run, error)
    List(ctx context.Context, f RunFilter) ([]*Run, error)
    Claim(ctx context.Context) (*Run, error)   // dequeue the next runnable run; nil if none
}

// EventSink is the observability stream: every meaningful event during a run. Default: in-process pub/sub with
// a SQLite log so late subscribers can replay a run's story.
type EventSink interface {
    Publish(e Event)
    Subscribe(runID string) (events <-chan Event, cancel func())   // runID == "" subscribes to all runs
}

// Provider builds the chat model a persona uses. Default: an OpenAI-compatible gateway. A fake implementation
// (returning scripted outputs) is what makes any workflow deterministically testable with zero tokens.
type Provider interface {
    Model(ctx context.Context, persona Persona) (engine.ChatModel, error)
}

// AgentRegistry resolves persona names to Personas loaded from markdown. Default: the configured
// ~/.flow + project .flow loader (§7), whose snapshot changes only on explicit reload.
type AgentRegistry interface {
    Persona(name string) (Persona, bool)
}

// Tracer observes execution for external observability, receiving a span per run, per step, and per agent
// call (model, token counts, prompt/response, latency). Default: a no-op (zero overhead). flow ships an
// OpenTelemetry implementation (GenAI/OpenInference semantic conventions) whose OTLP exporter is set by
// config — point it at Arize Phoenix, Langfuse, Jaeger, or any OTLP backend (§6.5). Implement Tracer
// directly for any other sink.
type Tracer interface {
    StartRun(ctx context.Context, r *Run) (context.Context, Span)
    StartStep(ctx context.Context, runID, label, kind string) (context.Context, Span)
    StartAgent(ctx context.Context, runID, persona string) (context.Context, Span)
}
type Span interface {
    Set(attrs ...Attr) // GenAI/OpenInference attributes (model, tokens, tool calls, …)
    End(err error)
}
```

The runtime composes an `AgentRegistry` and a `Provider` into the `engine.Registry` a compiled workflow needs:
for each persona name the workflow references, it loads the markdown persona, builds a chat model through the
provider, and returns the model plus the persona's system instruction.

**Defaults.** SQLite is the default for all three stores (a single pure-Go, no-CGO driver, opened with
`MaxOpenConns(1)` and WAL so the single worker never contends). The event bus never blocks a run (it drops for
a slow subscriber and relies on the SQLite log for replay). The provider defaults to a local gateway whose base
URL and key come from the environment. Postgres, Redis, or a hosted model endpoint are drop-in alternatives
implementing the same interfaces — this is the path from a laptop to a cluster with no workflow changes.

**Secrets.** The runtime reads credentials (the gateway key, any store DSN password) from environment variables
only; it never accepts them inline in code or config files, never logs them, and the repository's `.env` is
git-ignored.

**Principle: preserve every seam the engine exposes.** flow's job is to delete boilerplate, not to foreclose
choices. Wherever cloudwego/eino leaves a decision open — the chat model, tools, callbacks and tracing, the
checkpoint store, serialization — flow surfaces it as a port with a sensible local default, never a hardcoded
one. A default is a convenience, not a constraint: a user can always supply their own implementation, or drop
down to the underlying eino seam directly. flow removes the paperwork of using eino without removing any of its
flexibility; that is the whole value proposition, and it applies to observability (the `Tracer`) exactly as it
does to every other port.

---

## 6. The daemon (server)

The daemon is a long-running host with four parts: a **queue**, a **single worker**, an **event bus**, and an
**HTTP+SSE API**. It is workflow-agnostic — it runs whatever workflows the binary registered.

### 6.1 Run lifecycle

A **Run** is one execution of a registered workflow on an input. Its states:

| State | Meaning |
|---|---|
| `queued` | accepted, waiting for the worker |
| `running` | the worker is executing a step |
| `awaiting_review` | suspended at a human gate; waiting for a decision |
| `parked` | drained at a step boundary during shutdown; resumable (§12) |
| `needs_migration` | its workflow's shape changed under it; awaiting an operator decision (§12) |
| `succeeded` / `failed` / `canceled` | terminal |

A trigger enqueues a run (`queued`). The worker claims it (`running`) and drives the compiled
`engine.Runnable`, publishing events as steps start and finish. A human gate suspends the run into
`awaiting_review`; when a decision arrives the run returns to `queued` and the worker resumes it from the
checkpoint. Completion is terminal. On daemon startup, any run left `running` or `parked` is resumed from its
last checkpoint (crash recovery and planned-restart recovery are the same path).

### 6.2 The single worker

One worker processes the queue: it drives one workflow's model calls at a time, which keeps local resource use
(tokens, CPU, tool subprocesses) predictable on a single machine. Concurrency *within* a run — a `Parallel`
fan-out, several agents at once — is real goroutine concurrency inside the engine; it is the *cross-run*
scheduling that is serialized. A run that suspends at a gate is set aside so the worker moves on rather than
blocking on a human. (A future `Config.Workers > 1` can parallelize independent runs; the single-worker default
is the predictable baseline.)

### 6.3 HTTP + SSE API

The daemon exposes a **JSON API under `/api`** for the TUI and programmatic clients; the htmx web app's own `/`
and `/ui` routes are covered in §10, and both layers read the same RunStore and EventSink. JSON over HTTP for
commands and queries; Server-Sent Events for the live stream.

| Method & path | Purpose |
|---|---|
| `GET /api/workflows` | list registered workflows (name, description, input/output types) |
| `POST /api/runs` | trigger a run: `{workflow, input}` → `{id}` |
| `GET /api/runs` | list runs, filterable by workflow, status, and parent run (`?parent=`) |
| `GET /api/runs/{id}` | fetch one run (status, input, result/error, timing) |
| `GET /api/runs/{id}/events` | **SSE** stream of that run's events (replays history, then live) |
| `GET /api/events` | **SSE** stream of all runs' events |
| `POST /api/runs/{id}/decision` | submit a human decision: `{approved, feedback}` |
| `POST /api/runs/{id}/cancel` | request cancellation (cooperative, at the next step boundary) |
| `POST /api/runs/{id}/migration` | resolve a `needs_migration` run: `{action: restart|abandon|finish_on_previous}` (§12) |

### 6.4 Events

Events are the run's story, consumed identically by the TUI and the PWA. The kinds:

- `run.started`, `run.finished` (with status and result or error)
- `step.started`, `step.finished` — carry the step's label and kind, so a client can render the pipeline as it
  unfolds
- `agent.token` — a streamed token delta, tagged with the agent's node label, so a client shows an agent
  "typing" live (forwarded from the engine's streaming callback, including inside concurrent fan-outs)
- `gate.reached` — a human gate opened, carrying what the operator must see
- `decision.applied` — a decision was folded in and the run resumed
- `run.parked`, `run.resumed` — drain/redeploy transitions (§12)
- `trigger.skipped` — a scheduled trigger (§6.6) declined to enqueue, with the reason

Each event has a monotonic per-run sequence number so a late subscriber replays the log from the store and then
follows live without gaps. The same event model feeds both presentation layers: the `/api` SSE stream carries
events as JSON; the web app's SSE stream (§10) carries the same events rendered as HTML fragments for htmx to
swap into the page.

### 6.5 Observability (OpenTelemetry)

The `EventSink` (§6.4) is flow's *internal* stream that drives the clients live. **Observability** is the
separate, *external* story — tracing, evaluation, and cost analysis in a dedicated backend — and it goes
through the `Tracer` port (§5), which defaults to a no-op so nothing is assumed and there is zero overhead
until a user opts in.

flow instruments each run as an **OpenTelemetry span tree** — a span per run, per step, and per agent LLM call
— using the **GenAI / OpenInference semantic conventions** (model, token counts, prompt/response, tool calls,
latency), so LLM-aware backends render agent traces without custom mapping. Both the span tree and the
`EventSink` derive from the same engine callbacks (the ones that also feed token streaming), so it is one
instrumentation layer fanning out to two consumers.

flow ships one `Tracer` implementation — an OpenTelemetry exporter configured by environment (OTLP endpoint +
headers) — and the user chooses the destination:

- **Arize Phoenix** — self-hostable, OTLP-native, OpenInference conventions; runs locally, ideal for the
  local-first default. Point the exporter at its `/v1/traces` endpoint.
- **Langfuse** — self-host or cloud; an OTLP ingestion endpoint plus prompt/eval/cost features. Same exporter,
  its endpoint and auth headers.
- **Any other OTLP backend** — Jaeger, Grafana Tempo, Honeycomb, a collector — works with the same exporter.

No backend is required, none is bundled, and a user who wants something other than OpenTelemetry implements the
`Tracer` port directly. This is the pluggability principle of §5 applied to observability: rich insight when
you want it, nothing forced when you don't.

### 6.6 Scheduled triggers

`Config.Triggers` declares cron-scheduled runs. Each `app.Trigger` names a registered workflow, a standard
5-field cron spec (parsed with `github.com/robfig/cron/v3`), a canned JSON input, and an optional name
(defaulting to `workflow@spec`). `New` rejects an invalid spec or a duplicate name; `Serve` rejects a trigger
whose workflow is not registered or whose input does not decode to the workflow's input type. On each firing
the daemon enqueues a run through the same path as `POST /api/runs`, stamping the run with the trigger's name
(`Run.Trigger`) so scheduled runs are attributable. A firing is skipped — publishing a `trigger.skipped` event
with the reason — while the daemon is draining or while the trigger's previous run is still `queued`,
`running`, or `awaiting_review`, so a slow workflow never piles up behind its own schedule. The scheduler
stops with the worker on shutdown.

### 6.7 Child runs (workflow-calls-workflow)

A running workflow can spawn runs of other registered workflows and await their results, so composite
pipelines (a planning workflow spawning one implementation run per issue) are first-class. Workflows are
compiled Go that never sees the `App`, so the worker injects an `app.Spawner` into every run's context;
inside a `Do` step it is recovered with `app.SpawnerFrom(ctx)`:

- `Spawn(ctx, workflow, input)` marshals the input, validates it against the target workflow's input type,
  and enqueues a child run stamped with the calling run's id (`Run.ParentID`), through the same path as
  `POST /api/runs`. A spawn deeper than `app.MaxSpawnDepth` (8) ancestors is rejected, so accidental
  infinite recursion errors instead of looping.
- `Await(ctx, runID)` blocks until the child is terminal, returning its result; a failed or canceled child
  surfaces as an error. `app.SpawnAwait(ctx, workflow, input)` does both in one call.

Await never deadlocks the single worker (§6.2): the parent occupies the worker while a queued child would
otherwise wait forever, so Await claims the queued child directly and executes it **inline in the parent's
worker slot** (re-entrant execute). The tradeoff — stated in the `Await` doc comment — is that the parent
stays `running` while the child runs or waits at a human gate, and the inline chain is not independently
resumable across a restart; the seam is left for a future park-the-parent scheme without changing the API.

`Run.ParentID` is persisted, exposed as `parent_id` in the run JSON, and filterable (`GET /api/runs?parent=`,
`runs list --parent`), so clients can render run families. Canceling a parent cancels its non-terminal
descendants.

---

## 7. Agents and skills as data

Personas and skills are **markdown files, resolved by name**, not hard-coded Go. This is what lets an operator
edit an agent's behavior without a rebuild, and it enforces the invariant that an **agent file is a persona
while the runtime prompt is the task**.

- A **persona** file carries frontmatter (`name`, `profile`, `roles`, `tools`, `skills`) and a system instruction
  in its body. `profile` resolves to an ordered model ladder in configuration. Roles contribute reusable tool
  grants and skills; inline `tools` union one-off grants. No role or tool means deny-all. A **skill** follows the
  reusable `SKILL.md` convention.
- Configuration merges `~/.flow/config.yml` then `.flow/config.yml` (project wins); `FLOW_CONFIG` replaces the
  project config path. `agents` and `skills` are arrays of roots, defaulting to user and project
  `~/.flow/{agents,skills}` + `.flow/{agents,skills}`. Programmatic `Config.Agents`/`Config.Skills` arrays can
  override them.
- The registry watches config, agent, and skill files and marks its active snapshot **dirty**, but does not
  mutate a daemon underneath in-flight work. CLI, TUI, web, and HTTP API expose an explicit reload. The new
  snapshot is used by the next agent step; reload errors preserve the last valid snapshot.

The runtime ships a small set of default personas and skills so a freshly scaffolded project runs immediately;
they are ordinary files the developer edits or replaces.

---

## 8. The model provider

An `Agent` step's actual LLM call goes through the `Provider` port, which builds a chat model for a persona.

- The **default** provider targets an **OpenAI-compatible gateway** (base URL and key from the environment),
  which lets an operator point every workflow at a local model server or a hosted endpoint without code
  changes, and centralizes rate/cost control.
- A persona names an abstract profile and roles. Configuration resolves the profile to a primary model plus
  ordered fallbacks and roles to tool grants. The app supplies only granted tools to the native ReAct loop and
  guards each invocation against its resource/command pattern; shell wildcards cannot consume chaining or
  substitution metacharacters. The provider walks fallback models on failures. An `Agent`'s typed output defines
  a structured-output contract the runtime decodes and, on a malformed response, retries.
- A **fake provider** returns scripted structured outputs keyed by persona name and input. It is the backbone
  of the testing strategy (§14): any workflow, including the hardest multi-agent ones, runs deterministically
  with zero tokens.

---

## 9. The terminal UI (TUI)

A Bubble Tea terminal client of the daemon's API — an SSE/HTTP consumer with no engine or store dependency of
its own. It renders:

- a **run list**, grouped by status and filterable, updating live;
- a **pipeline** view derived from the `step.*` event stream, showing each step as it starts and finishes;
- an expandable **transcript**: per-agent output, streamed token-by-token from `agent.token` events, so the
  operator watches agents work in real time;
- a **gate prompt**: when a run reaches `awaiting_review`, the operator approves or returns feedback inline,
  which posts a decision to the API.

The TUI connects to a running daemon (local by default, any endpoint by flag), so the same client works against
a laptop daemon today and a remote one later.

---

## 10. The web app (PWA)

A **mobile-friendly, installable** web client built with **htmx** — server-rendered hypermedia, no client-side
framework and no Node build. htmx is the right fit here because the UI is a live dashboard: its SSE extension
swaps server-rendered HTML fragments into the page as events arrive, and its `hx-post` handles actions, so the
daemon stays the single source of truth and there is nothing to assemble or transpile. The web app keeps the
single-binary, no-external-toolchain ethos and stays in lockstep with the TUI because both consume one event
model.

### 10.1 Composition

The `web/` package is a **server-side renderer** living in the daemon:

- **Templates.** Go `html/template` files rendered server-side; small, composable partials (a run row, a
  pipeline step, a transcript line, a gate prompt) so the SSE stream can push exactly the fragment that
  changed. (`templ` is an acceptable Go-native alternative if typed templates are wanted; it is Go codegen, not
  a Node toolchain. Start with `html/template`.)
- **htmx, vendored and embedded.** `htmx.min.js` and its SSE extension are checked into `web/static/` and
  embedded via `embed.FS` — never loaded from a CDN, so the app works offline and installs cleanly. The daemon
  serves the app shell at `/` and the assets under `/static`.
- **Routes under `/ui`.** Action endpoints return HTML fragments: `POST /ui/runs` (trigger), `POST
  /ui/runs/{id}/decision` (approve/return feedback) invoked from `hx-post` forms; `GET /ui/runs`,
  `/ui/runs/{id}` render the list and detail partials.
- **Live via SSE.** A single `GET /ui/events` SSE endpoint subscribes to the `EventSink` in-process and emits
  the run's story as **named HTML-fragment events**. The page connects once with
  `hx-ext="sse" sse-connect="/ui/events"`, and elements swap on named events —
  `sse-swap="step.started"`, `sse-swap="agent.token"` (appended to the streaming transcript),
  `sse-swap="gate.reached"`, and so on — mirroring §6.4's event kinds, rendered as HTML rather than JSON.

### 10.2 Views

The web app renders the same four things as the TUI: the live **run list** (grouped by status), the **pipeline**
(built from `step.*` fragments), the streamed **transcript** (`agent.token` fragments appended live so an agent
appears to type), and the **gate prompt** (an `hx-post` form that submits a decision). Mobile-first CSS (a
single embedded stylesheet, no framework) makes it usable on a phone.

### 10.3 Installability and the cloud seam

A `manifest.webmanifest` and a service worker make it installable on a phone's home screen: the service worker
caches the app shell and the embedded htmx/CSS assets so the shell loads instantly and offline (live data still
requires the daemon). This is deliberately the seam toward a future cloud deployment — today the daemon runs on
`localhost`; tomorrow the identical app points at a daemon behind a URL. Authentication and multi-tenant
concerns are out of scope for the local-first v1 and belong to that later phase; the routes are already split
(`/api` for machines, `/ui` for the browser) to leave room for it.

---

## 11. The command-line client (CLI)

A scriptable client of the daemon's `/api` — the third way to interface with the runtime, alongside the
interactive TUI and the browser PWA. Where the TUI is for watching and the PWA is for reach, the **CLI is for
composition**: listing, triggering, approving, and tailing runs from a shell, a script, cron, or CI, with
output that pipes cleanly.

- **A pure client.** The client commands need only a daemon endpoint (`--endpoint`, default
  `http://localhost:7788`), not workflows compiled in — they call `/api`. So they work from the developer's
  daemon binary *and* from the standalone `flow` tool (§13) against any running daemon, without building
  anything: `flow runs list --endpoint …` just works.
- **The command tree.** The full tree is `app.CLI(a) *cobra.Command`, for a developer to mount in their own
  `main` or use ready-made on the reference binary:
  - **server:** `serve` — runs the daemon; the one command that needs an `App` (workflows compiled in).
  - **workflows:** `workflows list` — the registered workflows and their input/output types.
  - **runs:** `runs trigger <workflow> --input <json|@file>` · `runs list [--status <s>] [--workflow <w>]` ·
    `runs get <id>` · `runs watch <id>` (tail the SSE stream, including streamed tokens) ·
    `runs approve <id> [--feedback <text>]` · `runs cancel <id>` ·
    `runs migrate <id> <restart|abandon|finish_on_previous>` (§12).
  - **tui:** `tui [--endpoint …]` — launch the terminal client.

  The pure-client subset (`workflows`, `runs`, `tui`) is also available **App-free** as
  `app.ClientCLI() *cobra.Command` — endpoint only, no workflows required — which is what lets the standalone
  `flow` tool (§13) act as a client of any running daemon.
- **Script-friendly output.** Read commands print a human table by default and full JSON under `--json` (pipe
  to `jq`); commands exit non-zero on a failed or not-found run so the CLI composes in automation.

Because it consumes the same `/api` and event model as the other clients, the CLI stays in lockstep by
construction and needs no server support beyond what §6.3 already defines.

---

## 12. Safe redeployment and versioning

Workflows are typed Go, so changing one produces a new binary, and the runs are long-lived — an agent may work
for many minutes, hold a git worktree, or sit at a human gate for days. Deploying a new binary must therefore
never clobber in-flight work, waste spent tokens, or orphan external state. Two independent problems, each with
a clean answer.

### 12.1 Stopping safely — drain, never kill

Durability already provides **checkpoints at step boundaries**, so a safe stopping point is every gap *between*
steps. On `SIGTERM` (or context cancellation) the daemon:

1. stops claiming new runs and stops starting the *next* step of any run;
2. lets every currently-executing step **finish** — the in-flight model call completes, so **no spent tokens
   are discarded**; the daemon only declines to start the next step;
3. checkpoints each run at that boundary and marks it `parked`;
4. exits once everything is parked, or a configurable drain timeout fires.

Drain latency is bounded by **one uninterruptible unit — a single model call or tool execution — not a whole
workflow**. Agent steps are lowered so each model/tool call in a tool loop is its own checkpoint boundary, so
even a long-running agent parks within one call of the signal. Human gates are natural stopping points:
state-only, with no active step.

**External side effects** (git worktrees, files) stay consistent because parking happens *between* steps: the
worktree is in whatever state the last completed step left it. Two rules keep this robust under even an
ungraceful kill:

- steps that touch external state are **idempotent on re-entry** and reference their artifacts **by path in
  checkpointed state**, so whichever binary resumes finds them;
- a **run-id-keyed worktree/session manager** makes creation idempotent (reuse if present) and ties cleanup to
  run completion or abandonment, so no drain or crash orphans one.

On any startup — planned or after a crash — the daemon scans the store for `running`/`parked` runs and resumes
the compatible ones from their last checkpoint. A crash is simply an ungraceful drain, recovered by the same
path (a hard-killed step re-runs from its previous checkpoint, which is safe precisely because such steps are
idempotent).

### 12.2 Resuming against changed code — the fingerprint and three tiers

Whether a parked run can resume on a new binary depends entirely on whether the workflow's **shape** changed.
Because a definition is walkable data, the runtime computes a **structural fingerprint** — a hash of node
kinds, edges, and types — and stores it on each run. Three tiers follow:

**Tier 0 — not a binary change.** Editing configuration, a persona, skill, or prompt is *data* (§7): the daemon
detects it and an operator explicitly reloads it, with no rebuild and no deployment. A large share of "changing a workflow" lands
here and never engages this machinery.

**Tier 1 — binary changed, workflow shape unchanged.** Fixing a server bug, improving a UI, upgrading the
engine, or refining a step's internals leaves the fingerprint identical, so the checkpoint is still valid. This
is **drain + resume in place, fully lossless** (§12.1 and nothing more), and it is the overwhelming majority of
deploys.

**Tier 2 — the workflow's shape changed** (added, removed, or reordered steps; changed topology; an
incompatible state type). The checkpoint encodes the old shape, so resuming an in-flight run onto the new shape
is unsafe, and the runtime **never does it silently**. A fingerprint mismatch pins the run to its original
shape:

- **new runs** use the new shape immediately;
- **in-flight runs of the changed workflow** are handled explicitly, never clobbered. The default mechanism on
  one machine is a shared store with a `fingerprint` column and a brief **previous-binary drain process**: a
  supervisor keeps the prior binary running in drain-only mode, claiming *only* its pinned runs and taking no
  new work, until they complete and it exits; the new binary handles everything else. This is the
  version-pinned "run the previous version to completion" model, shrunk to a single box.
- if keeping the old binary alive is undesirable (a pinned run sits at a human gate indefinitely), the run is
  marked `needs_migration` and surfaced to the operator, who chooses via `POST /api/runs/{id}/migration`:
  finish on the previous binary, restart on the new shape (explicit progress loss), or abandon. Every choice is
  visible in the UI; none is automatic.

The deploy command states plainly what will happen — e.g. *"3 runs of `triage` are in flight on the current
shape; new runs use the new shape and the 3 finish on the previous binary"* — and carries it out.

### 12.3 The honest tradeoff

A structural change cannot be applied retroactively to a run already executing, because its checkpoint encodes
the old shape — the same wall every durable-execution system meets. The escape hatches are to run the old code
to completion (the default) or to restart the run. The third option other systems use — snapshotting the
workflow as reloadable *data* — is unavailable here by design: workflow behavior is Go closures, not
serializable data, which is the price of the compile-time type-safety ranked first among the north stars. That
price is billed only on Tier-2 changes to a workflow that currently has in-flight runs — a narrow, visible,
infrequent case. Everything else is hot (Tier 0) or lossless (Tier 1).

**A later refinement, not built first.** The whole-graph fingerprint is conservative: a change that touches
only a step a given run has not yet reached is in fact safe to continue on the new shape. A positional check —
does the diff affect the graph at or behind this run's current position? — would let more in-flight runs
migrate cleanly. Ship the conservative fingerprint first; add the positional refinement once the base is solid.

---

## 13. Distribution and consumption

**The package is the product, and the package is the update channel.** flow is consumed the way any Go library
is: `go get`, write a little code, `go build`. There is no fork, no clone, no template repository, and no
frozen snapshot.

To build and run a workflow project:

```
mkdir my-workflows && cd my-workflows
go mod init my-workflows
go get github.com/bjaus/flow/app        # brings in github.com/bjaus/flow transitively
# write a ~10-line main.go (§4.1) and your workflow files
go build && ./my-workflows serve
```

`go mod init` creates a **local module** — a directory with a `go.mod`. It is not a hosted repository, not a
fork, not a clone; it is simply where the developer's code lives, as every Go program requires. Nothing is
pushed anywhere unless the developer chooses to.

**Updates are free and on the developer's schedule.** Everything except the developer's own ~10-line `main` and
their workflow code is a **versioned dependency**. A fix or feature shipped to the server, either UI, the
engine, or a combinator reaches every project with `go get -u github.com/bjaus/flow/app` — semver-resolved and
reproducible via `go.sum`. The one thing that cannot arrive as a free update is a *new workflow*, because a
workflow is typed Go that must be compiled in; that is the single, deliberate cost of compile-time type-safety,
and §12 makes paying it safe.

**The app, TUI, and PWA are bundled.** Importing `flow/app` links the server, the terminal UI, and the
embedded web app into the developer's binary; there is nothing to assemble.

**Onboarding.** The primary onboarding artifact is a **copy-paste example in the README** plus the runnable
`examples/` — sufficient, and how comparable Go frameworks onboard. If first-run friction ever justifies it, an
optional scaffolder runnable without installation writes the starter into the *current directory* (no hosted
repository involved):

```
go run github.com/bjaus/flow/cmd/flow@latest init     # writes main.go, .flow/config.yml + agents/skills, a justfile
```

Build the scaffolder only if the example proves insufficient; it changes nothing about how updates flow.

---

## 14. Determinism and testing

Determinism is a requirement, not an aspiration: every side effect (model calls, gate decisions) passes through
a port, so a fake makes any workflow fully reproducible.

- Inject a **fake `Provider`** returning scripted structured outputs keyed by persona name and input.
- Run any workflow — including the hardest multi-agent ones — with **zero tokens**, asserting both the
  **enacted topology** (which steps ran, in what order, with what membership) and the final typed result.
- Every combinator and every hard multi-agent paradigm ships with such a test as its acceptance criterion; for
  dynamic dispatch and runtime membership, tests cover selection, membership change, and clean drain.
- **Durable HITL** is tested by invoking to an interrupt, then resuming from the checkpoint store with a
  scripted decision and asserting completion — proving interrupt → checkpoint → resume end to end.
- **Redeployment** is tested by parking a run mid-flight, then resuming it from a fresh daemon instance (Tier
  1), and by asserting a fingerprint mismatch drives a workflow-shape change to `needs_migration` rather than a
  silent resume (Tier 2).

The runtime's HTTP and SSE layers are tested against an in-memory store and the fake provider, so the full
trigger → run → stream → approve → resume path runs in a unit test without a network or a model.

---

## 15. Development workflow

These conventions make the runtime fast to iterate on and, crucially, let a coding agent verify its own work
without a human watching a terminal. Adopt them from Phase 0.

### 15.1 The `justfile`

A [`just`](https://github.com/casey/just) recipe file at the repo root is the single entry point for every
common task. The receiving agent runs recipes rather than remembering commands; every phase's *Verify* step
below is a `just` recipe.

```make
set dotenv-load                     # load ./.env (git-ignored; secrets by env-var reference only)

# build + lint + test across BOTH modules — must be clean before any commit
check: fmt
    go build ./...
    cd app && go build ./...
    golangci-lint run
    cd app && golangci-lint run
    go test ./...
    cd app && go test ./...

fmt:
    gofmt -w .

test path=".":                      # focused tests, e.g. `just test ./app/store/...`
    go test {{path}}

# run the reference daemon with live reload; --demo self-seeds a sample run.
# NOTE: use watchexec, NOT air/wgo — TUI-capable binaries render blank under those watchers.
dev:
    watchexec -r -e go,html,css -- go run ./app/cmd/flowd serve --demo

# run the terminal UI against a running daemon
tui endpoint="http://localhost:7788":
    go run ./app/cmd/flowd tui --endpoint {{endpoint}}

# open the web app (macOS `open`; Linux `xdg-open`)
web endpoint="http://localhost:7788":
    open {{endpoint}}

# start the local OpenAI-compatible model gateway on :4000 (key from .env)
gateway:
    litellm --config ./litellm.yaml --port 4000

# render a scripted TUI scenario to a shareable gif (see §15.2)
tape name:
    vhs tui/tapes/{{name}}.tape
```

`just check` is the merge gate: it must build both modules, report **zero** `golangci-lint` issues, and pass
all tests. Run it green before finishing any phase and commit per phase (single-line messages, no attribution).

### 15.2 Seeing the TUI: `vhs`

A remote agent cannot see the operator's terminal, so the TUI is iterated on by **rendering it to a GIF** with
[`vhs`](https://github.com/charmbracelet/vhs) and inspecting the result. Keep scripted scenarios (`.tape` files)
under `tui/tapes/` and render them with `just tape <name>`. A tape drives the TUI through a fixed scenario
against a `--demo` daemon (which self-seeds a run that is mid-pipeline and one awaiting review), so the GIF
shows real content:

```tape
# tui/tapes/approve.tape
Output tui/tapes/approve.gif
Set FontSize 16
Set Width 1280
Set Height 720

Type "go run ./app/cmd/flowd tui" Enter
Sleep 2s
Down Down                # move to the run awaiting review
Enter                    # open it
Sleep 1s
Type "a"                 # approve the gate
Sleep 2s
```

Workflow: change the TUI → `just tape approve` → open the GIF → refine layout/theme → repeat. Commit a couple
of canonical tapes so the rendering is reproducible. For the **web app**, open it in a browser (`just web`); an
agent with browser automation can navigate and screenshot `/` to refine layout the same way.

### 15.3 Other conventions

- **Determinism first.** Build and test every workflow and combinator against the **fake provider** (zero
  tokens) before pointing anything at the gateway. The gateway is only for end-to-end smoke checks.
- **The gateway.** An OpenAI-compatible endpoint on `:4000/v1`; workflows read `FLOW_GATEWAY_URL` and the key
  from the environment. `just gateway` starts it. Never inline or log a key; `.env` is git-ignored.
- **Live reload caveat (worth repeating).** `air`/`wgo` blank out a TUI; use `watchexec` for anything that may
  render a terminal UI. The daemon alone reloads fine under any watcher.
- **`--demo` mode.** The reference daemon takes a `--demo` flag that registers a couple of sample workflows and
  seeds runs (one streaming mid-pipeline, one awaiting review) so the TUI, the web app, and `vhs` always have
  something live to show without a real model.

---

## 16. Phased execution plan

Work the phases **in order**. Each is independently shippable, ends with a concrete *Done when* checklist and a
*Verify* command, and should be committed once `just check` is green. The `flow` module (DSL + `engine`) is a
finished dependency throughout — do not modify it except for the one-time renames in Phase 0. Every phase up to
7 is testable with the **fake provider and no network**, so the whole runtime can be built and verified with
zero tokens.

### Phase 0 — Repository bootstrap and tooling

- **Goal.** A two-module skeleton that compiles, with the dev loop working.
- **Build.**
  - Place the ready `flow` DSL + engine at the repo root (module `github.com/bjaus/flow`). Apply the one-time
    renames: package/dir `eino` → `engine`, and the compiled type `App` → `Runnable`; update all references.
  - Create the nested module `app/` (`github.com/bjaus/flow/app`) requiring the root module; add a `go.work`
    with `use (. ./app)`.
  - Add empty package stubs for `app/{server,store,provider,agent,tui,web}` and `app/cmd/flowd`.
  - Add the `justfile` (§15.1), a `.golangci.yml` (enable the standard set; treat issues as errors),
    `.env.example`, `.gitignore` (include `.env`), and a README skeleton.
- **Done when.** Both modules build; `golangci-lint` reports zero issues on the skeleton; `go.work` resolves the
  local `flow` dependency.
- **Verify.** `just check`.

### Phase 1 — Ports and the SQLite store

- **Goal.** Durable persistence, an event bus, and a fake model provider — the foundation everything binds to.
- **Build.**
  - In `app`: the `Run` struct (id, workflow, fingerprint, status, input, result, error, timestamps), the run
    `status` constants (§6.1), `Event` and its kinds (§6.4), `RunFilter`, `Persona`, and the port interfaces
    `CheckpointStore`, `RunStore`, `EventSink`, `Provider`, `AgentRegistry`, and `Tracer`/`Span` (§5), the last
    with a **no-op default** so nothing is emitted until a real tracer is configured.
  - `app/store`: SQLite implementations of all three stores in one database — single connection,
    `MaxOpenConns(1)`, WAL, embedded schema migrations. `RunStore.Claim` dequeues FIFO the next `queued`/
    resumable run atomically. The `EventSink` is in-process pub/sub with a SQLite event log for history replay.
  - `app/provider`: `FakeProvider` returning scripted structured outputs keyed by persona name + input.
  - Convenience constructors on `app`: `app.SQLite(path)`, `app.FakeProvider(script)`.
- **Done when.** Unit tests pass for: checkpoint set/get/delete round-trip; run save/get/list (status + workflow
  filters) and atomic `Claim`; event publish → subscribe delivering history-then-live in order with per-run
  sequence numbers; fake provider returning scripted output.
- **Verify.** `just test ./app/store/...` and `just test ./app/provider/...`.

### Phase 2 — The `app` core, worker, and engine wiring

- **Goal.** Trigger → run → complete, and interrupt → checkpoint → resume, headless and deterministic.
- **Build.**
  - `app.New(Config) (*App, error)`, `Config` (Store, Provider, Agents, Skills, Listen, drain timeout),
    `Register`, and `Serve(ctx)`; default any nil port to its local implementation.
  - The queue worker: claim a run → build its `engine.Registry` (Phase 4 supplies the real one; use the fake
    provider now) → `engine.Compile` with the checkpoint store → drive `Invoke`/`Stream`, translating engine
    callbacks into both `EventSink` events (`run.*`, `step.*`, `agent.token`) and `Tracer` spans (run → step →
    agent, with GenAI attributes) from the one callback pass → persist result and terminal status.
  - Run lifecycle transitions (§6.1), including a human gate suspending to `awaiting_review`, and a submitted
    decision returning the run to `queued` and resuming it from the checkpoint via the engine's resume path.
  - On-boot resume: on `Serve`, scan the store for `running`/`parked` runs and re-enqueue the resumable ones.
- **Done when.** Tests with the fake provider pass for: a linear workflow reaching `succeeded` with the right
  result; a workflow with a `Human` gate reaching `awaiting_review`, then a submitted decision resuming it to
  `succeeded`; recreating the `App` against the same store resuming an interrupted run from its checkpoint
  (proving durability across a process boundary in-test).
- **Verify.** `just test ./app/...`.

### Phase 3 — The JSON HTTP + SSE API and the CLI client

- **Goal.** The full `/api` surface, exercised end to end in memory, plus a scriptable CLI client over it.
- **Build.**
  - `app/server`: HTTP handlers for every `/api` endpoint in §6.3; JSON request/response types.
  - SSE endpoints `/api/runs/{id}/events` and `/api/events` that replay the stored event history for the run(s)
    and then stream live, using the per-run sequence number to avoid gaps or duplicates.
  - Wire the HTTP server's start and graceful stop into `Serve(ctx)`.
  - The **CLI command tree** (§11): `app.CLI(a) *cobra.Command` with `serve`, `workflows list`, and the `runs`
    subcommands (`trigger`/`list`/`get`/`watch`/`approve`/`cancel`; `migrate` is wired in Phase 7). The `runs`
    and `workflows` commands are pure `/api` clients taking `--endpoint`; each read command supports `--json`.
    Mount the tree on the reference `cmd/flowd` binary.
- **Done when.** `httptest` covers the full path: trigger a run → it appears in `list`/`get` → the SSE stream
  yields ordered events including `agent.token` deltas → a posted decision resumes the run → it reaches a
  terminal state; a late SSE subscriber replays history then follows live; `cancel` stops a run at the next
  boundary. The CLI drives the same path against a test daemon: `runs trigger` then `runs watch` streams the
  run to completion, and `--json` output parses.
- **Verify.** `just test ./app/server/...`; `flowd runs list --json` against `just dev`.

### Phase 4 — The agent/skill registry, the real provider, and the OpenTelemetry tracer

- **Goal.** Markdown personas/skills resolved into an `engine.Registry`, the real model gateway, and the
  external-observability integration.
- **Build.**
  - `app/agent`: a loader for configured user/project agent and skill roots (persona frontmatter
    `name`/`profile`/`roles`/`tools`/`skills` + system instruction), merged config files, profile model ladders,
    deny-by-default guarded tool grants, and a watcher that reports dirty state for explicit reload.
  - `app/provider`: the OpenAI-compatible gateway provider building an `engine.ChatModel` bound to
    `FLOW_GATEWAY_URL` and the key from the environment, with the persona's tools attached; structured-output
    decoding with retry-on-invalid.
  - `app/observe`: the OpenTelemetry `Tracer` implementation (§6.5) — a run/step/agent span tree with
    GenAI/OpenInference attributes and an OTLP exporter configured from the environment. Verify against a local
    **Arize Phoenix** and confirm **Langfuse** works by pointing the same exporter at its OTLP endpoint.
  - Compose `AgentRegistry` + `Provider` into the `engine.Registry` the worker passes to `engine.Compile`,
    resolving each persona's tool names to executable tools (a small app tool registry) and setting them on the
    `engine.Persona` so tool-bearing agents run their native ReAct loop; ship a small set of default
    persona/skill files.
- **Done when.** The loader parses sample personas/skills; editing a watched file reports dirty state and an
  explicit reload changes the next invocation; the gateway provider builds a working model in an
  integration test that is **skipped when `FLOW_GATEWAY_URL` is unset**; malformed structured output triggers a
  retry; with an OTLP endpoint set, a run produces a run→step→agent span tree in the collector (and the no-op
  default emits nothing).
- **Verify.** `just test ./app/agent/...`; with a gateway running, `just gateway` then a manual smoke run; a
  run against a local Phoenix shows the trace.

### Phase 5 — The terminal UI

- **Goal.** A Bubble Tea client of `/api`, verifiable by GIF.
- **Build.**
  - `app/tui`: the run list (status-grouped, filterable), the pipeline built from `step.*` events, the
    transcript streamed from `agent.token` events, and inline gate approval that `POST`s a decision; an
    `--endpoint` flag; `(*App).TUI(ctx, endpoint)` and a `tui` subcommand on the reference binary.
  - At least two `vhs` tapes under `tui/tapes/` (a pipeline streaming; a gate approval).
- **Done when.** Against a `--demo` daemon, the rendered GIFs show a run progressing, tokens streaming into the
  transcript, and a gate being approved; the layout is correct at the tape's dimensions.
- **Verify.** `just dev` in one shell, `just tui` in another; `just tape pipeline` and `just tape approve`, then
  review the GIFs.

### Phase 6 — The web app (PWA, htmx)

- **Goal.** An installable, mobile-friendly web client of the same core (§10).
- **Build.**
  - `app/web`: `html/template` partials; vendored `htmx.min.js` + SSE extension embedded via `embed.FS`; the
    app shell at `/` and assets at `/static`.
  - `/ui` routes: `GET /ui/runs`, `GET /ui/runs/{id}`, `POST /ui/runs`, `POST /ui/runs/{id}/decision`, and the
    `GET /ui/events` SSE endpoint emitting **named HTML-fragment events** the page swaps via
    `hx-ext="sse"`/`sse-swap`.
  - `manifest.webmanifest` + a service worker caching the shell and assets; mobile-first embedded CSS.
- **Done when.** In a browser the run list, pipeline, and transcript update live via SSE; triggering a run and
  approving a gate via `hx-post` work; the app is installable (manifest + service worker present, passes a PWA
  install check) and its shell loads offline.
- **Verify.** `just dev`, then `just web`; confirm live updates, the install prompt, and offline shell load
  (an agent may screenshot `/` via browser automation).

### Phase 7 — Safe redeployment and versioning

- **Goal.** Drain-to-boundary shutdown and the fingerprint tiers of §12, with no silent clobbering.
- **Build.**
  - A **structural fingerprint** computed from a workflow definition (hash of node kinds, edges, and types via
    the DSL's `Walk`), stored on each `Run` and on each registered workflow.
  - Graceful drain in `Serve`'s shutdown path: stop claiming, let in-flight steps finish and checkpoint, mark
    runs `parked`, exit within `Config.DrainTimeout`.
  - On-boot compatibility check: resume a parked/running run only if its stored fingerprint matches the
    registered workflow's; otherwise mark it `needs_migration`.
  - Previous-binary **drain-only mode** (a flag that claims only pinned, fingerprint-matching runs and takes no
    new work) and a `just deploy` supervisor recipe that orchestrates SIGTERM → drain → swap → resume.
  - `POST /api/runs/{id}/migration` resolving a `needs_migration` run (`restart` | `abandon` |
    `finish_on_previous`), surfaced in the TUI and web app and available as the CLI's `runs migrate`.
  - A run-id-keyed worktree/session manager with idempotent creation and completion-tied cleanup.
- **Done when.** Tests pass for: parking a run mid-flight then resuming it losslessly from a fresh `App` (Tier
  1); a workflow-shape change driving an in-flight run to `needs_migration` rather than a silent resume (Tier
  2); a drain completing within the timeout without discarding an in-flight step; idempotent worktree re-entry.
- **Verify.** `just test ./app/...` (redeployment suite); a manual `just deploy` rehearsal.

### Phase 8 — Packaging, scaffolder, and documentation

- **Goal.** A shippable, documented product.
- **Build.**
  - `app/cmd/flowd`: the reference daemon `main` (with `--demo`) and the `tui` subcommand.
  - The standalone `cmd/flow` tool: `flow init` writes a starter `main.go`, `.flow/config.yml`, project agents
    and skills, and a `justfile` into the current directory (no hosted repo), runnable via `go run …/cmd/flow@latest init`; it
    also mounts `app.ClientCLI()` (§11) so `flow runs …`/`flow workflows …`/`flow tui` work against any daemon
    without a build.
  - README quickstart (the ~10-line `main`, `go get`, `go build`), a populated `examples/`, and the
    world-class `flow`-package documentation of §3 (`doc.go`, `Example` functions, `AGENTS.md`).
- **Done when.** A fresh `go mod init` project following the README builds and runs against `--demo`; `flow
  init` scaffolds a project that builds; `go test` executes the `Example` functions; generated docs render
  cleanly.
- **Verify.** `just check`; a scaffold-then-build-then-run smoke test in a temp directory.

### Phase 9 — Later (beyond v1)

Out of scope for the first complete product, recorded so the design leaves room: additional triggers (webhook,
event — cron is implemented, §6.6); `Config.Workers > 1` for parallel independent runs; and the cloud phase (authentication, a
remote daemon) that the split `/api`–`/ui` routes and the installable web app already anticipate.
