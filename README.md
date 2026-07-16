# flow

**Type-safe Go workflows, durable local execution, and human steering in one binary.**

Flow lets you describe agentic systems as ordinary, generic Go. Typed steps compose into pipelines, panels,
revision loops, routers, swarms, human approvals, and larger workflows. The local runtime adds SQLite
checkpoints, a queue worker, model/tool execution, HTTP/SSE, CLI and terminal clients, and an embedded web app.

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/bjaus/flow"
    "github.com/bjaus/flow/app"
)

func main() {
    wf := flow.Define("hello", "Greet someone",
        flow.Do("greet", func(_ context.Context, name string) (string, error) {
            return "Hello, " + name, nil
        }))

    a, err := app.New(app.Config{})
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if err := a.Register(wf); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if err := a.CLI().Execute(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

## Quickstart

Requires Go 1.26.5 or newer.

```sh
mkdir my-workflows && cd my-workflows
go run github.com/bjaus/flow/app/cmd/flow@latest init
go mod tidy
go run . serve
```

Or initialize manually with `go mod init` and `go get github.com/bjaus/flow/app` before writing the `main`
above.

The daemon defaults to `http://localhost:7788` and `./flow.db`. In another terminal:

```sh
go run . workflows list
go run . runs trigger hello --input '"Ada"'
go run . runs watch RUN_ID
go run . tui
```

The web app is served at `http://localhost:7788`. For agent workflows, configure an OpenAI-compatible endpoint
through `FLOW_GATEWAY_URL`; deterministic `Do` workflows and fake-provider tests need no model.

## Why flow

- **Type-safe:** `Step[In, Out]` makes invalid hand-offs compile errors.
- **Composable:** every combinator returns a step, so components and patterns nest without a depth limit.
- **Durable:** SQLite checkpoints survive restarts; human gates can wait for days and resume in place.
- **Local-first:** one compiled binary contains the daemon, CLI, TUI, and installable web UI.
- **Model-independent:** personas select abstract model profiles and ordered fallbacks outside workflow code.
- **Least privilege:** agent tools are deny-by-default and guarded by role/persona grants.
- **Testable without tokens:** fake models and port interfaces make complete workflows deterministic.
- **Observable:** replayable run events are built in; OpenTelemetry export is optional.

## Mental model

There is one authoring type: `flow.Step[In, Out]`.

**Leaves do work:**

- `Do` — deterministic Go and calls through your own ports.
- `Agent` — a typed model task resolved to a markdown persona.
- `StateDo` — graph-local ambient state for coordination edges cannot express cleanly.
- `Human` — durable operator approval or feedback.

**Combinators arrange work:**

- `Then`, `Seq` — typed sequencing.
- `Parallel`, `Map`, `Reduce` — fixed or runtime fan-out and fan-in.
- `Route` — run one static branch.
- `Loop`, `StateGate`, `Guard` — bounded iteration and policy tripwires.
- `Bind` — lift a narrow child step onto a larger typed state.
- `Router` — data-dependent turns over a fixed participant set.
- `Network` — turn-scheduled actors whose membership lives in runtime state.

```go
research := flow.Parallel(priorArt, security, operations)
summarize := flow.Agent[[]Finding, Brief]("synthesizer", renderFindings)
review := flow.Human("approve", applyDecision, renderBrief)

root := flow.Then(flow.Then(research, summarize), review)
wf := flow.Define("research-review", "Research with independent experts and approve the brief", root)
```

Use the least dynamic shape that fits. Static typed dataflow is easier to reason about than a router; a fixed
router is easier than dynamic membership.

## The 34 multi-agent paradigms

Flow expresses the full pattern catalog by composition rather than 34 unrelated modes:

- collaboration: orchestrate, refine, review, committee, debate, vote, tournament, roundtable, mesh, pair,
  relay, RACI, retro, and human;
- common recipes and aliases: handoff, swarm, best-of-N, mixture of experts, ensemble, mixture of agents,
  plan–execute, cascade, escalation ladder, reflexion, scatter–gather, MapReduce, supervisor, ReAct, and tool
  loop;
- state-coordinated: blackboard, AutoLoop, BabyAGI, tree search, and Tree of Thoughts.

Each pattern's use cases, tradeoffs, exact flow shape, durability behavior, and production guardrails are in
[The 34 multi-agent paradigms](docs/PARADIGMS.md).

## Compose small workflows into bigger ones

Package reusable fragments as Go functions returning `Step`. Connect same-edge types with `Then`; lift a
narrow `Step[In, Out]` into a project-wide state with `Bind`:

```go
type Job struct {
    Question Question
    Brief    Brief
    Draft    Draft
}

root := flow.Seq(
    flow.Bind(Research(),
        func(j Job) Question { return j.Question },
        func(j Job, b Brief) Job { j.Brief = b; return j }),
    flow.Bind(DraftAndReview(),
        func(j Job) Brief { return j.Brief },
        func(j Job, d Draft) Job { j.Draft = d; return j }),
)
```

For independently registered workflows, call `app.SpawnAwait` from a `Do` step. Child runs retain a parent ID,
appear in API/CLI filters, and are canceled with their parent. See [Composition](docs/COMPOSITION.md) for when
to use in-run composition versus child runs.

## Personas, skills, models, and tools

An `Agent` name identifies a reusable persona; its prompt function is the task for this invocation. Flow merges
user and project configuration (`~/.flow/config.yml`, then `.flow/config.yml`):

```yaml
profiles:
  coding:
    model: primary-model
    fallbackModels: [fallback-model]
roles:
  reader:
    tools: ["read(**)", "grep(**)", "find(**)"]
    skills: [repository-reading]
```

```markdown
---
name: planner
profile: coding
roles: [reader]
tools: []
skills: [implementation-planning]
---
You produce concrete implementation plans with explicit verification.
```

Personas with no grants have no tools. Configuration changes become active only after explicit reload, so an
invalid edit does not silently alter active runs. See [Agents, skills, models, and tools](docs/AGENTS-AND-SKILLS.md).

## Runtime at a glance

`app.New` accepts independently replaceable checkpoint, run, event, provider, persona-registry, tracer, and
tool ports. Omitted persistence uses pure-Go SQLite. `app.FakeProvider` scripts model output for zero-token
tests; `app.Gateway` targets an OpenAI-compatible endpoint.

The CLI includes `serve`, workflow discovery, run trigger/list/get/watch, approval/return, cancellation,
migration, config reload, and TUI commands. The JSON/SSE API exposes the same operations for custom clients.
Cron triggers and parent/child workflow runs are built in. See [Runtime and client reference](docs/RUNTIME.md).

## Documentation

- [Getting started](docs/GETTING-STARTED.md) — structure a complete workflow project
- [Authoring reference](docs/AUTHORING.md) — leaves, combinators, state, gates, and analysis
- [The 34 multi-agent paradigms](docs/PARADIGMS.md) — patterns, aliases, and tradeoffs
- [Composition](docs/COMPOSITION.md) — reusable sub-workflows and child runs
- [Agents, skills, models, and tools](docs/AGENTS-AND-SKILLS.md)
- [Runtime and clients](docs/RUNTIME.md) — durability, config, CLI, HTTP/SSE, schedules, tracing
- [In-process engine](docs/ENGINE.md) — compile, stream, checkpoint, and resume without the daemon
- [Testing and operations](docs/TESTING.md) — fake models, checkpoint tests, and production practices
- [Documentation index](docs/README.md)

Generated API documentation is available on
[pkg.go.dev/flow](https://pkg.go.dev/github.com/bjaus/flow) and
[pkg.go.dev/flow/app](https://pkg.go.dev/github.com/bjaus/flow/app).
