# flow

**Type-safe Go workflows, durable local execution, and human steering in one binary.**

```go
package main

import (
    "context"
    "log"
    "github.com/bjaus/flow"
    "github.com/bjaus/flow/app"
)

func main() {
    wf := flow.Define("hello", "Greet someone",
        flow.Do("greet", func(_ context.Context, name string) (string, error) {
            return "Hello, " + name, nil
        }))
    a, err := app.New(app.Config{})
    if err != nil { log.Fatal(err) }
    if err = a.Register(wf); err != nil { log.Fatal(err) }
    if err = a.CLI().Execute(); err != nil { log.Fatal(err) }
}
```

```sh
mkdir my-workflows && cd my-workflows
go mod init my-workflows
go get github.com/bjaus/flow/app
go build .
./my-workflows serve
```

Or scaffold the same project with `go run github.com/bjaus/flow/app/cmd/flow@latest init`.

The default daemon listens on `:7788`, stores runs and checkpoints in `./flow.db`, and serves:

- the JSON and SSE API at `/api`;
- the installable, offline-shell web app at `http://localhost:7788`;
- `./my-workflows tui` for the terminal client;
- `workflows` and `runs` commands for scripts and CI.

## Mental model

There is one core authoring type: `flow.Step[In, Out]`. **Leaves do work** and **combinators arrange work**;
every combinator returns another step, so nesting is unlimited and Go checks every edge.

### Leaves

- `Do` — deterministic Go code and the escape hatch for any component.
- `StateDo` — code with graph-local blackboard access; prefer typed edges when possible.
- `Agent` — a typed model task. Its name resolves a markdown persona at runtime.
- `Human` — a durable gate that checkpoints and resumes with an operator decision.

An agent file is a reusable **persona**. The `prompt` passed to `Agent` is the per-invocation **task**. Do not
put a task into the persona or model configuration into workflow code.

### Composition

- `Then` / `Seq` — typed sequencing; use `Seq` for a same-type state spine.
- `Parallel` / `Map` / `Reduce` — fixed or runtime fan-out and fan-in.
- `Route` — one static branch selected by data.
- `Loop` / `Guard` — bounded convergence and tripwires.
- `Bind` — lift a differently typed step into a state spine with read/write lenses.
- `Router` — dynamic turn selection over fixed participants.
- `Network` — runtime membership represented in checkpointed state.

See [`examples/`](./examples), package examples in [`examples_test.go`](./examples_test.go), and the complete
runtime contract in [`SPEC.md`](./SPEC.md).

## Personas and skills

Flow merges `~/.flow/config.yml` and `.flow/config.yml` (project wins); `FLOW_CONFIG` can replace the project
config path. Agent and skill roots are arrays and default to both user and project `.flow` directories:

```yaml
profiles:
  coding: [primary-model, fallback-model]
roles:
  reader: ["read(**)"]
  checks: ["bash(pnpm * check)"]
```

Personas declare abstract profiles and reusable roles. Inline tools add one-off grants; an agent with no grants
has no tools:

```md
---
name: planner
profile: coding
roles: [reader, checks]
skills: [review]
tools: ["search(docs/**)"]
---
You create concise, executable plans.
```

Skills use the portable `SKILL.md` convention. Flow reports watched changes without activating them; use
`flow config reload`, press `c` in the TUI, or use the web/API reload control. Shell wildcard grants cannot
match command chaining or substitution. Gateway credentials come only from the environment; copy
[`.env.example`](./.env.example).

## Runtime configuration

`app.Config` accepts independent checkpoint, run, event, provider, persona-registry, tracer, and tool ports.
Omitted persistence defaults to pure-Go SQLite. `app.FakeProvider` scripts model output deterministically for
zero-token tests. `app.Gateway` targets an OpenAI-compatible endpoint from `FLOW_GATEWAY_URL`.
`Config.Triggers` schedules cron-driven runs: each `app.Trigger` pairs a registered workflow with a standard
5-field cron spec and a canned JSON input; scheduled runs carry the trigger's name, and a firing is skipped
(with a `trigger.skipped` event) while the daemon drains or the previous scheduled run is still active.

## Development

This repository contains two Go modules joined by `go.work`: the lightweight DSL/engine at the root and the
runtime under `app/`. Run the complete merge gate with:

```sh
just check
```

See [`AGENTS.md`](./AGENTS.md) for repository conventions.
