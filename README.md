# flow

A local-first, single-binary Go platform for authoring and running durable, long-lived agentic workflows.

- **Author** workflows in type-safe Go with the `flow` package — one core `Step[In, Out]` type, composed
  without limit.
- **Run, observe, and steer** them with the `flow/app` runtime: a durable daemon that serves a terminal UI, a
  command-line client, and an installable web app over one HTTP + SSE API. Backends (checkpoint/run store,
  model provider) are pluggable interfaces with zero-config local defaults.

## Status

The `flow` DSL and its `engine` are built and tested. The `app/` runtime is under construction per
**[SPEC.md](./SPEC.md)**. Contributors and coding agents: start with **[AGENTS.md](./AGENTS.md)**.

## Modules

- `github.com/bjaus/flow` — the DSL (`package flow`) + `engine`. Import this to author workflows.
- `github.com/bjaus/flow/app` — the runtime. Import this to build a daemon that runs and serves them.

## Repository map

```
.                     the flow DSL (package flow)
engine/               compiles a workflow to a runnable graph (streaming, durable HITL)
internal/ir/          the erased definition tree the DSL and engine share
app/                  the runtime (to be built — SPEC.md §16)
SPEC.md               the build specification (source of truth)
AGENTS.md             how to work in this repo  (CLAUDE.md symlinks here)
.agents/skills/       task-specific guides for coding agents
```
