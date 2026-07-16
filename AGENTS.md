# AGENTS.md — working in the `flow` repository

## Objective

Build **flow** — a local-first, single-binary Go platform for authoring and running durable, long-lived
agentic workflows — to the specification in **[SPEC.md](./SPEC.md)**. SPEC.md is the source of truth; this
file is operating conventions. If they ever disagree, SPEC.md wins — but surface the conflict rather than
guessing.

## Current state — read before touching anything

- The **`flow` DSL** (repo root, `package flow`) and the **`engine`** package (`./engine`) are **built,
  tested, and ready**. `go test ./...` is green today. Treat them as a finished dependency.
- **Do not modify the DSL or the engine** except where SPEC.md explicitly calls for it. The spec-mandated
  renames (`eino`→`engine`, the compiled type `App`→`Runnable`, and the module path) are already applied.
- **What remains is the entire `app/` runtime** — the durable server, pluggable stores, model provider,
  markdown agent/skill registry, terminal UI, CLI client, and htmx web app — plus packaging and the
  world-class DSL documentation. That is the job.

## Where to start

Work **SPEC.md §16 "Phased execution plan"** strictly in order, beginning at **Phase 0**. Every phase has a
concrete *Done when* checklist and a *Verify* command. Ship and commit each phase before starting the next.
Phases 0–7 are all verifiable with the fake provider and **no network**, so build the whole runtime with zero
tokens before wiring the real model gateway.

## Module layout (SPEC.md §2)

- `github.com/bjaus/flow` — root module: the DSL (`package flow`) + `engine` (`package engine`). Deps: eino.
- `github.com/bjaus/flow/app` — nested module: the runtime you build. Deps: everything heavy.
- A `go.work` (`use (. ./app)`) ties them for local dev; create it in Phase 0.

## Conventions (non-negotiable)

- **Verify with `just check`** before finishing any phase — it builds both modules, runs `golangci-lint`
  (must be **zero** issues), and runs all tests. Full dev workflow: SPEC.md §15 and the `verify-and-iterate`
  skill.
- **Determinism first.** Test every workflow against the **fake provider** (zero tokens), asserting the
  enacted topology and the final typed state. The real gateway is only for end-to-end smoke checks.
- **Go style.** Use `any`, never `interface{}`. Favor a functional, declarative style over imperative
  boilerplate where it reads more clearly. Name a package for **what it contains**, not what it provides.
- **Secrets.** Read secrets only from the environment; never inline or log one. `.env` is git-ignored — see
  `.env.example`.
- **TUI iteration.** You cannot see the terminal: render the TUI to a GIF with `vhs` (`just tape <name>`) and
  inspect it. Use `watchexec` for live reload — `air`/`wgo` blank out a TUI.
- **Git.** Commit per phase. Commit messages are a **single line, present tense, no attribution**.

## Skills and references

Task-specific guides live in **`.agents/skills/`** (portable `SKILL.md` format — not `.claude/`, `.cursor/`,
etc.). Load the one relevant to your current task:

- **`flow-dsl`** — the DSL public API, for authoring workflows in tests, examples, and the runtime.
- **`engine-api`** — how the runtime compiles and runs a workflow via `engine`, incl. streaming and durable
  interrupt/resume.
- **`verify-and-iterate`** — the `just` recipes, `vhs` tapes, the gateway, and determinism-first testing.
