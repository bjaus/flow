# AGENTS.md — working in the `flow` repository

## Objective

Build **flow** — a local-first, single-binary Go platform for authoring and running durable, long-lived
agentic workflows — to the specification in **[SPEC.md](./SPEC.md)**. SPEC.md is the source of truth; this
file is operating conventions. If they ever disagree, SPEC.md wins — but surface the conflict rather than
guessing.

## Current state — read before touching anything

- The **`flow` DSL**, **`engine`**, and **`app/` runtime** are built and covered by deterministic tests. The
  runtime includes SQLite durability, the queue worker, HTTP/SSE API, CLI, Markdown registry, gateway/fake
  providers, OpenTelemetry, Bubble Tea TUI, embedded htmx PWA, safe-redeployment controls, and scaffolder.
- **Do not modify the DSL or the engine** except where SPEC.md explicitly calls for it. The spec-mandated
  renames (`eino`→`engine`, the compiled type `App`→`Runnable`, and the module path) are already applied.
- Extend or repair the implemented product against `SPEC.md`; preserve its port seams and zero-token test
  coverage. `just check` is the source of truth for repository health.

## Where to start

For unfinished or replacement work, follow **SPEC.md §16 "Phased execution plan"** and its verification
commands. Phases 0–8 have an implementation; changes must keep every earlier phase green. Prefer the fake
provider and deterministic ports—normal verification spends no model tokens.

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
  When choosing *which composition* expresses a multi-agent shape (pipeline, panel, generator–critic,
  supervisor, swarm, …), consult **[docs/PARADIGMS.md](./docs/PARADIGMS.md)** alongside it.
- **`engine-api`** — how the runtime compiles and runs a workflow via `engine`, incl. streaming and durable
  interrupt/resume.
- **`verify-and-iterate`** — the `just` recipes, `vhs` tapes, the gateway, and determinism-first testing.
- **`documentation-maintenance`** — required for changes to public API or behavior; keeps user guides, package
  comments, examples, CLI/API references, and the 34-pattern catalog synchronized.
