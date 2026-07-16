---
name: verify-and-iterate
description: The build/verify/iterate loop — just recipes, rendering the TUI with vhs, the model gateway, and determinism-first testing. Load when running, testing, or refining the runtime.
---

# Verify and iterate

## The merge gate

Run `just check` before finishing any phase. It builds both modules, runs `golangci-lint` (must report
**zero** issues), and runs all tests. Keep it green. Commit per phase with a single-line, present-tense
message and no attribution.

Tooling this assumes (install in Phase 0): `just`, `golangci-lint`, `watchexec`, `vhs`, and a local
OpenAI-compatible model gateway (e.g. `litellm`).

## Determinism first

Every side effect goes through a port, so a fake makes any workflow reproducible. Build and test every
workflow — including the hardest multi-agent ones — against the **fake provider** (zero tokens), asserting the
enacted topology (which steps ran, in what order, with what membership) and the final typed state. Only reach
for the real gateway for end-to-end smoke checks. Phases 0–7 need no network.

## The model gateway

An OpenAI-compatible endpoint on `:4000/v1`. `just gateway` starts it. Workflows read `FLOW_GATEWAY_URL` and
the key from the environment. Never inline or log a key; `.env` is git-ignored (copy `.env.example`).

## Seeing the TUI (you cannot see the terminal)

Render scripted scenarios to GIFs with `vhs` and inspect them:

- Keep `.tape` files under `tui/tapes/`; render with `just tape <name>`.
- Drive the TUI against a `--demo` daemon, which self-seeds a run streaming mid-pipeline and one awaiting
  review, so the GIF shows real content.
- Loop: change the TUI → `just tape <name>` → open the GIF → refine layout/theme → repeat. Commit a couple of
  canonical tapes so rendering is reproducible.

For the web app, run `just dev`, open `/` in a browser, and screenshot it (browser automation) to refine.

## Live reload

Use `watchexec` (`just dev`) for anything that may render a terminal UI — `air`/`wgo` blank the screen. The
daemon alone reloads fine under any watcher.
