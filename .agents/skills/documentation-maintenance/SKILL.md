---
name: documentation-maintenance
description: Keep flow's public user documentation synchronized with feature work. Load whenever changing public API, workflow semantics, runtime configuration, personas/skills/tools, CLI/API behavior, events, durability, examples, or onboarding.
---

# Maintain flow's public documentation

The goal is that an application author or operator can use a released feature without reading implementation
code. Documentation is part of the feature, not follow-up work.

## Audience and scope

Write for people **using flow in their own repositories**. Do not put contributor workflow, internal build
history, private consumer names, private paths, backlog notes, or implementation-task narration in public
usage guides. Derive general practices from real consumers, but present them as standalone public guidance.

Public surfaces:

- `README.md` — quickstart, value, mental model, and links; keep it skimmable.
- `docs/GETTING-STARTED.md` — complete first consumer project.
- `docs/AUTHORING.md` — DSL behavior and selection guidance.
- `docs/PARADIGMS.md` — all 34 named patterns and honest semantic limitations.
- `docs/COMPOSITION.md` — reusable steps, `Bind`, and parent/child runs.
- `docs/AGENTS-AND-SKILLS.md` — config, personas, skills, models, and permissions.
- `docs/RUNTIME.md` — app config, lifecycle, clients, routes, events, schedules, tracing.
- `docs/ENGINE.md` — direct compile/run/stream/checkpoint behavior.
- `docs/TESTING.md` — consumer testing and operating practices.
- `doc.go`, `app/doc.go`, exported comments, and `Example...` tests — pkg.go.dev reference.

`SPEC.md` and `AGENTS.md` are sources for behavior/constraints, not onboarding destinations.

## Source-to-document map

| Change | Required review |
|---|---|
| leaf, combinator, decorator, state semantics | package comments, examples, `docs/AUTHORING.md`, paradigm recipes |
| named multi-agent pattern | `docs/PARADIGMS.md` catalog/index/count and composition examples |
| workflow nesting or spawning | README, `docs/COMPOSITION.md`, runtime lifecycle caveats |
| `app.Config` field or port | exported comment and `docs/RUNTIME.md` config table |
| status, event, HTTP route, CLI command/flag | `docs/RUNTIME.md`; README only if first-use flow changes |
| persona frontmatter, profile, role, skill, tool permission | `docs/AGENTS-AND-SKILLS.md` and starter snippets |
| engine compile/run/stream/checkpoint behavior | `docs/ENGINE.md`, package comments, runnable examples |
| durability, retry, fallback, drain, migration | authoring/runtime/testing caveats and examples |
| fake/test helper | `docs/TESTING.md` and a runnable example where useful |
| scaffolder/default | README quickstart and `docs/GETTING-STARTED.md` exact generated behavior |
| supported Go/module requirement | README prerequisite and all scaffold snippets |

## Workflow

1. Identify externally observable changes from declarations, tests, and behavior.
2. Search every public name and old behavior across Markdown, doc comments, examples, and scaffold templates.
3. Update the narrow reference page first, then README summaries/links if the first-use story changed.
4. Add or update an executable `Example...` when an exported API gains a common usage path.
5. State semantics precisely: input/output types, defaults, errors, ordering, concurrency, checkpoint behavior,
   bounds, side effects, security, and limitations.
6. Keep code snippets compilable against the current API. Prefer complete small snippets over pseudocode; if a
   fragment intentionally omits declarations, say so through context rather than implying it is standalone.
7. Check navigation and terminology, then run verification.

## Documentation standards

- Follow Go doc conventions: package comment starts `Package x`; exported comments start with the symbol name.
- Explain when to use an API versus its nearest alternative.
- Never claim stronger semantics than implemented (for example, distinguish turn-scheduled `Network` from a
  concurrent mailbox mesh).
- Distinguish persona identity from invocation task, typed edges from ambient state, and in-run composition
  from child-run lifecycle.
- Document every loop/dispatch budget, fallback, partial-failure, and human-approval boundary.
- Use relative links inside the repository and stable official links externally.
- Do not duplicate long reference text in README; summarize and link.
- Do not expose secrets or use real credentials in examples.
- Keep aliases discoverable while pointing to one canonical implementation recipe.

## Verification

Run the project's normal check plus documentation-specific checks:

```sh
just check
go test ./...                 # executes root package examples
(cd app && go test ./...)
go doc github.com/bjaus/flow
go doc github.com/bjaus/flow/app
rg -n 'TODO|TBD|private consumer name' README.md docs doc.go app/doc.go
```

Also manually verify:

- every README/docs relative link resolves;
- the quickstart's commands and minimum Go version agree with `go.mod` and the scaffolder;
- CLI commands/flags agree with Cobra definitions;
- API paths/events/config defaults agree with declarations and tests;
- all 34 paradigm headings remain present when pattern documentation changes;
- no public guide tells users how to contribute to or develop flow itself.

If behavior is uncertain, test it or describe the limitation. Never fill a documentation gap by guessing.
