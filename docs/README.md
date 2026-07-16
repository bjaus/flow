# flow documentation

Start with [Getting started](GETTING-STARTED.md), then use this map by task.

## Author workflows

- [Authoring reference](AUTHORING.md) — every leaf, combinator, decorator, and state rule
- [The 34 multi-agent paradigms](PARADIGMS.md) — selection guidance and flow recipes
- [Composition](COMPOSITION.md) — reusable steps, `Bind`, and parent/child runs
- [Agents, skills, models, and tools](AGENTS-AND-SKILLS.md) — personas, config, fallback models, permissions

## Run and verify

- [Runtime and clients](RUNTIME.md) — `app.Config`, durability, CLI, API/SSE, scheduling, tracing
- [In-process engine](ENGINE.md) — compile, invoke, stream, checkpoint, and resume without the daemon
- [Testing and operations](TESTING.md) — fake models, checkpoint tests, checks, deployment practices

## Reference

- [Package flow on pkg.go.dev](https://pkg.go.dev/github.com/bjaus/flow)
- [Package app on pkg.go.dev](https://pkg.go.dev/github.com/bjaus/flow/app)

API details ultimately come from exported Go declarations and their doc comments. These guides explain how the
parts fit together for application authors and operators.
