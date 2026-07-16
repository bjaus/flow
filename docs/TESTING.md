# Testing and operating workflows

Flow is designed so normal verification is deterministic, local, and zero-token. Test topology and policy
before testing a model gateway.

## Test layers

1. **Pure functions:** selectors, gates, reducers, lenses, validators, and port adapters with no model.
2. **Workflow + fake registry/provider:** assert which agents ran and the final typed value.
3. **Durability:** interrupt at `Human`, persist a checkpoint, resume with a scripted decision.
4. **Runtime/API:** trigger, watch SSE, decide, and observe the terminal status with local stores.
5. **Optional smoke:** one real gateway call, skipped unless its environment is configured.

Do not put paid, flaky, or credentialed model calls in the merge gate.

## Fake model responses

`app.FakeProvider` scripts structured response strings by persona and prompt key and records calls:

```go
fake := app.FakeProvider(app.FakeScript{
    "planner": {
        "*": {`{"steps":["inspect","change","test"],"approved":false}`},
    },
})

a, err := app.New(app.Config{Provider: fake, AgentRegistry: registry, Store: stores})
// register, trigger, and wait for the expected status
calls := fake.Calls()
```

Responses must match the agent output's JSON shape. Assert agent names/order where topology is sequential; for
concurrent fan-out assert membership rather than scheduling order. Also assert calls made to injected tracker,
filesystem, git, or notification fakes.

## Test every control path

- every `Route` case and default;
- loop success and budget exhaustion;
- router/network unknown actor, clean completion, and `Max` behavior;
- fan-out empty/single/many inputs and partial failures;
- gate approval, rejection, and feedback;
- retries and fallback models;
- cancellation and idempotent re-entry;
- trigger skip behavior;
- parent/child success, failure, cancellation, and depth cap.

A paradigm test should prove **enacted topology plus final state**, not merely that construction succeeded.

## Human checkpoint tests

Run until `awaiting_review`, capture the pending run, submit `Decision`, then assert terminal state and result.
Cover gates nested in `Parallel`, `Map`, `Bind`, routes, and dispatch participants when your workflow uses those
shapes. Recreate the app against the same SQLite store to prove process-boundary resume.

## Project check

Give your workflow project one repeatable merge gate that includes module hygiene and vulnerability analysis:

```sh
go test ./...
go test -race ./...
golangci-lint run
go mod tidy -diff
go mod verify
govulncheck ./...
```

Run race tests in CI even if they are too slow for every edit. Pin CI tool versions and use the same `just`
recipe locally. Check generated docs with `go doc` and verify every `Example...` function through `go test`.

## Real gateway smoke tests

Guard with an environment check:

```go
if os.Getenv("FLOW_GATEWAY_URL") == "" {
    t.Skip("FLOW_GATEWAY_URL not configured")
}
```

Keep the smoke narrow: one persona, one small typed response, no destructive tools. Its purpose is provider
compatibility, not workflow correctness.

## TUI and web verification

A coding agent cannot reliably inspect an interactive terminal. Render committed scenarios with `vhs`:

```sh
just tape pipeline
just tape approve
```

Inspect the GIFs after UI changes. Test the web handler with `httptest`; use browser screenshots for layout and
verify the manifest, service worker, and offline shell separately.

## Operational checklist

- Keep `flow.db` on durable storage and back it up consistently.
- Send SIGTERM and allow the configured drain timeout; do not routinely hard-kill.
- Treat `needs_migration` as an operator decision, never an automatic restart.
- Call the OTLP tracer shutdown function before process exit.
- Reload persona/config snapshots explicitly and inspect reload errors.
- Monitor failed runs, loop/turn exhaustion, human rejection rate, token use, latency, and trigger skips.
- Keep external side effects idempotent and keyed by run ID where practical.
