---
name: engine-api
description: How the runtime compiles and runs a workflow via the engine package, including token streaming and durable interrupt/checkpoint/resume. Load when building the worker, the checkpoint store, or human-gate handling.
---

# Running workflows with the `engine` package

The engine lives in package `engine` (module path `github.com/bjaus/flow/engine`). It lowers a `flow.Workflow`
to a native runnable graph (built on cloudwego/eino) and gives you real concurrency, per-node tracing, token
streaming, and durable interrupt/checkpoint/resume for free. The runtime never re-implements these — it drives
the engine.

## Compile and run

```go
r, err := engine.Compile(ctx, wf, registry, checkpointStore) // returns engine.Runnable[In, Out]
out, err := r.Invoke(ctx, in)                                // synchronous → typed Out
sr,  err := r.Stream(ctx, in, compose.WithCallbacks(sink))   // → *schema.StreamReader[any]
```

- `registry` implements `engine.Registry`:
  `Persona(name string) (model.BaseChatModel, systemPrompt string, err error)` — resolve a persona name to a
  chat model and its system instruction. `engine.RegistryFunc` adapts a plain func (used in tests with a fake
  model). In the runtime, build the registry from a markdown persona (the `agent` package) + the model
  provider.
- `checkpointStore` is an eino `compose.CheckPointStore` (pass `nil` to disable durability). The app's
  `CheckpointStore` port adapts to this interface.

## Streaming tokens to the UI

Run with `Stream` and a `compose.Callbacks` handler. Forward each ChatModel's token stream to the EventSink as
`agent.token` events, tagged by node name: in the callback, filter `RunInfo.Component == "ChatModel"` and read
the stream in `OnEndWithStreamOutput`. This works for sequential agents and inside concurrent fan-outs. The
engine's `tokenSink` in `engine/native_test.go` is a complete working example.

## Durable human-in-the-loop

On resume, eino restores checkpointed **state**, not values in flight on an edge — so an interrupting step must
checkpoint its own input, which the DSL's `Human` already does. The runtime path:

1. Compile with a checkpoint store; run with `compose.WithCheckPointID(id)`.
2. A human gate pauses via `compose.StatefulInterrupt(ctx, payload, input)`. Detect the pause with
   `compose.ExtractInterruptInfo(err)`; the interrupt id is at `.InterruptContexts[0].ID`.
3. Resume by re-invoking with the same checkpoint id and
   `compose.ResumeWithData(ctx, interruptID, flow.Decision{Approved: true})`. Inside the node the value is
   recovered with `compose.GetInterruptState[T]` and the decision with `compose.GetResumeContext[flow.Decision]`.

Custom types that ride a checkpoint are auto-registered by the DSL (each workflow's In/Out and every `Human`'s
carried type). Gotcha: two distinct Go types sharing a package-qualified name collide in the serializer — a
non-issue for real code, but avoid duplicate type names across packages in tests.

The tests `engine/native_test.go`, `engine/streaming_test.go`, and `engine/combinators_test.go` are working,
runnable references for all of the above. Full durability + safe-redeployment design: **SPEC.md §12**.
