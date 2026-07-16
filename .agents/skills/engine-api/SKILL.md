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
r, err := engine.Compile(ctx, wf, registry, checkpointStore, opts...) // engine.Runnable[In, Out]; opts are eino GraphCompileOptions
out, err := r.Invoke(ctx, in)                                         // synchronous → typed Out
sr,  err := r.Stream(ctx, in, compose.WithCallbacks(sink))            // → *schema.StreamReader[any]
val, err := r.Collect(ctx, streamIn)                                  // streaming input → value
str, err := r.Transform(ctx, streamIn)                                // streaming input → streaming output
raw := r.Underlying()                                                 // the eino runnable, to nest a flow workflow back into an eino graph
```

- `registry` implements `engine.Registry`: `Persona(name string) (engine.Persona, error)`, where
  `engine.Persona{ Model model.BaseChatModel; System string; Tools []tool.BaseTool }`. Resolve a persona name
  to its model, system instruction, and any tools. When `Tools` is non-empty the `Model` must implement
  `model.ToolCallingChatModel`, and the Agent lowers to a native **ReAct loop** (ChatModel ⇄ ToolsNode) so it
  actually calls tools; otherwise it is one completion. `engine.RegistryFunc` adapts a plain func (tests use a
  fake model). In the runtime, build the registry from a markdown persona (the `agent` package) + the model
  provider, resolving tool names to executable tools.
- `checkpointStore` is an eino `compose.CheckPointStore` (pass `nil` to disable durability). The app's
  `CheckpointStore` port adapts to this interface. Durability composes through nesting: a `Human` inside a
  `Parallel`/`Map` branch, a `Bind`, or a dispatch participant checkpoints and resumes at the exact point.
- `opts` are passed straight through to eino's `Graph.Compile` (after flow's defaults, so they override them) —
  use them for anything flow does not set: `compose.WithSerializer`, `compose.WithInterruptBeforeNodes`, a DAG
  trigger mode, etc. Per-node run options target an Agent/Do node by its `Step.ID` via `DesignateNode`.

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
