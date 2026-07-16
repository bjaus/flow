# In-process engine reference

Import `github.com/bjaus/flow/engine` when you want to compile and execute a typed workflow in process without
the durable daemon, queue, or clients. Most applications that need long-lived runs and human operations should
use `github.com/bjaus/flow/app` instead.

## Resolve personas

An agent workflow needs an `engine.Registry`:

```go
registry := engine.RegistryFunc(func(name string) (engine.Persona, error) {
    switch name {
    case "planner":
        return engine.Persona{
            Model:  model,
            System: "You create concise plans.",
            Tools:  toolSet,
        }, nil
    default:
        return engine.Persona{}, fmt.Errorf("unknown persona %q", name)
    }
})
```

`Persona` binds a chat model, system instruction, and optional tools. With tools, the model must support tool
calling and the `Agent` lowers to a native ReAct loop. The app module's markdown registry/provider assembly is
the easier production path.

## Compile and invoke

```go
runnable, err := engine.Compile(ctx, workflow, registry, checkpointStore)
if err != nil {
    return err
}
out, err := runnable.Invoke(ctx, input)
```

Pass `nil` for the checkpoint store when no `Human` step or durable engine interrupt is needed. Additional
`compose.GraphCompileOption` values are passed through after flow defaults, preserving underlying eino
configuration seams.

`Runnable[In, Out]` supports all four native data modes:

```go
out, err := runnable.Invoke(ctx, in)
stream, err := runnable.Stream(ctx, in, options...)
out, err := runnable.Collect(ctx, inputStream)
outStream, err := runnable.Transform(ctx, inputStream)
raw := runnable.Underlying()
```

Use `Underlying` only when embedding the compiled workflow into a hand-written eino graph. Keep normal flow
composition at the `Step` level.

## Streaming tokens and callbacks

Supply eino callback options to `Stream`. Agent model token streams are visible through ChatModel callback
components, including agents inside concurrent fan-outs. Tag UI events by node/component metadata rather than
assuming one active agent. Consumers must close stream readers according to eino's ownership rules.

The app runtime already translates callbacks into replayable `agent.token` and step events; do not duplicate
that integration when using `app`.

## Durable human interruption

Durability requires a `compose.CheckPointStore` and one stable checkpoint ID:

1. invoke with `compose.WithCheckPointID(id)`;
2. detect a pause with `compose.ExtractInterruptInfo(err)`;
3. retain the interrupt context ID presented for the human gate;
4. invoke again with the same checkpoint ID and `compose.ResumeWithData(ctx, interruptID, flow.Decision{...})`.

`Human` registers its carried type automatically. Workflow input/output types are also registered. If a custom
ambient `StateDo` value crosses a checkpoint, call `engine.Register[StateType]()` once before execution.

Durability composes through a human nested in `Parallel`, `Map`, `Bind`, `Route`, `Router`, or `Network`.
Completed sibling results survive the interruption. Keep checkpoint IDs isolated per run; use
`engine.WithCheckpointScope(ctx, scope)` when multiple durable sub-runs share a store.

## Type-erased runtime integration

`CompileDefinition` returns `DynamicRunnable` from an erased workflow definition. It is intended for runtimes
that decode types dynamically; ordinary callers should prefer generic `Compile`, which preserves `In` and
`Out` at compile time.

## Errors and validation

Call `workflow.Validate()` before compile and preflight `workflow.AgentNames()` against the registry. Compile
errors cover invalid topology and lowering failures; invocation errors cover step/model/tool failures and
interrupts. Distinguish a deliberate human interrupt from failure before marking a run failed.

For complete queueing, migration, API, and operator behavior, use the [app runtime](RUNTIME.md).
