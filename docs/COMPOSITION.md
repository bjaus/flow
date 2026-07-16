# Composing workflows

Flow composition operates at two levels: **steps in one run** and **registered workflows as parent/child
runs**. Choose based on lifecycle, not code organization.

## Reusable steps in one run

Ordinary Go functions are the component system:

```go
func Research() flow.Step[Question, Brief] { return /* composed step */ }
func Draft() flow.Step[Brief, Draft]       { return /* composed step */ }

func Editorial() flow.Workflow[Question, Draft] {
    return flow.Define("editorial", "Research and draft an article",
        flow.Then(Research(), Draft()))
}
```

This gives one typed call graph, run record, checkpoint scope, event stream, and final result. A `Human` nested
anywhere in the component resumes at its exact point.

## State-spine composition with `Bind`

Large workflows often need one state while children should retain narrow contracts:

```go
type Job struct {
    Question Question
    Brief    Brief
    Draft    Draft
}

root := flow.Seq(
    flow.Bind(Research(),
        func(j Job) Question { return j.Question },
        func(j Job, b Brief) Job { j.Brief = b; return j }),
    flow.Bind(Draft(),
        func(j Job) Brief { return j.Brief },
        func(j Job, d Draft) Job { j.Draft = d; return j }),
)
```

Treat `read`/`write` as lenses. Keep them pure and test them. A child should not know the parent state type.
This makes components reusable in another workflow and limits accidental coupling.

## Parent/child runs

A `Do` step can obtain the runtime spawner and call a separately registered workflow:

```go
child := flow.Do("run-child", func(ctx context.Context, in ChildInput) (ChildOutput, error) {
    raw, err := app.SpawnAwait(ctx, "child-workflow", in)
    if err != nil {
        return ChildOutput{}, err
    }
    var out ChildOutput
    err = json.Unmarshal(raw, &out)
    return out, err
})
```

For separate operations, use `spawner, ok := app.SpawnerFrom(ctx)`, then `Spawn` and `Await`. Child runs record
`ParentID`, can be listed with `runs list --parent`, and are canceled when their parent is canceled. Spawn depth
is capped by `app.MaxSpawnDepth`.

Current tradeoff: awaiting executes queued children inline to avoid deadlocking the single worker. The parent
stays running, and an inline chain is not independently resumable across restart. Use this feature when
separate run records or registration boundaries justify that lifecycle; prefer in-run step composition for
strongest checkpoint semantics.

## Packaging components

- Put domain types and workflow constructors in a `workflows` package.
- Return `Step` for reusable fragments and `Workflow` only at registration boundaries.
- Inject external capabilities as small interfaces into constructors.
- Keep personas and skills referenced by stable names.
- Avoid package globals carrying mutable run state.
- Do not expose engine/internal definition types as your application's API.

## Combining paradigms

A production workflow is usually a spine of patterns: route by complexity, scatter research, refine a draft,
ask a human, then guard publication. Each pattern remains an ordinary step and is lifted onto the parent with
`Bind`. See [The 34 multi-agent paradigms](PARADIGMS.md) for recipes and selection guidance.
