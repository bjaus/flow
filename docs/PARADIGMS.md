# The 34 multi-agent paradigms

Flow does not hard-code a separate runtime mode for every named agent pattern. A paradigm is a recipe made
from typed `Step[In, Out]` values, and the result is another step. This keeps the vocabulary large while the
execution model stays small, testable, and composable.

This catalog uses the 34 names commonly used by this project. Several are aliases from different communities;
those entries are documented separately so a reader can find the term they know. Aliases deliberately point to
the same flow shape rather than pretending they have different semantics.

## Before choosing a pattern

| If the uncertainty is… | Start with |
|---|---|
| none; stages are known | `Then` or `Seq` |
| which one branch should run | `Route` |
| independent opinions or items | `Parallel` or `Map`, then fan in |
| how many revisions are needed | `Loop` |
| who should act next | `Router` |
| which actors exist at runtime | `Network` |
| whether work may proceed | `Human` and/or `Guard` |

Prefer the least dynamic shape that solves the problem. Static dataflow is easier to understand, cheaper to
run, and easier to test than dynamic dispatch. Every loop and dispatcher needs an explicit bound. Today,
`Router.Max` and `Network.Max` are enforced; a `Loop` should also carry its attempt budget in state and include
it in the exit predicate.

The snippets below use these representative types:

```go
type Work struct {
    Goal, Draft, Feedback string
    Round                 int
    Approved, Done        bool
    Tasks                 []Task
    Results               []Result
    Transcript            []Message
}
```

## Core collaboration patterns

### 1. Orchestrate

**Shape:** one coordinator chooses a specialist each turn over shared typed state.

**Use it when:** the cast is fixed but the order of work depends on intermediate results. Use `Route` instead
when exactly one branch runs once; use `Network` when membership itself changes.

```go
team := flow.Router("delivery", flow.RouterConfig[Work]{
    Participants: map[string]flow.Step[Work, Work]{
        "research": researcher,
        "write":    writer,
        "check":    checker,
    },
    Select: nextSpecialist,
    Done:   func(w Work) bool { return w.Done },
    Max:    12,
})
```

Keep `Select` and `Done` pure. Put the coordinator's model decision into state in a participant when selection
must be agentic; the selector itself remains deterministic and replayable.

### 2. Refine

**Shape:** produce, evaluate, and revise until quality or budget ends the cycle.

**Use it when:** there is a meaningful definition of “good enough.” Do not loop merely to ask the same model
again.

```go
refine := flow.Loop("refine", flow.Seq(writer, critic),
    flow.StateGate(func(w Work) bool { return w.Approved || w.Round >= 3 }), 3)
```

The state should preserve the latest artifact, critique, round count, and terminal reason. A critic panel may
replace the single critic without changing the surrounding loop.

### 3. Review

**Shape:** create an artifact, inspect it independently, then gate or revise.

**Use it when:** correctness can be checked separately from generation. Prefer deterministic checks in `Do`
and `Guard`; use an agent only for qualities that code cannot evaluate.

```go
reviewed := flow.Seq(produce, reviewPanel, approvedGuard)
```

A review should return structured findings with severity and evidence. Never let prose such as “looks good” be
the only value controlling a side effect.

### 4. Committee

**Shape:** fixed independent members answer the same question, then a chair synthesizes their reports.

**Use it when:** distinct perspectives matter and independence reduces anchoring. It costs roughly one model
call per member plus synthesis.

```go
committee := flow.Then(
    flow.Parallel(productView, securityView, operationsView),
    flow.Agent[[]Opinion, Decision]("chair", renderOpinions),
)
```

Members are blind in `Parallel`. If they must react to each other, use debate or roundtable.

### 5. Debate

**Shape:** named participants alternately add claims and rebuttals to a transcript; a judge closes the debate.

**Use it when:** adversarial examination is valuable enough to justify extra turns. Avoid it for routine work
where an independent panel is cheaper and less prone to rhetorical drift.

```go
debate := flow.Router("debate", flow.RouterConfig[Work]{
    Participants: map[string]flow.Step[Work, Work]{"pro": pro, "con": con, "judge": judge},
    Select:       debateTurn,
    Done:         func(w Work) bool { return w.Done },
    Max:          7,
})
```

Store every claim, evidence reference, and speaker in `Transcript`. Make the final judge a distinct persona and
cap both rounds and context growth.

### 6. Vote

**Shape:** independent voters emit a constrained ballot; deterministic code counts it.

**Use it when:** the decision is categorical and majority, plurality, quorum, or a weighted score is a valid
policy. Voting is not synthesis: it discards minority reasoning unless you preserve it explicitly.

```go
vote := flow.Reduce(flow.Parallel(voterA, voterB, voterC), countBallots)
```

Define tie-breaking, abstention, quorum, and invalid-ballot behavior in Go. Use an odd panel only when simple
majority is actually the desired rule.

### 7. Tournament

**Shape:** candidates are compared pairwise through a bracket until one remains.

**Use it when:** judging two candidates is more reliable than assigning absolute scores to many. It takes more
judgments and can be sensitive to bracket order.

Build a fixed bracket from nested `Parallel` comparisons, or use `Loop` over a typed `Candidates` state for a
runtime-sized bracket. Randomized seeding must be recorded in state so a run is reproducible. Preserve losing
scores and reasons for auditability rather than returning only the winner.

### 8. Roundtable

**Shape:** a fixed cast takes turns in a known order, each seeing the accumulated transcript.

**Use it when:** participants should build on prior contributions. Use a committee when independence matters;
use debate when roles are explicitly adversarial.

```go
roundtable := flow.Router("roundtable", flow.RouterConfig[Work]{
    Participants: speakers,
    Select:       nextSpeakerRoundRobin,
    Done:         consensusOrBudget,
    Max:          12,
})
```

Keep a cursor and round number in state. A separate synthesizer should close the discussion; do not treat the
last speaker as consensus by accident.

### 9. Mesh

**Shape:** actors collaborate through a membership registry and shared messages in checkpointed state.

**Use it when:** the set of active actors is discovered at runtime. If the cast is known, `Router` is simpler.

```go
mesh := flow.Network("investigation", flow.NetworkConfig[Work]{
    Actors: actorBehaviors,
    Next:   scheduleFromState,
    Max:    32,
})
```

Flow's current `Network` is a **turn-scheduled mesh**, not simultaneous mailbox actors. `Next` must be pure and
actors spawn or retire peers by updating state. If true concurrent directed messaging is required, model it as
an external port inside `Do` or wait for a dedicated concurrent actor primitive; do not claim `Network` has
those semantics.

### 10. Pair

**Shape:** two roles work the same artifact in tight alternating turns, such as driver/navigator or
implementer/reviewer.

**Use it when:** rapid feedback is more useful than independent opinions.

Use `Seq(driver, navigator)` for one exchange or a bounded `Loop` for repeated exchanges. State should say who
acts next and carry concrete feedback. Pairing is cheaper than a committee and more interactive than a blind
review.

### 11. Relay

**Shape:** each specialist enriches and hands the artifact to the next specialist exactly once.

**Use it when:** responsibility changes in a known order. This is the typed pipeline form.

```go
relay := flow.Then(research, flow.Then(draft, publish))
// For Step[Work, Work] stages: flow.Seq(research, draft, publish)
```

Give each hand-off an honest output type. If all stages use a giant state struct merely for convenience, use
`Bind` to retain narrow sub-workflow contracts.

### 12. RACI

**Shape:** responsible actors do the work, an accountable actor approves it, consulted actors advise, and
informed actors receive the result.

**Use it when:** governance and ownership matter as much as generation.

Compose consulted roles as a `Parallel` panel, bind their advice into state, run the responsible step, place a
`Human` or accountable agent gate after it, then notify informed parties in deterministic `Do` steps. Exactly
one accountable decision should control the guarded side effect. Persist role, decision, feedback, and time in
the workflow state or an external audit port.

### 13. Retro

**Shape:** inspect completed runs, find recurring successes/failures, and propose changes to workflows,
personas, or skills.

**Use it across runs**, not inside the run being evaluated. A `Do` step loads events and outcomes through an
application port, agents analyze them, and a `Human` gate approves any change. Never let a retrospective
silently rewrite production instructions. Compare changes against stable measures such as pass rate, cost,
latency, and human rejection rate.

### 14. Human

**Shape:** suspend durably, show a prompt, and fold an operator decision back into the typed value.

```go
approval := flow.Human("publish", func(w Work, d flow.Decision) Work {
    w.Approved, w.Feedback = d.Approved, d.Feedback
    return w
}, renderApproval)
```

Use it for irreversible actions, policy decisions, ambiguity, or high-impact exceptions—not to compensate for
an undefined automated contract. Follow it with `Guard` before the side effect. A `Human` can be nested inside
`Then`, `Parallel`, `Map`, `Bind`, `Route`, `Router`, or `Network` and resumes from its checkpoint.

## Combinator-desugared patterns and aliases

### 15. Handoff

**Shape:** the current specialist explicitly selects the next specialist and transfers typed context.

Use `Router` when a participant may hand off repeatedly, or `Route` for a one-time transfer. Record the next
participant and reason in state, then let a pure selector dispatch it. Reject unknown recipients and enforce a
turn cap. A handoff is not a background spawn; ownership moves to one next actor.

### 16. Swarm

**Shape:** a coordinator discovers tasks and changes worker membership while work proceeds.

Use `Network` with backlog, active members, completed results, and scheduling cursor in state. This is the same
dynamic-membership family as mesh, but “swarm” emphasizes recruiting/retiring workers rather than peer
conversation. Flow schedules actors one turn at a time; fan independent tasks out with `Map` when true
concurrency is the actual need.

### 17. Best-of-N

**Shape:** generate N independent candidates, then choose one with a judge or deterministic scorer.

```go
best := flow.Then(
    flow.Parallel(candidateA, candidateB, candidateC),
    flow.Agent[[]Candidate, Candidate]("judge", renderCandidates),
)
```

Use it when generation variance is high and selection is easier than creation. Blind candidates to each other,
shuffle only with a recorded seed, and require the judge to return an index plus rationale. A deterministic
scorer in `Do` is preferable when possible.

### 18. Mixture of Experts (MoE)

**Shape:** a router chooses the single expert best suited to the input.

```go
moe := flow.Route(classify, map[string]flow.Step[Request, Answer]{
    "billing": billing,
    "legal":   legal,
    "technical": technical,
}).Default(generalist)
```

Use it to avoid paying every expert. Validate classifier confidence and define a fallback. Unlike ensemble,
only one expert normally runs.

### 19. Ensemble

**Shape:** several experts solve the same task and their answers are combined.

Use `Parallel` followed by deterministic aggregation or a synthesizer agent. Ensemble improves robustness at
roughly linear call cost. Keep prompts/roles genuinely diverse; cloning one persona N times often yields
correlated errors. This differs from MoE because all selected members run.

### 20. Mixture of Agents (MoA)

**Shape:** multiple proposal agents run, an aggregator creates a new candidate, and one or more layers repeat.

Represent each layer as `Parallel` proposals followed by a synthesis step; compose layers with `Then` or lift
them onto a `Work` spine with `Bind`. Bound the number of layers and compress prior outputs between layers to
control context. Use best-of-N when selection is enough; MoA pays extra for synthesis.

### 21. Plan–Execute

**Shape:** a planner emits typed tasks, workers execute them, and a reducer assembles the result.

```go
planExecute := flow.Then(planner,
    flow.Then(flow.Map(executor), flow.Do("assemble", assembleResults)))
```

Use `Parallel` instead of `Map` for a fixed task set. Validate the plan before execution, encode dependencies
rather than assuming list order, and insert a `Human` gate before expensive or destructive execution. For
cross-run isolation, a `Do` step can call `app.SpawnAwait` for registered child workflows.

### 22. Cascade

**Shape:** start with the cheapest adequate worker and escalate only when confidence or checks fail.

Model the current rung, result, and evaluation in a typed state and use `Router` to select the next rung. A
deterministic check should stop the cascade as soon as quality is sufficient. Set a final fallback and a total
budget. Cascade optimizes cost without giving up a stronger model for difficult cases.

### 23. Escalation Ladder

This is the operational alias of **cascade**. The emphasis is policy: each rung has an entry condition,
resource budget, and terminal failure behavior. Implement it with the same bounded `Router` recipe. Record why
each escalation occurred; otherwise routing quality and cost cannot be improved later.

### 24. Reflexion

**Shape:** act, evaluate the result, write a compact lesson, and retry with that lesson.

Use `Loop` over state containing artifact, evaluation, reflection, and attempt count. Reflection must change the
next prompt; merely appending an ever-growing transcript is not reflexion. Keep lessons concise and scoped to
the run unless a reviewed retro promotes them into a durable skill.

### 25. Scatter–Gather

**Shape:** scatter runtime items to the same worker and gather all results.

```go
scatterGather := flow.Then(flow.Map(worker), flow.Do("gather", gather))
```

Use it for independent partitions. Preserve item identifiers because completion order is not a business
contract. Define partial-failure policy explicitly; do not silently drop failed partitions.

### 26. MapReduce

This is the data-processing alias of **scatter–gather**. `Map` performs runtime-sized fan-out; `Reduce` folds
when the reduced type equals each mapped output type. To gather into a different type, follow `Map` with
`Do[[]Out, Summary]`. Map branches do not share `StateDo` state—coordination belongs in the gather phase.

### 27. Supervisor

**Shape:** a supervisor repeatedly assigns work to a fixed worker roster and decides when the shared task is
complete.

This is the role-oriented name for **orchestrate** and uses `Router`. Keep supervision policy inspectable in
state: assignment, worker result, acceptance decision, and next action. Avoid a supervisor agent that can loop
without a deterministic `Max` or completion predicate.

### 28. ReAct

**Shape:** reason about the task, call a tool, observe the result, and continue until producing the typed answer.

In flow, grant tools to the persona and use an ordinary `Agent`; the engine lowers a tool-bearing persona to a
native model↔tool loop.

```go
lookup := flow.Agent[Question, Answer]("researcher", renderQuestion)
```

Tools are deny-by-default and granted in persona/role configuration. Constrain filesystem and shell patterns,
keep irreversible operations behind human approval, and cap retries/model budgets at the surrounding workflow.

### 29. Tool Loop

This is the implementation-oriented alias of **ReAct**. Authors do not manually write the inner tool-call
cycle: `Agent` plus a tool-bearing persona provides it. Use an explicit `Loop` only for workflow-level
iteration across complete agent invocations, not for individual model tool calls.

## State-coordinated patterns and aliases

### 30. Blackboard

**Shape:** specialists read and write a shared knowledge structure, with scheduling driven by its contents.

Prefer a typed `Work` spine with `Seq`, `Bind`, and optionally `Router`. Use `StateDo` only for graph-local
coordination that cannot travel cleanly on edges. `StateDo` state is not shared across `Parallel`/`Map`
boundaries and must be registered with `engine.Register[T]` if it crosses a human checkpoint. Define ownership
or merge rules for every field.

### 31. AutoLoop

**Shape:** maintain a task queue, select the next task, execute it, add newly discovered tasks, and stop on an
empty queue or budget.

Use bounded `Router` or `Loop` over state containing queue, completed task IDs, results, attempts, and total
budget. Task IDs make re-entry idempotent. Validate newly generated tasks, deduplicate them, and require human
approval before side effects. Dynamic task creation does not require dynamic actor creation; prefer `Router`
unless the worker roster also changes.

### 32. BabyAGI

This is a well-known name for the **AutoLoop** task-creation/prioritization/execution cycle. Implement it with
the same typed queue recipe. Treat historical BabyAGI prompts as inspiration, not a production contract: add
hard termination, typed tasks, idempotency, deterministic tests, and approval boundaries.

### 33. Tree Search

**Shape:** expand several candidate states, score them, retain the best frontier, and repeat to a depth/node
budget.

Use `Loop` over a typed tree/frontier state. Inside each iteration, `Map` can expand frontier nodes and a `Do`
step can score, deduplicate, prune, and select the next frontier. Record parent IDs, depth, score, terminal
status, and expansion budget. Search can grow exponentially; width, depth, token, and wall-clock limits are
part of correctness, not tuning extras.

### 34. Tree of Thoughts (ToT)

This is the language-model reasoning variant of **tree search**: agents propose “thought” continuations and a
judge scores/prunes them. Use the same bounded frontier recipe, but keep thoughts as structured candidate
states rather than an opaque transcript. Prefer best-of-N for one expansion layer; use ToT only when exploring
multiple dependent decisions justifies the much higher cost.

## Combining paradigms into larger workflows

Build reusable sub-workflows as ordinary Go functions returning `Step` or `Workflow`. `Bind` is the adapter
between a narrow sub-workflow and a larger state spine:

```go
func Research() flow.Step[Question, Brief] { return researchCommittee }
func DraftAndReview() flow.Step[Brief, Draft] { return refineDraft }

type Job struct {
    Question Question
    Brief    Brief
    Draft    Draft
}

root := flow.Seq(
    flow.Bind(Research(),
        func(j Job) Question { return j.Question },
        func(j Job, b Brief) Job { j.Brief = b; return j }),
    flow.Bind(DraftAndReview(),
        func(j Job) Brief { return j.Brief },
        func(j Job, d Draft) Job { j.Draft = d; return j }),
    flow.Human("publish", applyDecision, renderDraft),
    flow.Guard("approved", flow.StateGate(func(j Job) bool { return j.Draft.Approved })),
)
```

That example combines committee, refine, human, and review/guard patterns while each child keeps narrow types.
At the runtime boundary, `app.SpawnAwait` composes separately registered workflows as parent/child runs. Prefer
in-process `Step` composition when one checkpoint tree and one typed call graph are desired; use child runs
when independent run records, lifecycle, or reusable registered services matter.

## Production checklist for every paradigm

1. Define typed inputs, outputs, state, and structured model contracts.
2. Choose the least dynamic combinator that expresses the topology.
3. Put a deterministic cap on loops, turns, fan-out, and recursive child runs.
4. State ordering, tie, partial-failure, retry, and fallback policies explicitly.
5. Gate irreversible side effects and enforce the decision with `Guard`.
6. Keep selectors and scheduling functions pure; record non-deterministic choices in state.
7. Test topology and final state with `app.FakeProvider` before using a real gateway.
8. Emit enough state/events to explain who acted, why routing changed, and why the run stopped.
