# Getting started

This guide builds a real project around flow's public modules. It uses the structure that proved effective in
the first production consumer: a tiny composition root, workflow constructors grouped in a package, ports for
side effects, markdown personas, deterministic fake-model tests, and one repository-wide check command.

## Prerequisites

- Go **1.26.5 or newer** (the version declared by both modules)
- `just` and `golangci-lint` if you use the recommended development loop
- an OpenAI-compatible endpoint for real agent calls; deterministic tests need no endpoint

## Scaffold or create the project

The scaffolder writes a starter into the current directory and refuses to overwrite existing files:

```sh
mkdir my-workflows && cd my-workflows
go run github.com/bjaus/flow/app/cmd/flow@latest init
go mod tidy
go run . serve
```

To create it manually:

```sh
mkdir my-workflows && cd my-workflows
go mod init example.com/my-workflows
go get github.com/bjaus/flow/app
mkdir -p workflows .flow/agents .flow/skills
```

Use your real module path when the project will be published. Commit `go.mod` and `go.sum`; do not commit
credentials or a local `flow.db`.

## Recommended layout

```text
my-workflows/
├── main.go                    # composition root only
├── workflows/
│   ├── triage.go              # typed workflow constructors
│   ├── triage_test.go         # deterministic topology/result tests
│   └── ports.go               # interfaces for tracker, git, mail, etc.
├── adapters/                  # production implementations of workflow ports
├── .flow/
│   ├── config.yml             # profiles, roles, roots, variables
│   ├── agents/*.md            # reusable personas
│   └── skills/*/SKILL.md      # reusable instructions
├── .env.example               # variable names, never secrets
├── justfile                   # one check/dev entry point
├── go.mod
└── go.sum
```

This separation matters:

- **workflows own policy** and depend on small interfaces;
- **adapters own external systems** such as GitHub, Linear, files, and commands;
- **`main` wires dependencies and registers workflows**;
- **personas describe who an agent is**, while the `Agent` prompt describes this invocation's task;
- **tests replace ports and models with fakes**, so normal verification spends no tokens and has no network
  side effects.

## Write a typed workflow

```go
package workflows

import (
    "context"

    "github.com/bjaus/flow"
)

type Ticket struct {
    Title string `json:"title"`
}

type Plan struct {
    Steps    []string `json:"steps"`
    Approved bool     `json:"approved"`
    Feedback string   `json:"feedback,omitempty"`
}

func Triage() flow.Workflow[Ticket, Plan] {
    plan := flow.Agent[Ticket, Plan]("planner", func(t Ticket) string {
        return "Create an implementation plan for: " + t.Title
    })
    approve := flow.Human("approve-plan",
        func(p Plan, d flow.Decision) Plan {
            p.Approved, p.Feedback = d.Approved, d.Feedback
            return p
        },
        func(Plan) string { return "Approve this plan?" },
    )
    guard := flow.Guard("approved",
        flow.StateGate(func(p Plan) bool { return p.Approved }))

    return flow.Define("triage", "Plan and approve a ticket",
        flow.Then(flow.Then(plan, approve), guard))
}
```

The Go compiler checks every edge. Keep domain types explicit and JSON-tagged because the runtime API decodes
run input and stores results as JSON.

## Add a persona and profile

`.flow/config.yml`:

```yaml
profiles:
  planning:
    model: fast-model
    fallbackModels: [strong-model]
roles:
  read-only:
    tools: ["read(**)", "grep(**)", "find(**)"]
    skills: [planning]
```

`.flow/agents/planner.md`:

```markdown
---
name: planner
profile: planning
roles: [read-only]
tools: []
skills: []
---
You are a software architect. Return a concrete, ordered plan that maps every requirement to verification.
```

`.flow/skills/planning/SKILL.md`:

```markdown
---
name: planning
---
State assumptions, affected components, acceptance criteria, and exact checks.
```

The persona name must equal the name passed to `flow.Agent`. Profiles select an ordered model ladder without
putting provider details in workflow code. Tools are deny-by-default and grants should be narrow. See
[Agents, skills, and tools](AGENTS-AND-SKILLS.md).

## Wire the runtime

```go
package main

import (
    "fmt"
    "os"

    "example.com/my-workflows/workflows"
    "github.com/bjaus/flow/app"
)

func main() {
    a, err := app.New(app.Config{})
    if err != nil {
        fmt.Fprintln(os.Stderr, "initialize:", err)
        os.Exit(1)
    }
    if err := a.Register(workflows.Triage()); err != nil {
        fmt.Fprintln(os.Stderr, "register:", err)
        os.Exit(1)
    }
    if err := a.CLI().Execute(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Defaults are `./flow.db`, `:7788`, the user/project `.flow` roots, built-in root-confined tools, an
OpenAI-compatible gateway, and no-op external tracing.

For a real gateway:

```sh
export FLOW_GATEWAY_URL=http://localhost:4000/v1
# Set the credential expected by your compatible gateway in the environment.
go run . serve
```

Then, from another terminal:

```sh
go run . workflows list
go run . runs trigger triage --input '{"title":"Add audit logging"}'
go run . runs watch RUN_ID
go run . runs approve RUN_ID
# or: go run . runs return RUN_ID --feedback 'Include retention policy'
```

Open `http://localhost:7788` for the web UI or run `go run . tui`.

## Put side effects behind ports

Do not make workflow policy depend directly on one vendor client. Define the smallest interface the workflow
needs and inject it into the constructor:

```go
type Tracker interface {
    Issue(context.Context, string) (Issue, error)
    Complete(context.Context, string) error
}

type Deps struct { Tracker Tracker }

func IssueToReport(d Deps) flow.Workflow[Request, Report] {
    fetch := flow.Do("fetch-issue", func(ctx context.Context, r Request) (Issue, error) {
        return d.Tracker.Issue(ctx, r.ID)
    })
    // compose fetch with agents, checks, and gates...
}
```

Production `main` supplies a vendor adapter. Tests supply an in-memory fake and assert its calls. Make external
steps idempotent because crash recovery may re-enter the last uncheckpointed step.

## Build reusable workflow components

Expose narrow constructors returning `Step`, then compose them in larger workflow constructors:

```go
func Research() flow.Step[Question, Brief] { /* ... */ }
func Review() flow.Step[Draft, Verdict] { /* ... */ }

func Publish() flow.Workflow[Request, Job] {
    return flow.Define("publish", "Research, draft, review, and publish",
        flow.Seq(
            flow.Bind(Research(), readQuestion, writeBrief),
            flow.Bind(Review(), readDraft, writeVerdict),
            approval,
            publishGuard,
            publishSideEffect,
        ))
}
```

Use `Bind` as a lens: the child retains honest `In → Out` types while the parent carries a broader state. Use
`app.SpawnAwait` inside a `Do` only when separately registered workflows should have parent/child run records.
See [Composition](COMPOSITION.md) and the [34 paradigms](PARADIGMS.md).

## Add the zero-token test first

Use `app.FakeProvider` for model steps and fakes for every side-effect port. Script structured JSON matching
the agent output type, invoke through the runtime or engine, and assert both the final typed/JSON state and the
calls made. Keep one optional gateway smoke test behind an environment check; it is not part of normal CI.

A useful `justfile` baseline:

```make
set dotenv-load

check: fmt
    go build ./...
    golangci-lint run
    go test ./...
    go mod tidy -diff
    govulncheck ./...

fmt:
    gofmt -w .
    goimports -w .

dev:
    watchexec -r -e go,md,yml -- go run . serve
```

Pin tool versions in CI, keep local and CI commands identical, and run `just check` before review. See
[Testing and operations](TESTING.md).

## Next reading

1. [Authoring reference](AUTHORING.md)
2. [The 34 multi-agent paradigms](PARADIGMS.md)
3. [Composition and child workflows](COMPOSITION.md)
4. [Agents, skills, and tools](AGENTS-AND-SKILLS.md)
5. [Runtime and clients](RUNTIME.md)
6. [Testing and operations](TESTING.md)
