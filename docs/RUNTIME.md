# Runtime and client reference

Import `github.com/bjaus/flow/app` to turn compiled-in workflows into a durable local service. The default
runtime uses SQLite, one cross-run worker, an HTTP/SSE API, CLI and TUI clients, and an embedded htmx PWA.

## Construct and register

```go
stores, err := app.SQLite("flow.db")
if err != nil { return err }
defer stores.Close()

a, err := app.New(app.Config{
    Store: stores,
    Listen: ":7788",
    Provider: app.Gateway(os.Getenv("FLOW_GATEWAY_URL")),
    DrainTimeout: 30 * time.Second,
})
if err != nil { return err }
if err := a.Register(workflows.Triage()); err != nil { return err }
return a.Serve(ctx)
```

`app.New(app.Config{})` applies local defaults. `Register` validates structure, rejects duplicate workflow
names, and checks referenced personas when a registry is active. Register every workflow before `Serve`.

## Important `Config` fields

| Field | Purpose |
|---|---|
| `Store` | bundled checkpoint/run/event stores |
| `Checkpoint`, `RunStore`, `Events` | swap individual persistence ports |
| `Provider` | model provider; `Gateway` or `FakeProvider` |
| `AgentRegistry` | persona resolver |
| `Tracer` | external traces; no-op by default |
| `Agents`, `Skills`, `ConfigFiles` | registry path overrides |
| `Tools` | executable tool registry; extended with discovered MCP tools |
| `MCPServers` | MCP servers to discover at startup |
| `Listen` | daemon address, default `:7788` |
| `DrainTimeout` | graceful shutdown bound, default 30 seconds |
| `DrainOnly` | resume compatible pinned work but reject new work |
| `Triggers` | cron-scheduled workflow inputs |

Ports are independent interfaces, so a custom store/provider/tracer does not change workflow definitions.

## MCP tools

Configure MCP servers through `Config.MCPServers`. Flow connects and lists tools during `app.New`; a failed connection or discovery prevents startup. Each discovered tool is exposed as `mcp__<server>__<tool>` and is added to the normal executable tool registry. Connections close when `Serve` returns (or when `App.Close` is called).

```go
import (
    "github.com/bjaus/flow/app"
    "github.com/bjaus/flow/app/mcp"
)

_, err := app.New(app.Config{
    MCPServers: []mcp.Server{{
        Name: "github",
        Command: "npx",
        Args: []string{"-y", "@modelcontextprotocol/server-github"},
        // Env replaces the child process environment when non-empty. Supply
        // credentials from the host environment rather than source code.
    }},
})
```

Import `github.com/bjaus/flow/app/mcp` for `mcp.Server`. A server has a `Command` plus `Args` (stdio), or a `URL`; URLs use Streamable HTTP by default and the legacy SSE transport when `SSE: true` is set. `Name` may contain letters, digits, `_`, and `-` only. Flow does not automatically grant discovered tools: explicitly grant each tool to a persona or role, including an argument pattern appropriate to the server's API.

```yaml
roles:
  github-reader:
    tools:
      - "mcp__github__search_repositories(*)"
```

Flow currently exposes MCP **tools** only; it does not inject MCP resources or prompts into an agent. MCP tool calls run through the same deny-by-default grant guard as built-in and application-supplied tools. A grant is not a sandbox: only configure servers you trust, use narrowly scoped server credentials, and avoid blanket `(*)` grants when a server's arguments identify a resource that can be restricted.

## Runs and durability

Statuses are `queued`, `running`, `awaiting_review`, `parked`, `needs_migration`, `succeeded`, `failed`, and
`canceled`. One worker claims runs FIFO; concurrency inside `Parallel`/`Map` remains real. A human interrupt
checkpoints state and frees the worker for another run.

On shutdown the daemon stops claiming work, lets the current step finish, and parks at a checkpoint boundary.
On startup, compatible parked/running runs resume. Each registered workflow has a structural fingerprint; a
shape mismatch becomes `needs_migration` rather than silently applying an incompatible checkpoint.

External effects should be idempotent and their artifact paths should live in checkpointed state. Use
`workspace.Manager` for run-keyed git worktrees when workflows modify repositories.

## Scheduled triggers

```go
app.Config{Triggers: []app.Trigger{{
    Name: "nightly-review",
    Workflow: "review",
    Spec: "0 2 * * *",
    Input: json.RawMessage(`{"scope":"all"}`),
}}}
```

Specs use standard five-field cron syntax. Names must be unique, and inputs are validated at serve time. A
firing is skipped while draining or while that trigger's prior run remains active, preventing schedule pileup.

## CLI

A binary using `a.CLI()` exposes:

```text
flow serve [--drain-only]
flow workflows list [--json]
flow runs trigger <workflow> --input '<json|@file>'
flow runs list [--status S] [--workflow W] [--parent ID] [--json]
flow runs get <id> [--json]
flow runs watch <id>
flow runs approve <id> [--feedback TEXT]
flow runs return <id> [--feedback TEXT]
flow runs cancel <id>
flow runs migrate <id> <restart|abandon|finish_on_previous>
flow config status
flow config reload
flow tui
```

`app.ClientCLI()` provides client-only commands for a standalone binary. `--endpoint` defaults to
`http://localhost:7788`.

## Embedded application API

A host can use the same operations without HTTP:

- `Workflows()` discovers registered definitions;
- `Trigger(ctx, workflow, jsonInput)` enqueues a run;
- `ListRuns` and `GetRun` query lifecycle state;
- `Decide` applies a human decision;
- `Cancel` requests cancellation;
- `Migrate` resolves `needs_migration` with `restart`, `abandon`, or `finish_on_previous`;
- `ConfigStatus` and `ReloadConfig` manage the active persona/config snapshot;
- `EventSink` exposes event subscription;
- `Close` releases runtime-owned stores and MCP connections;
- `Handler` returns the mounted API and web application;
- `CLI` and `TUI` expose bundled clients.

All operations accept caller contexts where relevant. Keep one `App` as the owner of its worker and stores;
do not run `Serve` more than once concurrently on the same instance.

## HTTP/SSE API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/workflows` | discover workflows and types |
| `POST` | `/api/runs` | enqueue `{workflow,input}` |
| `GET` | `/api/runs` | filter by status, workflow, or parent |
| `GET` | `/api/runs/{id}` | fetch a run |
| `GET` | `/api/runs/{id}/events` | replay then stream one run |
| `GET` | `/api/events` | replay/stream all events |
| `POST` | `/api/runs/{id}/decision` | apply approval/feedback |
| `POST` | `/api/runs/{id}/cancel` | request cancellation |
| `POST` | `/api/runs/{id}/migration` | resolve incompatible state |
| `GET` | `/api/config` | registry status |
| `POST` | `/api/config/reload` | activate a valid snapshot |

Events include run/step start and finish, streamed agent tokens, gates, decisions, parking/resume, configuration
changes, and skipped triggers. They carry monotonic per-run sequence numbers, allowing late subscribers to
replay before following live.

## Child runs

Inside runtime-driven `Do`, `app.SpawnAwait(ctx, name, input)` invokes another registered workflow. Use
`app.SpawnerFrom` for separate spawn/await. Parent IDs are queryable and cancellation cascades to descendants.
See [Composition](COMPOSITION.md) for lifecycle tradeoffs.

## Clients

- Web UI: daemon root `/`; static shell is embedded and installable.
- TUI: `flow tui`; consumes the same HTTP/SSE API.
- CLI: script-friendly tables or JSON.
- Custom client: use the documented JSON/SSE endpoints; no engine dependency is required.

## Observability

`EventSink` drives product clients and durable replay. `Tracer` is a separate external observability port.
`app.OTLPTracer(ctx)` reads standard OTLP environment configuration and returns a shutdown function. Always
call that shutdown function so buffered spans flush. The no-op tracer has no export overhead.

## In-process engine use

Import `github.com/bjaus/flow/engine` when you need a typed `Runnable` without daemon lifecycle. `Compile`
accepts a persona registry, optional eino checkpoint store, and additional graph compile options. `Runnable`
exposes `Invoke`, `Stream`, `Collect`, `Transform`, and `Underlying`. Runtime applications should normally use
`app` rather than duplicate queue, checkpoint, and human-resume handling.
