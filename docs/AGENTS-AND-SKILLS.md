# Agents, skills, models, and tools

A flow `Agent` names a **persona**. Its prompt function supplies the **task for one invocation**. Keeping those
separate lets operators improve instructions, model routing, and permissions without recompiling workflow code.

## Resolution and precedence

By default the loader merges `~/.flow/config.yml` and `.flow/config.yml`; project definitions win. Set
`FLOW_CONFIG` to replace the project config path. Agent and skill roots default to both user and project
`.flow/{agents,skills}`. Later roots win by name. `app.Config{Agents, Skills, ConfigFiles}` can override paths.

Configuration changes are watched and marked dirty but are not activated under an in-flight process until an
explicit reload:

```sh
flow config status
flow config reload
```

A failed reload preserves the last valid snapshot.

## Configuration

```yaml
agents:
  - ~/.flow/agents
  - .flow/agents
skills:
  - ~/.flow/skills
  - .flow/skills

profiles:
  cheap: [small-model, fallback-model]
  coding:
    model: coding-model
    fallbackModels: [strong-model]
    temperature: 0.2
    maxCompletionTokens: 1200
    stop: ["END"]
  exploratory:
    models: [creative-model]
    topP: 0.9
    presencePenalty: 0.3
    frequencyPenalty: 0.1
    seed: 42

vars:
  repo: "./"

roles:
  reader:
    tools: ["read(**)", "grep(**)", "find(**)"]
    skills: [repository-reading]
  checker:
    allow: ["bash(go test ./...)"] # `allow` is accepted as a tools alias
```

A profile is an ordered model ladder and can set generation defaults for every persona using it. `temperature`
(0–2), `topP` (0–1), `maxCompletionTokens` (positive), `stop`, `presencePenalty` (-2–2),
`frequencyPenalty` (-2–2), and `seed` are passed to the OpenAI-compatible provider. Omit a setting to
preserve the provider's default; an explicit `temperature: 0` is honored. Usually set either `temperature`
or `topP`, not both. Roles bundle tool grants and skill names. Variables substitute into grants. Keep secrets
out of YAML; provider credentials come from environment variables.

## Persona files

```markdown
---
name: reviewer
profile: cheap
roles: [reader]
tools: []
skills: [go-review]
---
You review changes for correctness, security, and maintainability. Cite concrete evidence.
```

`name` is required and must match `flow.Agent(..., "reviewer", ...)`. A profile must resolve to at least one
model. The legacy `model` field is accepted but new personas should use `profile`. Role and inline grants are
unioned. Skill bodies are appended to the system instruction.

Keep a persona focused on identity, standards, and stable output behavior. Do not include the current ticket,
repository path, or transient task; those belong in the prompt input.

## Skills

Skills follow the portable `SKILL.md` convention:

```markdown
---
name: go-review
---
Check error handling, concurrency ownership, API compatibility, tests, and documentation.
```

Store one concern per skill so roles/personas can reuse it. Treat changes as code: review them, test workflows
that depend on them, and reload explicitly.

## Least-privilege tools

A persona with no grants has no tools. The built-in registry includes root-confined `bash`, `read`, `write`,
`edit`, `ls`, `grep`, and `find`. A grant has `tool(pattern)` form. Shell matching rejects command chaining and
substitution; still grant exact commands whenever practical.

```yaml
roles:
  docs:
    tools:
      - "read(docs/**)"
      - "grep(docs/**)"
      - "write(docs/**)"
  tests:
    tools:
      - "bash(go test ./...)"
```

Configure `app.Config.Tools` to replace or extend executable tools. A textual grant without a corresponding
tool implementation cannot execute. Root tools at the actual workspace; never expose a broader directory just
to avoid defining a proper port.

MCP servers configured through `app.Config.MCPServers` contribute discovered tools under names such as
`mcp__github__search_repositories`. They remain deny-by-default: grant the generated name exactly as for any
other tool. See [the runtime MCP guide](RUNTIME.md#mcp-tools) for connection configuration and trust boundaries.

## Model fallbacks and contracts

The gateway walks `Model` then `FallbackModels` on provider failure. Output is decoded into the `Agent`'s Go
output type. Design small structs with JSON tags and fields whose semantics are unambiguous. Prefer enums,
booleans, scores with stated ranges, and evidence arrays over unstructured verdict prose.

Use fallback ladders for availability or capability escalation, not silent policy changes. Tests should use
`app.FakeProvider` and exact structured responses; production gateway smoke tests are optional.
