# .agents/

Tool-agnostic helpers for coding agents working in this repo — deliberately not `.claude/`, `.cursor/`, or
any single tool's directory. The entry point is **[AGENTS.md](../AGENTS.md)** at the repo root; `CLAUDE.md`
is a symlink to it.

- **`skills/`** — task-specific guides in the portable `SKILL.md` format (frontmatter `name` + `description`,
  then instructions). Load the one relevant to the task at hand:
  - `flow-dsl` — authoring workflows with the `flow` package.
  - `engine-api` — compiling and running workflows via `engine` (streaming, durable HITL).
  - `verify-and-iterate` — `just` recipes, `vhs` TUI rendering, the gateway, determinism-first testing.
