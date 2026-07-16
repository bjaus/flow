// Package engine compiles a flow workflow into a NATIVE cloudwego/eino graph: each step becomes a real eino
// node (edges, branches, cycles), and each Agent becomes a real ChatModel node — or a native ReAct loop when
// it has tools — so streaming, per-node tracing, concurrency, tool loops, and durable interrupt/resume are all
// native, not emulated. The DSL's job is to delete the graph-wiring boilerplate; eino keeps all of its power,
// and every seam eino exposes (compile options, callbacks, the checkpoint store, this registry) stays open.
package engine

import (
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

// Register makes a custom type durable: register any type whose VALUE crosses a human gate (a Human step's
// input) once, so eino can serialize it into the checkpoint. Primitive types (string, int, …) and the
// workflow's In/Out types are handled automatically; register only intermediate struct types that a Human
// step carries. Idempotent.
func Register[T any]() { safeRegister[T]() }

// safeRegister registers T with eino's serializer, tolerating a duplicate (idempotent across Compiles).
func safeRegister[T any]() {
	defer func() { _ = recover() }()
	schema.Register[T]()
}

// Persona is a resolved agent identity: the chat model an Agent step runs, its system instruction, and the
// tools it may call. When Tools is non-empty the Model MUST implement model.ToolCallingChatModel — the engine
// then lowers the Agent to a native ReAct loop (ChatModel ⇄ ToolsNode) instead of a single completion, so a
// flow Agent is a real tool-using agent. Leave Tools nil for a plain single-shot completion.
type Persona struct {
	Model  model.BaseChatModel
	System string
	Tools  []tool.BaseTool
}

// Registry resolves a persona name (what an Agent step references) to a Persona. Production returns
// gateway-backed models (optionally tool-bound); tests return a fake streaming model. This is the only seam
// between the workflow and the outside world.
type Registry interface {
	Persona(name string) (Persona, error)
}

// RegistryFunc adapts a function to a Registry.
type RegistryFunc func(name string) (Persona, error)

func (f RegistryFunc) Persona(name string) (Persona, error) { return f(name) }
