// Package eino compiles a flow workflow into a NATIVE cloudwego/eino graph: each step becomes a real eino
// node (edges, branches, cycles), and each Agent becomes a real ChatModel node — so streaming, per-node
// tracing, concurrency, tool loops, and durable interrupt/resume are all native, not emulated. The DSL's job
// is to delete the graph-wiring boilerplate; eino keeps all of its power.
package engine

import (
	"github.com/cloudwego/eino/components/model"
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

// Registry resolves a persona name (what an Agent step references) to a live model plus its system
// instruction. Production returns gateway-backed models; tests return a fake streaming model. This is the
// only seam between the workflow and the outside world.
type Registry interface {
	Persona(name string) (m model.BaseChatModel, systemInstruction string, err error)
}

// RegistryFunc adapts a function to a Registry.
type RegistryFunc func(name string) (model.BaseChatModel, string, error)

func (f RegistryFunc) Persona(name string) (model.BaseChatModel, string, error) { return f(name) }
