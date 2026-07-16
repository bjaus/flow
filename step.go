// Package flow is a type-safe DSL for authoring agentic workflows. The core is one abstraction: a typed
// Step[In, Out]. Leaves do work; combinators arrange steps and return steps, so composition is unbounded.
// Data flows along typed edges; an ambient state is used only for coordination. The DSL is otherwise
// stdlib-only; the single external touch is registering a Human step's carried type with the checkpoint
// serializer at construction (durable HITL is inherently a backend concern), so no author registration is
// needed for durable workflows.
package flow

import (
	"context"
	"reflect"

	"github.com/bjaus/flow/internal/ir"

	"github.com/cloudwego/eino/schema"
)

// registerDurable registers a type with the checkpoint serializer so its value can cross a human gate.
// Idempotent (tolerates re-registration across constructions).
func registerDurable[T any]() {
	defer func() { _ = recover() }()
	schema.Register[T]()
}

// Step is a typed unit of execution: In -> Out. Reusable and unit-testable in isolation; a backend compiles
// it to run. The zero value is not useful — always construct via a leaf or combinator.
type Step[In, Out any] struct{ n *ir.Node }

func typeOf[T any]() reflect.Type { return reflect.TypeFor[T]() }

// ---- leaves ----

// Do lifts a plain typed function into a step (the deterministic primitive).
func Do[In, Out any](name string, fn func(context.Context, In) (Out, error)) Step[In, Out] {
	return Step[In, Out]{n: &ir.Node{
		Kind: ir.KAction, Name: name, In: typeOf[In](), Out: typeOf[Out](),
		Invoke: func(ctx context.Context, in any) (any, error) { return fn(ctx, in.(In)) },
	}}
}

// Agent is an LLM leaf, typed on input AND output. The persona is resolved by name at runtime; `prompt`
// renders the typed input into the task; Out's type defines the structured-output contract. The actual
// model call is supplied by the backend's provider (so the leaf stays pure and testable with a fake).
func Agent[In, Out any](name string, prompt func(In) string) Step[In, Out] {
	return Step[In, Out]{n: &ir.Node{
		Kind: ir.KAgent, Name: name, Persona: name, In: typeOf[In](), Out: typeOf[Out](),
		Render: func(in any) string { return prompt(in.(In)) },
	}}
}

// StateDo is like Do but also gives the step read/write access to the workflow's shared state — an ambient
// value (of any type you choose) for coordination that typed edges can't express cleanly, such as a blackboard
// two steps both touch. Prefer edges and Bind when the data flow is linear; reach for shared state only for
// genuine coordination. The state is per-graph: it is initialized lazily (get returns nil until first set)
// and is NOT shared across a Parallel/Map fan-out boundary (each branch is its own graph). A workflow that
// both uses state and pauses at a Human gate must register its state type with engine.Register so it can
// cross the checkpoint.
func StateDo[In, Out any](name string, fn func(ctx context.Context, in In, get func() any, set func(any)) (Out, error)) Step[In, Out] {
	return Step[In, Out]{n: &ir.Node{
		Kind: ir.KAction, Name: name, In: typeOf[In](), Out: typeOf[Out](),
		StateInvoke: func(ctx context.Context, in any, get func() any, set func(any)) (any, error) {
			return fn(ctx, in.(In), get, set)
		},
	}}
}

// Decision is the generic operator answer a human gate yields.
type Decision struct {
	Approved bool
	Feedback string
}

// Human suspends the run for a person; the backend checkpoints and resumes, recovering the in-flight value.
// `apply` folds the operator's Decision into the passing value.
func Human[T any](name string, apply func(T, Decision) T, prompt func(T) string) Step[T, T] {
	registerDurable[T]() // the carried type crosses the checkpoint; register it once so no author setup is needed
	return Step[T, T]{n: &ir.Node{
		Kind: ir.KHuman, Name: name, In: typeOf[T](), Out: typeOf[T](),
		Render: func(in any) string { return prompt(in.(T)) },
		Apply:  func(in any, d any) any { return apply(in.(T), d.(Decision)) },
	}}
}

// ID names a step (for analysis, logging, duplicate-id validation).
func (s Step[In, Out]) ID(id string) Step[In, Out] { s.n.ID = id; return s }

// Retries sets how many times the step is re-attempted on error.
func (s Step[In, Out]) Retries(n int) Step[In, Out] { s.n.Retries = n; return s }
