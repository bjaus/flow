// Package flow is a type-safe DSL for agentic workflows.
//
// A Step[In, Out] is the only core abstraction. Leaves such as Do, Agent, and
// Human perform work; combinators such as Then, Parallel, Route, and Loop
// arrange steps and return another Step, so composition has no artificial
// nesting limit. Typed edges carry ordinary data. StateDo is reserved for
// coordination that cannot be represented cleanly by an edge.
//
// Agent names identify reusable personas resolved by the runtime, while an
// Agent's prompt function renders the task for one invocation. Keeping persona
// and task separate lets operators update instructions without recompiling a
// workflow.
//
// A minimal deterministic workflow is:
//
//	wf := flow.Define("double", "Double an integer",
//		flow.Do("double", func(_ context.Context, n int) (int, error) {
//			return n * 2, nil
//		}))
//
// Compile a Workflow with package engine for in-process execution, or register
// it with github.com/bjaus/flow/app for durable queueing, human review, and the
// HTTP, CLI, terminal, and web clients.
package flow
