// Package ir is the erased intermediate representation shared by the flow DSL (which builds it, fully typed)
// and a backend (which lowers it to an executable engine). It lives under internal/ so DSL users never touch
// it. A Node's SHAPE is data; its leaf BEHAVIOR rides as erased closures (typed at the DSL boundary).
package ir

import (
	"context"
	"reflect"
)

type Kind int

const (
	KAction   Kind = iota // a plain function leaf
	KAgent                // an LLM leaf (persona resolved by name; behavior supplied by the backend's provider)
	KHuman                // a durable human gate
	KThen                 // sequential, two typed children (edge)
	KSeq                  // sequential, N same-typed children
	KParallel             // concurrent fan-out, N children -> []Out
	KMap                  // runtime-sized fan-out over a body
	KReduce               // fold a fan-out
	KRoute                // classify to 1-of-N static cases
	KLoop                 // repeat a body until a gate
	KGate                 // standalone pass/fail
	KBind                 // lift a typed step into a state spine via lenses
	KRouter               // dynamic control: select a participant each turn (M1)
	KConverse             // shared-transcript group (reserved; backend-built)
	KNetwork              // actor mesh over a bus (reserved; backend-built)
)

// Erased behavior forms. The DSL wraps typed closures into these; the backend calls them.
type (
	Invoke      func(ctx context.Context, in any) (any, error)                                // a leaf / fold
	StateInvoke func(ctx context.Context, in any, get func() any, set func(any)) (any, error) // a leaf with shared-state access
	Predicate   func(ctx context.Context, in any) (bool, error)                               // a gate
	Selector    func(in any) string                                                           // a classifier / next-picker
	Lens        func(s any) any                                                               // read: S -> In
	Merge       func(s any, out any) any                                                      // write: (S, Out) -> S
	ApplyFn     func(in any, decision any) any                                                // human: (T, Decision) -> T
	RenderFn    func(in any) string                                                           // prompt render
)

// Node is one step's erased definition. Kind selects which fields are meaningful.
type Node struct {
	Kind Kind
	ID   string
	Name string
	In   reflect.Type
	Out  reflect.Type

	// leaves
	Invoke      Invoke      // KAction
	StateInvoke StateInvoke // KAction variant with shared-state access (flow.StateDo)
	Persona     string      // KAgent
	Render      RenderFn    // KAgent / KHuman

	// structure
	Steps []*Node // KThen (2), KSeq / KParallel (N)
	Body  *Node   // KMap / KLoop / KBind
	Over  *Node   // KReduce

	// route / router
	Cases    map[string]*Node // KRoute / KRouter participants
	Default  *Node            // KRoute
	Classify Selector         // KRoute (classify) / KRouter (select next)

	// loop / gate / dispatcher termination
	Until Predicate // KLoop / KGate / KRouter done
	Max   int       // KLoop / KRouter iteration cap

	// reduce
	Fold Invoke // KReduce

	// bind lenses
	Read  Lens
	Write Merge

	// human
	Apply ApplyFn

	// capabilities
	Retries int
}

// Children returns every structurally-nested child (for validation, analysis, and lowering).
func (n *Node) Children() []*Node {
	out := append([]*Node(nil), n.Steps...)
	for _, c := range []*Node{n.Body, n.Over, n.Default} {
		if c != nil {
			out = append(out, c)
		}
	}
	for _, c := range n.Cases {
		out = append(out, c)
	}
	return out
}

// Walk visits every node depth-first, parents before children.
func Walk(n *Node, visit func(*Node)) {
	if n == nil {
		return
	}
	visit(n)
	for _, c := range n.Children() {
		Walk(c, visit)
	}
}
