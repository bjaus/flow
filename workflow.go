package flow

import (
	"fmt"

	"github.com/bjaus/flow/internal/ir"
)

// Workflow is a named, typed step tree ready for a backend to compile.
type Workflow[In, Out any] struct {
	Name string
	Desc string
	root *ir.Node
}

// Define wraps a typed root step into a Workflow. Description is required — it's the one-liner a launcher or
// registry shows.
func Define[In, Out any](name, desc string, root Step[In, Out]) Workflow[In, Out] {
	return Workflow[In, Out]{Name: name, Desc: desc, root: root.n}
}

// Definition returns the erased root node — the seam a backend compiles. DSL users don't need this.
func (w Workflow[In, Out]) Definition() *ir.Node { return w.root }

// AgentNames returns the distinct personas the workflow references (for load-time preflight).
func (w Workflow[In, Out]) AgentNames() []string {
	seen := map[string]bool{}
	var names []string
	ir.Walk(w.root, func(n *ir.Node) {
		if n.Kind == ir.KAgent && !seen[n.Persona] {
			seen[n.Persona] = true
			names = append(names, n.Persona)
		}
	})
	return names
}

// Validate returns structural problems checkable from the tree alone (empty => ok): duplicate ids and empty
// required slots. Reference checks (personas exist) need a registry and run at load time.
func (w Workflow[In, Out]) Validate() []string {
	if w.Name == "" {
		return []string{"workflow needs a name"}
	}
	var problems []string
	seen := map[string]bool{}
	ir.Walk(w.root, func(n *ir.Node) {
		if n.ID != "" {
			if seen[n.ID] {
				problems = append(problems, fmt.Sprintf("duplicate step id %q", n.ID))
			}
			seen[n.ID] = true
		}
		switch n.Kind {
		case ir.KThen:
			if len(n.Steps) != 2 {
				problems = append(problems, "then needs exactly two steps")
			}
		case ir.KSeq, ir.KParallel:
			if len(n.Steps) == 0 {
				problems = append(problems, fmt.Sprintf("%q has no steps", n.Name))
			}
		case ir.KAgent:
			if n.Persona == "" {
				problems = append(problems, "agent has no persona")
			}
		case ir.KLoop:
			if n.Body == nil || n.Until == nil {
				problems = append(problems, "loop needs a body and a gate")
			}
		case ir.KRoute, ir.KRouter:
			if len(n.Cases) == 0 {
				problems = append(problems, fmt.Sprintf("%q has no cases", n.Name))
			}
		}
	})
	return problems
}
