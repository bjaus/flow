package flow_test

import (
	"context"
	"fmt"

	"github.com/bjaus/flow"
)

func Example() {
	workflow := flow.Define("double", "Double an integer", flow.Do("double", func(_ context.Context, n int) (int, error) { return n * 2, nil }))
	fmt.Println(workflow.Name, workflow.Desc)
	// Output: double Double an integer
}
func ExampleAgent() {
	type Plan struct{ Steps []string }
	step := flow.Agent[string, Plan]("planner", func(goal string) string { return "Plan: " + goal })
	workflow := flow.Define("plan", "Create a plan", step)
	fmt.Println(workflow.AgentNames())
	// Output: [planner]
}
func ExampleThen() {
	parse := flow.Do("length", func(_ context.Context, text string) (int, error) { return len(text), nil })
	classify := flow.Do("classify", func(_ context.Context, n int) (string, error) {
		if n > 4 {
			return "long", nil
		}
		return "short", nil
	})
	workflow := flow.Define("classify", "Classify text length", flow.Then(parse, classify))
	fmt.Println(workflow.Name)
	// Output: classify
}
func ExampleLoop() {
	increment := flow.Do("increment", func(_ context.Context, n int) (int, error) { return n + 1, nil })
	workflow := flow.Define("count", "Count to three", flow.Loop("until-three", increment, flow.StateGate(func(n int) bool { return n >= 3 }), 10))
	fmt.Println(workflow.Name)
	// Output: count
}
