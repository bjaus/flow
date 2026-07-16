package engine_test

import (
	"context"
	"testing"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"
)

// StateDo gives a step read/write access to the workflow's shared state, so a later step sees what an earlier
// one stashed — coordination the typed edges don't carry.
func TestSharedState(t *testing.T) {
	write := flow.StateDo("write", func(_ context.Context, s string, _ func() any, set func(any)) (string, error) {
		set("stashed:" + s)
		return s, nil
	})
	read := flow.StateDo("read", func(_ context.Context, s string, get func() any, _ func(any)) (string, error) {
		v, _ := get().(string)
		return v, nil
	})
	wf := flow.Define("state", "", flow.Then(write, read))
	app, err := engine.Compile(context.Background(), wf, registry(nil), nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := app.Invoke(context.Background(), "x")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out != "stashed:x" {
		t.Fatalf("shared state not visible across steps: %q", out)
	}
}
