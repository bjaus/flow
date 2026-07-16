package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// A compiled workflow exposes all four eino run modes, not just Invoke/Stream: here it consumes a STREAMING
// input via Collect (stream in → value out).
func TestStreamingInputCollect(t *testing.T) {
	up := flow.Do("up", func(_ context.Context, s string) (string, error) { return strings.ToUpper(s), nil })
	wf := flow.Define("collect", "", up)
	app, err := engine.Compile(context.Background(), wf, registry(nil), nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	sr, sw := schema.Pipe[string](1)
	sw.Send("hello", nil)
	sw.Close()
	out, err := app.Collect(context.Background(), sr)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if out != "HELLO" {
		t.Fatalf("collect result wrong: %q", out)
	}
}

// A caller may pass through any eino compile option flow does not set itself. Here interrupt-before-node can
// only match because the node key is the author's Step.ID ("target"), not an internal generated key.
func TestCompileOptionInterruptBeforeNamedNode(t *testing.T) {
	first := flow.Do("first", func(_ context.Context, s string) (string, error) { return s + "1", nil })
	target := flow.Do("target", func(_ context.Context, s string) (string, error) { return s + "2", nil }).ID("target")
	wf := flow.Define("io", "", flow.Then(first, target))

	app, err := engine.Compile(context.Background(), wf, registry(nil), &memStore{m: map[string][]byte{}},
		compose.WithInterruptBeforeNodes([]string{"target"}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = app.Invoke(context.Background(), "x", compose.WithCheckPointID("io-1"))
	if _, paused := compose.ExtractInterruptInfo(err); !paused {
		t.Fatalf("expected an interrupt before the node named %q (proves the compile-option seam and the stable node key), got: %v", "target", err)
	}
}
