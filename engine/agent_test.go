package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/schema"
)

// toolModel is a fake ToolCallingChatModel: it asks for the "shout" tool once, then, once it sees the tool's
// result, returns a final answer — exactly the ReAct shape a real model drives.
type toolModel struct{}

func (toolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, m := range msgs {
		if m.Role == schema.Tool {
			return schema.AssistantMessage("final:"+m.Content, nil), nil
		}
	}
	return &schema.Message{
		Role:      schema.Assistant,
		ToolCalls: []schema.ToolCall{{ID: "c1", Function: schema.FunctionCall{Name: "shout", Arguments: `{"text":"hi"}`}}},
	}, nil
}

func (m toolModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, sw := schema.Pipe[*schema.Message](1)
	sw.Send(out, nil)
	sw.Close()
	return sr, nil
}

func (m toolModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) { return m, nil }

// A persona with tools lowers to a native ReAct loop (ChatModel ⇄ ToolsNode), so an Agent actually calls its
// tools and iterates — not a single completion.
func TestAgentUsesTools(t *testing.T) {
	shout, err := utils.InferTool("shout", "uppercase text", func(_ context.Context, in struct {
		Text string `json:"text"`
	}) (string, error) {
		return strings.ToUpper(in.Text), nil
	})
	if err != nil {
		t.Fatalf("tool: %v", err)
	}
	reg := engine.RegistryFunc(func(name string) (engine.Persona, error) {
		return engine.Persona{Model: toolModel{}, Tools: []tool.BaseTool{shout}}, nil
	})
	wf := flow.Define("toolagent", "", flow.Agent[string, string]("worker", func(s string) string { return s }))
	app, err := engine.Compile(context.Background(), wf, reg, nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := app.Invoke(context.Background(), "go")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	// The tool ran (uppercased "hi" -> "HI") and the model answered with it: proof of a real tool loop.
	if !strings.Contains(out, "HI") || !strings.HasPrefix(out, "final:") {
		t.Fatalf("agent did not run the tool loop; got %q", out)
	}
}
