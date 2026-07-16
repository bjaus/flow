package provider

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/bjaus/flow/app/internal/core"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

// Script maps persona name to input prompt to one or more responses. Responses are consumed in order;
// the final response is reused. The "*" input is a fallback.
type Script map[string]map[string][]string

type Fake struct {
	mu     sync.Mutex
	script Script
	calls  []Call
}

type Call struct {
	Persona string
	Input   string
}

func NewFake(script Script) *Fake { return &Fake{script: script} }

func (f *Fake) Model(_ context.Context, p core.Persona) (model.BaseChatModel, error) {
	if _, ok := f.script[p.Name]; !ok {
		return nil, fmt.Errorf("fake provider: no script for persona %q", p.Name)
	}
	return &fakeModel{owner: f, persona: p.Name}, nil
}

func (f *Fake) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Call(nil), f.calls...)
}

func (f *Fake) response(persona, input string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, Call{Persona: persona, Input: input})
	byInput := f.script[persona]
	answers, ok := byInput[input]
	if !ok {
		answers, ok = byInput["*"]
	}
	if !ok || len(answers) == 0 {
		return "", fmt.Errorf("fake provider: no response for %q input %q", persona, input)
	}
	answer := answers[0]
	if len(answers) > 1 {
		byInput[input] = answers[1:]
	}
	return answer, nil
}

type fakeModel struct {
	owner   *Fake
	persona string
}

// WithTools accepts any tool set and keeps replying from the script, so tool-bearing personas run
// deterministically with zero tokens. The scripted replies never emit tool calls.
func (m *fakeModel) WithTools(_ []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return m, nil
}

func prompt(messages []*schema.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == schema.User {
			return messages[i].Content
		}
	}
	return ""
}

func (m *fakeModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	answer, err := m.owner.response(m.persona, prompt(messages))
	if err != nil {
		return nil, err
	}
	return schema.AssistantMessage(answer, nil), nil
}

func (m *fakeModel) Stream(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	answer, err := m.owner.response(m.persona, prompt(messages))
	if err != nil {
		return nil, err
	}
	r, w := schema.Pipe[*schema.Message](16)
	go func() {
		defer w.Close()
		parts := strings.SplitAfter(answer, " ")
		for _, part := range parts {
			if part != "" {
				w.Send(schema.AssistantMessage(part, nil), nil)
			}
		}
		if len(parts) == 0 {
			w.Send(nil, io.EOF)
		}
	}()
	return r, nil
}
