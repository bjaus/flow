package engine_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// ---- a fake streaming model + registry (zero real tokens) ----

type fakeModel struct{ reply string }

func (m fakeModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.reply, nil), nil
}
func (m fakeModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, sw := schema.Pipe[*schema.Message](32)
	go func() {
		defer sw.Close()
		for _, w := range strings.Fields(m.reply) {
			sw.Send(schema.AssistantMessage(w+" ", nil), nil)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return sr, nil
}

func registry(replies map[string]string) engine.Registry {
	return engine.RegistryFunc(func(name string) (engine.Persona, error) {
		return engine.Persona{Model: fakeModel{reply: replies[name]}, System: "you are the " + name}, nil
	})
}

// tokenSink collects each agent's streamed tokens (this is the TUI feed).
type tokenSink struct {
	mu  sync.Mutex
	got map[string]*strings.Builder
	wg  sync.WaitGroup
}

func newSink() *tokenSink { return &tokenSink{got: map[string]*strings.Builder{}} }

func (s *tokenSink) OnStart(ctx context.Context, _ *callbacks.RunInfo, _ callbacks.CallbackInput) context.Context {
	return ctx
}
func (s *tokenSink) OnEnd(ctx context.Context, _ *callbacks.RunInfo, _ callbacks.CallbackOutput) context.Context {
	return ctx
}
func (s *tokenSink) OnError(ctx context.Context, _ *callbacks.RunInfo, _ error) context.Context {
	return ctx
}
func (s *tokenSink) OnStartWithStreamInput(ctx context.Context, _ *callbacks.RunInfo, in *schema.StreamReader[callbacks.CallbackInput]) context.Context {
	in.Close()
	return ctx
}
func (s *tokenSink) OnEndWithStreamOutput(ctx context.Context, info *callbacks.RunInfo, out *schema.StreamReader[callbacks.CallbackOutput]) context.Context {
	if info.Component != "ChatModel" {
		out.Close()
		return ctx
	}
	name := info.Name
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer out.Close()
		for {
			chunk, err := out.Recv()
			if err == io.EOF || err != nil {
				return
			}
			if mo := model.ConvCallbackOutput(chunk); mo != nil && mo.Message != nil {
				s.mu.Lock()
				if s.got[name] == nil {
					s.got[name] = &strings.Builder{}
				}
				s.got[name].WriteString(mo.Message.Content)
				s.mu.Unlock()
			}
		}
	}()
	return ctx
}
func (s *tokenSink) text(agent string) string {
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.got[agent]; b != nil {
		return b.String()
	}
	return ""
}

type memStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (s *memStore) Get(_ context.Context, id string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[id]
	return b, ok, nil
}
func (s *memStore) Set(_ context.Context, id string, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id] = b
	return nil
}
func (s *memStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id)
	return nil
}

// ---- the test: native streaming + durable human gate on one real graph ----

func TestNativeStreamingAndDurableHuman(t *testing.T) {
	// plan (streams) -> human approve -> finalize
	plan := flow.Agent[string, string]("planner", func(ticket string) string { return "plan for " + ticket })
	approve := flow.Human("approve",
		func(planText string, dec flow.Decision) string {
			if dec.Approved {
				return "APPROVED: " + planText
			}
			return "REVISE: " + planText
		},
		func(planText string) string { return "approve this plan?" })
	finalize := flow.Do("finalize", func(_ context.Context, s string) (string, error) { return s + " (shipped)", nil })

	wf := flow.Define("ship", "plan, approve, ship", flow.Then(flow.Then(plan, approve), finalize))

	prov := registry(map[string]string{"planner": "add retry logic with backoff"})
	app, err := engine.Compile(context.Background(), wf, prov, &memStore{m: map[string][]byte{}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := context.Background()
	const cp = "run-1"
	sink := newSink()

	// Stream: the planner streams its tokens to the sink, then the run pauses at the human gate.
	sr, err := app.Stream(ctx, "JH-56", compose.WithCheckPointID(cp), compose.WithCallbacks(sink))
	if err == nil {
		// drain until the interrupt surfaces from the stream
		for {
			if _, e := sr.Recv(); e != nil {
				err = e
				break
			}
		}
	}
	if _, paused := compose.ExtractInterruptInfo(err); !paused {
		t.Fatalf("expected the run to pause at the human gate, got: %v", err)
	}

	// PROVE streaming: the planner's tokens arrived at the sink, token-by-token.
	streamed := sink.text("planner")
	if !strings.Contains(streamed, "add retry logic with backoff") {
		t.Fatalf("planner did not stream to the sink; got %q", streamed)
	}

	// PROVE durability: resume with approval; the run continues natively to completion.
	rctx := compose.ResumeWithData(ctx, interruptID(t, err), flow.Decision{Approved: true})
	out, err := app.Invoke(rctx, "JH-56", compose.WithCheckPointID(cp))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.HasPrefix(out, "APPROVED:") || !strings.HasSuffix(out, "(shipped)") {
		t.Fatalf("resumed result wrong: %q", out)
	}
}

func interruptID(t *testing.T, err error) string {
	t.Helper()
	info, ok := compose.ExtractInterruptInfo(err)
	if !ok {
		t.Fatalf("no interrupt in %v", err)
	}
	for _, ic := range info.InterruptContexts {
		if ic != nil {
			return ic.ID
		}
	}
	t.Fatal("interrupt carried no id")
	return ""
}
