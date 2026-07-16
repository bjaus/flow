package app

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/engine"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/schema"
	"github.com/stretchr/testify/require"
)

type reviewState struct {
	Text     string `json:"text"`
	Approved bool   `json:"approved"`
	Feedback string `json:"feedback"`
}

func testApp(t *testing.T, provider Provider) (*App, *Stores) {
	t.Helper()
	stores, err := SQLite(filepath.Join(t.TempDir(), "flow.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.Close()) })
	a, err := New(Config{Store: stores, Provider: provider, Listen: "127.0.0.1:0", DrainTimeout: 2 * time.Second})
	require.NoError(t, err)
	return a, stores
}
func serve(t *testing.T, a *App) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("serve did not stop")
		}
	})
	return cancel
}
func waitRun(t *testing.T, s *Stores, id string, want Status) *Run {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		r, err := s.Runs.Get(context.Background(), id)
		if err == nil && r.Status == want {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	r, err := s.Runs.Get(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, want, r.Status)
	return r
}

func TestLinearWorkflowCompletesAndPersistsTypedResult(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	wf := flow.Define("double", "doubles then formats", flow.Then(flow.Do("double", func(_ context.Context, in int) (int, error) { return in * 2, nil }), flow.Do("format", func(_ context.Context, in int) (map[string]int, error) { return map[string]int{"value": in}, nil })))
	require.NoError(t, a.Register(wf))
	require.Error(t, a.Register(wf))
	_, err := a.Trigger(context.Background(), "missing", json.RawMessage(`1`))
	require.Error(t, err)
	_, err = a.Trigger(context.Background(), "double", json.RawMessage(`"bad"`))
	require.Error(t, err)
	serve(t, a)
	id, err := a.Trigger(context.Background(), "double", json.RawMessage(`21`))
	require.NoError(t, err)
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `{"value":42}`, string(run.Result))
	require.NotNil(t, run.StartedAt)
	require.NotNil(t, run.FinishedAt)
}
func TestAgentStreamsTokensAndUsesFakeProvider(t *testing.T) {
	p := FakeProvider(FakeScript{"writer": {"say hi": {"{\"text\":\"hello world\"}"}}})
	a, s := testApp(t, p)
	type answer struct {
		Text string `json:"text"`
	}
	require.NoError(t, a.Register(flow.Define("write", "write", flow.Agent[string, answer]("writer", func(in string) string { return in }))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "write", json.RawMessage(`"say hi"`))
	require.NoError(t, err)
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `{"text":"hello world"}`, string(run.Result))
	events, cancel := s.Events.Subscribe(id)
	defer cancel()
	var kinds []EventKind
	for len(events) > 0 {
		kinds = append(kinds, (<-events).Kind)
	}
	require.Contains(t, kinds, EventAgentToken)
	require.Equal(t, EventRunStarted, kinds[0])
	require.Equal(t, EventRunFinished, kinds[len(kinds)-1])
	require.Len(t, p.Calls(), 1)
}

type staticRegistry map[string]Persona

func (r staticRegistry) Persona(name string) (Persona, bool) { p, ok := r[name]; return p, ok }

type toolProvider struct{}

type recordingFallbackProvider struct {
	mu     sync.Mutex
	models []string
}

func (p *recordingFallbackProvider) Model(_ context.Context, persona Persona) (model.BaseChatModel, error) {
	p.mu.Lock()
	p.models = append(p.models, persona.Model)
	p.mu.Unlock()
	return namedResultModel{name: persona.Model}, nil
}

type namedResultModel struct{ name string }

func (m namedResultModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	if m.name == "primary" {
		return nil, errors.New("primary unavailable")
	}
	return schema.AssistantMessage(`{"text":"fallback"}`, nil), nil
}
func (m namedResultModel) Stream(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}
	r, w := schema.Pipe[*schema.Message](1)
	w.Send(out, nil)
	w.Close()
	return r, nil
}

func TestRuntimeUsesProfileFallbackModels(t *testing.T) {
	stores, err := SQLite(filepath.Join(t.TempDir(), "fallback.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.Close()) })
	provider := &recordingFallbackProvider{}
	registry := staticRegistry{"worker": {Name: "worker", Model: "primary", FallbackModels: []string{"secondary"}}}
	a, err := New(Config{Store: stores, Provider: provider, AgentRegistry: registry, Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	type answer struct {
		Text string `json:"text"`
	}
	require.NoError(t, a.Register(flow.Define("fallback", "fallback", flow.Agent[string, answer]("worker", func(in string) string { return in }))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "fallback", json.RawMessage(`"go"`))
	require.NoError(t, err)
	require.JSONEq(t, `{"text":"fallback"}`, string(waitRun(t, stores, id, StatusSucceeded).Result))
	require.Equal(t, []string{"primary", "secondary"}, provider.models)
}

func (toolProvider) Model(context.Context, Persona) (model.BaseChatModel, error) {
	return appToolModel{}, nil
}

type appToolModel struct{}

func (appToolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, msg := range msgs {
		if msg.Role == schema.Tool {
			return schema.AssistantMessage("final:"+msg.Content, nil), nil
		}
	}
	return &schema.Message{Role: schema.Assistant, ToolCalls: []schema.ToolCall{{ID: "1", Function: schema.FunctionCall{Name: "shout", Arguments: `{"text":"hello"}`}}}}, nil
}
func (m appToolModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	r, w := schema.Pipe[*schema.Message](1)
	w.Send(out, nil)
	w.Close()
	return r, nil
}
func (m appToolModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return m, nil
}
func TestRuntimeResolvesPersonaToolNamesIntoReactAgent(t *testing.T) {
	stores, err := SQLite(filepath.Join(t.TempDir(), "tools.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.Close()) })
	shout, err := utils.InferTool("shout", "uppercase", func(_ context.Context, in struct {
		Text string `json:"text"`
	}) (string, error) {
		return strings.ToUpper(in.Text), nil
	})
	require.NoError(t, err)
	a, err := New(Config{Store: stores, Provider: toolProvider{}, AgentRegistry: staticRegistry{"worker": {Name: "worker", Model: "fake", Tools: []string{"shout"}}}, Tools: map[string]tool.BaseTool{"shout": shout}, Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, a.Register(flow.Define("tool", "tool", flow.Agent[string, string]("worker", func(in string) string { return in }))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "tool", json.RawMessage(`"go"`))
	require.NoError(t, err)
	require.Contains(t, string(waitRun(t, stores, id, StatusSucceeded).Result), "HELLO")
}

func TestMalformedStructuredAgentOutputRetries(t *testing.T) {
	p := FakeProvider(FakeScript{"writer": {"*": {"not-json", `{"text":"valid"}`}}})
	a, stores := testApp(t, p)
	type answer struct {
		Text string `json:"text"`
	}
	require.NoError(t, a.Register(flow.Define("retry", "retry malformed output", flow.Agent[string, answer]("writer", func(in string) string { return in }))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "retry", json.RawMessage(`"task"`))
	require.NoError(t, err)
	run := waitRun(t, stores, id, StatusSucceeded)
	require.JSONEq(t, `{"text":"valid"}`, string(run.Result))
	require.Len(t, p.Calls(), 2)
}

func TestHumanInterruptDecisionAndDurableResume(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	wf := flow.Define("review", "durable review", flow.Then(flow.Do("draft", func(_ context.Context, in string) (reviewState, error) { return reviewState{Text: in}, nil }), flow.Human("approve", func(v reviewState, d flow.Decision) reviewState {
		v.Approved = d.Approved
		v.Feedback = d.Feedback
		return v
	}, func(v reviewState) string { return "Review " + v.Text })))
	require.NoError(t, a.Register(wf))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "review", json.RawMessage(`"draft"`))
	require.NoError(t, err)
	paused := waitRun(t, s, id, StatusAwaitingReview)
	require.NotEmpty(t, paused.InterruptID)
	require.Contains(t, paused.GatePrompt, "Review draft")
	require.Error(t, a.Decide(context.Background(), "missing", Decision{}))
	require.NoError(t, a.Decide(context.Background(), id, Decision{Approved: true, Feedback: "ship"}))
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `{"text":"draft","approved":true,"feedback":"ship"}`, string(run.Result))
}

// waitGate waits until the run pauses at a gate whose prompt has not been decided yet.
func waitGate(t *testing.T, s *Stores, id string, seen map[string]bool) *Run {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		r, err := s.Runs.Get(context.Background(), id)
		require.NoError(t, err)
		if r.Status == StatusFailed {
			t.Fatalf("run failed while waiting for a gate: %s", r.Error)
		}
		if r.Status == StatusAwaitingReview && !seen[r.GatePrompt] {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	r, err := s.Runs.Get(context.Background(), id)
	require.NoError(t, err)
	t.Fatalf("no fresh gate reached; run is %s (gate %q, err %q)", r.Status, r.GatePrompt, r.Error)
	return nil
}

type batchState struct {
	Items   []string `json:"items"`
	Results []string `json:"results"`
}

// Regression: two Human gates nested in a flow.Map inside a flow.Bind, driven one decision at a
// time through the daemon worker (which recompiles the graph on every claim), must both resume durably.
// The enclosing Route mirrors the failing daemon workflow: its cases live in a Go map, so lowering them in
// iteration order made node keys differ between compiles and checkpoint restore failed on resume.
func TestMapNestedHumanGatesResumeThroughWorker(t *testing.T) {
	engine.Register[batchState]()
	pass := func(name string) flow.Step[batchState, batchState] {
		return flow.Do(name, func(_ context.Context, s batchState) (batchState, error) { return s, nil })
	}
	gate := flow.Human("approve-item",
		func(v string, d flow.Decision) string { return v + ":" + d.Feedback },
		func(v string) string { return "approve " + v })
	review := flow.Bind(flow.Map(gate),
		func(s batchState) []string { return s.Items },
		func(s batchState, outs []string) batchState { s.Results = outs; return s })
	route := flow.Route(func(batchState) string { return "review" }, map[string]flow.Step[batchState, batchState]{
		"a": pass("case-a"), "b": pass("case-b"), "review": review, "d": pass("case-d"), "e": pass("case-e"),
	})
	wf := flow.Define("batch-review", "map-nested human gates", route)
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(wf))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "batch-review", json.RawMessage(`{"items":["a","b"]}`))
	require.NoError(t, err)

	feedback := map[string]string{"approve a": "alpha", "approve b": "beta"}
	seen := map[string]bool{}
	for range 2 {
		run := waitGate(t, s, id, seen)
		fb, ok := feedback[run.GatePrompt]
		require.True(t, ok, "unexpected gate prompt %q", run.GatePrompt)
		seen[run.GatePrompt] = true
		require.NoError(t, a.Decide(context.Background(), id, Decision{Approved: true, Feedback: fb}))
	}
	run := waitRun(t, s, id, StatusSucceeded)
	require.JSONEq(t, `{"items":["a","b"],"results":["a:alpha","b:beta"]}`, string(run.Result))
}

func TestProcessBoundaryRecoveryAndFingerprintMismatch(t *testing.T) {
	stores, err := SQLite(filepath.Join(t.TempDir(), "flow.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.Close()) })
	wf1 := flow.Define("recover", "v1", flow.Do("one", func(_ context.Context, in int) (int, error) { return in + 1, nil }))
	a1, err := New(Config{Store: stores, Provider: FakeProvider(nil), Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, a1.Register(wf1))
	id, err := a1.Trigger(context.Background(), "recover", json.RawMessage(`1`))
	require.NoError(t, err)
	r, err := stores.Runs.Get(context.Background(), id)
	require.NoError(t, err)
	r.Status = StatusParked
	require.NoError(t, stores.Runs.Save(context.Background(), r))
	a2, err := New(Config{Store: stores, Provider: FakeProvider(nil), Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, a2.Register(wf1))
	serve(t, a2)
	require.JSONEq(t, `2`, string(waitRun(t, stores, id, StatusSucceeded).Result))
	wf2 := flow.Define("recover", "v2", flow.Then(flow.Do("one", func(_ context.Context, in int) (int, error) { return in + 1, nil }), flow.Do("two", func(_ context.Context, in int) (int, error) { return in + 1, nil })))
	a3, err := New(Config{Store: stores, Provider: FakeProvider(nil), Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, a3.Register(wf2))
	id2, err := a3.Trigger(context.Background(), "recover", json.RawMessage(`1`))
	require.NoError(t, err)
	r2, err := stores.Runs.Get(context.Background(), id2)
	require.NoError(t, err)
	r2.Status = StatusParked
	r2.Fingerprint = r.Fingerprint
	require.NoError(t, stores.Runs.Save(context.Background(), r2))
	serve(t, a3)
	waitRun(t, stores, id2, StatusNeedsMigration)
}

type recordTracer struct {
	mu           sync.Mutex
	starts, ends []string
}
type recordSpan struct {
	owner *recordTracer
	name  string
}

func (t *recordTracer) add(dst *[]string, s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	*dst = append(*dst, s)
}
func (t *recordTracer) StartRun(ctx context.Context, r *Run) (context.Context, Span) {
	t.add(&t.starts, "run:"+r.Workflow)
	return ctx, recordSpan{t, "run"}
}
func (t *recordTracer) StartStep(ctx context.Context, _, label, kind string) (context.Context, Span) {
	t.add(&t.starts, "step:"+label+":"+kind)
	return ctx, recordSpan{t, "step"}
}
func (t *recordTracer) StartAgent(ctx context.Context, _, p string) (context.Context, Span) {
	t.add(&t.starts, "agent:"+p)
	return ctx, recordSpan{t, "agent"}
}
func (s recordSpan) Set(...Attr) {}
func (s recordSpan) End(error)   { s.owner.add(&s.owner.ends, s.name) }
func TestTracerReceivesRunAndStepSpans(t *testing.T) {
	tr := &recordTracer{}
	a, s := testApp(t, FakeProvider(nil))
	a.tracer = tr
	require.NoError(t, a.Register(flow.Define("trace", "trace", flow.Do("work", func(_ context.Context, in int) (int, error) { return in, nil }))))
	serve(t, a)
	id, err := a.Trigger(context.Background(), "trace", json.RawMessage(`1`))
	require.NoError(t, err)
	waitRun(t, s, id, StatusSucceeded)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	require.Contains(t, tr.starts, "run:trace")
	require.NotEmpty(t, tr.ends)
}
