package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/app/server"
	"github.com/bjaus/flow/engine"
	"github.com/bjaus/flow/internal/ir"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/google/uuid"
)

type AnyWorkflow interface {
	Definition() *ir.Node
	Validate() []string
	AgentNames() []string
}

type Config struct {
	Store         *Stores
	Checkpoint    CheckpointStore
	RunStore      RunStore
	Events        EventSink
	Provider      Provider
	AgentRegistry AgentRegistry
	Tracer        Tracer
	Agents        string
	Skills        string
	Listen        string
	DrainTimeout  time.Duration
	DrainOnly     bool
}

type WorkflowInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputType   string `json:"input_type"`
	OutputType  string `json:"output_type"`
	Fingerprint string `json:"fingerprint"`
}

type registeredWorkflow struct {
	info       WorkflowInfo
	definition *ir.Node
	agents     []string
}

type App struct {
	cfg        Config
	checkpoint CheckpointStore
	runs       RunStore
	events     EventSink
	provider   Provider
	registry   AgentRegistry
	tracer     Tracer
	ownedStore *Stores

	mu        sync.RWMutex
	workflows map[string]*registeredWorkflow
	wake      chan struct{}
}

func New(cfg Config) (*App, error) {
	owned := false
	if cfg.Store == nil && (cfg.Checkpoint == nil || cfg.RunStore == nil || cfg.Events == nil) {
		s, err := SQLite("flow.db")
		if err != nil {
			return nil, fmt.Errorf("default store: %w", err)
		}
		cfg.Store, owned = s, true
	}
	if cfg.Store != nil {
		if cfg.Checkpoint == nil {
			cfg.Checkpoint = cfg.Store.Checkpoint
		}
		if cfg.RunStore == nil {
			cfg.RunStore = cfg.Store.Runs
		}
		if cfg.Events == nil {
			cfg.Events = cfg.Store.Events
		}
	}
	if cfg.Checkpoint == nil || cfg.RunStore == nil || cfg.Events == nil {
		return nil, errors.New("checkpoint, run, and event stores are required")
	}
	if cfg.Provider == nil {
		cfg.Provider = Gateway("")
	}
	if cfg.Tracer == nil {
		cfg.Tracer = NoopTracer()
	}
	if cfg.Listen == "" {
		cfg.Listen = ":7788"
	}
	if cfg.Agents == "" {
		cfg.Agents = "./agents"
	}
	if cfg.Skills == "" {
		cfg.Skills = "./skills"
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	a := &App{cfg: cfg, checkpoint: cfg.Checkpoint, runs: cfg.RunStore, events: cfg.Events, provider: cfg.Provider, registry: cfg.AgentRegistry, tracer: cfg.Tracer, workflows: make(map[string]*registeredWorkflow), wake: make(chan struct{}, 1)}
	if owned {
		a.ownedStore = cfg.Store
	}
	return a, nil
}

func workflowMetadata(wf AnyWorkflow) (string, string, error) {
	v := reflect.ValueOf(wf)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return "", "", errors.New("workflow must be a flow.Workflow value")
	}
	n, d := v.FieldByName("Name"), v.FieldByName("Desc")
	if !n.IsValid() || n.Kind() != reflect.String {
		return "", "", errors.New("workflow has no Name metadata")
	}
	name, desc := n.String(), ""
	if d.IsValid() && d.Kind() == reflect.String {
		desc = d.String()
	}
	return name, desc, nil
}

func (a *App) Register(wf AnyWorkflow) error {
	if wf == nil {
		return errors.New("register: nil workflow")
	}
	name, desc, err := workflowMetadata(wf)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if problems := wf.Validate(); len(problems) > 0 {
		return fmt.Errorf("register %q: %s", name, strings.Join(problems, "; "))
	}
	root := wf.Definition()
	if root == nil {
		return fmt.Errorf("register %q: nil definition", name)
	}
	info := WorkflowInfo{Name: name, Description: desc, InputType: root.In.String(), OutputType: root.Out.String(), Fingerprint: fingerprint(root)}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.workflows[name]; exists {
		return fmt.Errorf("register: duplicate workflow %q", name)
	}
	a.workflows[name] = &registeredWorkflow{info: info, definition: root, agents: wf.AgentNames()}
	return nil
}

func (a *App) Workflows() []WorkflowInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]WorkflowInfo, 0, len(a.workflows))
	for _, wf := range a.workflows {
		out = append(out, wf.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (a *App) RegisteredWorkflows() []server.WorkflowInfo {
	workflows := a.Workflows()
	out := make([]server.WorkflowInfo, len(workflows))
	for i, wf := range workflows {
		out[i] = server.WorkflowInfo{Name: wf.Name, Description: wf.Description, InputType: wf.InputType, OutputType: wf.OutputType, Fingerprint: wf.Fingerprint}
	}
	return out
}

func (a *App) ListRuns(ctx context.Context, filter RunFilter) ([]*Run, error) {
	return a.runs.List(ctx, filter)
}

func (a *App) GetRun(ctx context.Context, id string) (*Run, error) { return a.runs.Get(ctx, id) }
func (a *App) EventSink() EventSink                                { return a.events }

func fingerprint(root *ir.Node) string {
	h := sha256.New()
	var walk func(*ir.Node)
	walk = func(n *ir.Node) {
		if n == nil {
			io.WriteString(h, "nil;")
			return
		}
		fmt.Fprintf(h, "%d|%s|%s|%s|%d{", n.Kind, n.ID, typeName(n.In), typeName(n.Out), len(n.Steps))
		for _, child := range n.Steps {
			walk(child)
		}
		walk(n.Body)
		walk(n.Over)
		keys := make([]string, 0, len(n.Cases))
		for key := range n.Cases {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			io.WriteString(h, key)
			walk(n.Cases[key])
		}
		walk(n.Default)
		io.WriteString(h, "}")
	}
	walk(root)
	return hex.EncodeToString(h.Sum(nil))
}

func typeName(t reflect.Type) string {
	if t == nil {
		return ""
	}
	return t.PkgPath() + ":" + t.String()
}

func (a *App) Trigger(ctx context.Context, workflow string, input json.RawMessage) (string, error) {
	a.mu.RLock()
	wf := a.workflows[workflow]
	a.mu.RUnlock()
	if wf == nil {
		return "", fmt.Errorf("workflow %q not found", workflow)
	}
	if len(input) == 0 {
		input = json.RawMessage("null")
	}
	if _, err := decodeInput(input, wf.definition.In); err != nil {
		return "", fmt.Errorf("decode input: %w", err)
	}
	now := time.Now().UTC()
	r := &Run{ID: uuid.NewString(), Workflow: workflow, Fingerprint: wf.info.Fingerprint, Status: StatusQueued, Input: append([]byte(nil), input...), CreatedAt: now, UpdatedAt: now}
	if err := a.runs.Save(ctx, r); err != nil {
		return "", err
	}
	a.signal()
	return r.ID, nil
}

func decodeInput(data []byte, typ reflect.Type) (any, error) {
	v := reflect.New(typ)
	if err := json.Unmarshal(data, v.Interface()); err != nil {
		return nil, err
	}
	return v.Elem().Interface(), nil
}

func (a *App) Decide(ctx context.Context, id string, d Decision) error {
	r, err := a.runs.Get(ctx, id)
	if err != nil {
		return err
	}
	if r.Status != StatusAwaitingReview {
		return fmt.Errorf("run %q is %s, not awaiting_review", id, r.Status)
	}
	r.Decision = &d
	r.Status = StatusQueued
	if err := a.runs.Save(ctx, r); err != nil {
		return err
	}
	a.events.Publish(Event{RunID: id, Kind: EventDecisionApplied, Data: mustJSON(d)})
	a.signal()
	return nil
}

func (a *App) Migrate(ctx context.Context, id, action string) error {
	r, err := a.runs.Get(ctx, id)
	if err != nil {
		return err
	}
	if r.Status != StatusNeedsMigration {
		return fmt.Errorf("run %q is %s, not needs_migration", id, r.Status)
	}
	switch action {
	case "restart":
		a.mu.RLock()
		wf := a.workflows[r.Workflow]
		a.mu.RUnlock()
		if wf == nil {
			return fmt.Errorf("workflow %q not registered", r.Workflow)
		}
		if err := a.checkpoint.Delete(ctx, r.ID); err != nil {
			return err
		}
		r.Fingerprint, r.Status, r.Result, r.Error = wf.info.Fingerprint, StatusQueued, nil, ""
		r.InterruptID, r.GatePrompt, r.StartedAt, r.FinishedAt, r.Decision = "", "", nil, nil, nil
	case "abandon":
		now := time.Now().UTC()
		r.Status, r.FinishedAt, r.Error = StatusCanceled, &now, "abandoned during migration"
	case "finish_on_previous":
		r.Status = StatusParked
	default:
		return fmt.Errorf("invalid migration action %q", action)
	}
	if err := a.runs.Save(ctx, r); err != nil {
		return err
	}
	if action == "restart" {
		a.signal()
	}
	return nil
}

func (a *App) Cancel(ctx context.Context, id string) error {
	r, err := a.runs.Get(ctx, id)
	if err != nil {
		return err
	}
	if r.Status.Terminal() {
		return fmt.Errorf("run %q is already terminal", id)
	}
	if r.Status == StatusQueued || r.Status == StatusAwaitingReview || r.Status == StatusParked {
		now := time.Now().UTC()
		r.Status = StatusCanceled
		r.FinishedAt = &now
	} else {
		r.CancelPending = true
	}
	return a.runs.Save(ctx, r)
}

func (a *App) signal() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *App) Serve(ctx context.Context) error {
	defer func() {
		if a.ownedStore != nil {
			_ = a.ownedStore.Close()
		}
	}()
	if err := a.recoverRuns(ctx); err != nil {
		return err
	}
	httpServer := &http.Server{Addr: a.cfg.Listen, Handler: server.New(a), ReadHeaderTimeout: 10 * time.Second}
	httpErr := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpErr <- err
	}()
	workerErr := make(chan error, 1)
	go func() { workerErr <- a.work(ctx) }()
	select {
	case err := <-httpErr:
		return err
	case err := <-workerErr:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.DrainTimeout)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		return a.drain()
	}
}

func (a *App) work(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-a.wake:
		}
		for {
			r, err := a.runs.Claim(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if r == nil {
				break
			}
			if err := a.execute(ctx, r); err != nil && ctx.Err() == nil {
				return err
			}
		}
	}
}

func (a *App) recoverRuns(ctx context.Context) error {
	for _, status := range []Status{StatusRunning, StatusParked} {
		runs, err := a.runs.List(ctx, RunFilter{Status: status})
		if err != nil {
			return err
		}
		for _, r := range runs {
			a.mu.RLock()
			wf := a.workflows[r.Workflow]
			a.mu.RUnlock()
			if wf == nil || wf.info.Fingerprint != r.Fingerprint {
				r.Status = StatusNeedsMigration
			} else {
				r.Status = StatusQueued
				a.events.Publish(Event{RunID: r.ID, Kind: EventRunResumed})
			}
			if err := a.runs.Save(ctx, r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *App) drain() error {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.DrainTimeout)
	defer cancel()
	_ = ctx
	return nil
}

func (a *App) engineRegistry(ctx context.Context) engine.Registry {
	return engine.RegistryFunc(func(name string) (model.BaseChatModel, string, error) {
		p := Persona{Name: name, Model: name}
		if a.registry != nil {
			var ok bool
			p, ok = a.registry.Persona(name)
			if !ok {
				return nil, "", fmt.Errorf("persona %q not found", name)
			}
		}
		m, err := a.provider.Model(ctx, p)
		return m, p.SystemInstruction, err
	})
}

func (a *App) execute(ctx context.Context, run *Run) error {
	a.mu.RLock()
	wf := a.workflows[run.Workflow]
	a.mu.RUnlock()
	if wf == nil || wf.info.Fingerprint != run.Fingerprint {
		run.Status = StatusNeedsMigration
		return a.runs.Save(context.Background(), run)
	}
	in, err := decodeInput(run.Input, wf.definition.In)
	if err != nil {
		return a.fail(run, err)
	}
	runnable, err := engine.CompileDefinition(ctx, wf.info.Name, wf.definition, a.engineRegistry(ctx), a.checkpoint)
	if err != nil {
		return a.fail(run, err)
	}
	runCtx, span := a.tracer.StartRun(ctx, run)
	a.events.Publish(Event{RunID: run.ID, Kind: EventRunStarted})
	cb := &eventCallbacks{runID: run.ID, events: a.events, tracer: a.tracer}
	opts := []compose.Option{compose.WithCheckPointID(run.ID), compose.WithCallbacks(cb)}
	if run.Decision != nil && run.InterruptID != "" {
		runCtx = compose.ResumeWithData(runCtx, run.InterruptID, flow.Decision{Approved: run.Decision.Approved, Feedback: run.Decision.Feedback})
		run.Decision = nil
	}
	sr, err := runnable.Stream(runCtx, in, opts...)
	var out any
	if err == nil {
		defer sr.Close()
		for {
			chunk, recvErr := sr.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				err = recvErr
				break
			}
			out = chunk
		}
	}
	span.End(err)
	if info, interrupted := compose.ExtractInterruptInfo(err); interrupted {
		for _, ic := range info.InterruptContexts {
			if ic != nil && ic.IsRootCause {
				run.InterruptID = ic.ID
				run.GatePrompt = fmt.Sprint(ic.Info)
				break
			}
		}
		if run.InterruptID == "" && len(info.InterruptContexts) > 0 {
			run.InterruptID = info.InterruptContexts[0].ID
			run.GatePrompt = fmt.Sprint(info.InterruptContexts[0].Info)
		}
		run.Status = StatusAwaitingReview
		if saveErr := a.runs.Save(context.Background(), run); saveErr != nil {
			return saveErr
		}
		a.events.Publish(Event{RunID: run.ID, Kind: EventGateReached, Data: mustJSON(map[string]any{"prompt": run.GatePrompt})})
		return nil
	}
	if err != nil {
		return a.fail(run, err)
	}
	result, err := json.Marshal(out)
	if err != nil {
		return a.fail(run, err)
	}
	now := time.Now().UTC()
	run.Result = result
	run.Status = StatusSucceeded
	run.FinishedAt = &now
	run.InterruptID = ""
	run.GatePrompt = ""
	if err := a.runs.Save(context.Background(), run); err != nil {
		return err
	}
	_ = a.checkpoint.Delete(context.Background(), run.ID)
	a.events.Publish(Event{RunID: run.ID, Kind: EventRunFinished, Data: mustJSON(map[string]any{"status": run.Status, "result": out})})
	return nil
}

func (a *App) fail(run *Run, cause error) error {
	now := time.Now().UTC()
	run.Status = StatusFailed
	run.Error = cause.Error()
	run.FinishedAt = &now
	if err := a.runs.Save(context.Background(), run); err != nil {
		return err
	}
	a.events.Publish(Event{RunID: run.ID, Kind: EventRunFinished, Data: mustJSON(map[string]any{"status": run.Status, "error": run.Error})})
	return nil
}

func mustJSON(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

type eventCallbacks struct {
	runID  string
	events EventSink
	tracer Tracer
}

func (c *eventCallbacks) OnStart(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackInput) context.Context {
	if info == nil || info.Component == "Graph" {
		return ctx
	}
	c.events.Publish(Event{RunID: c.runID, Kind: EventStepStarted, Data: mustJSON(map[string]string{"label": info.Name, "kind": string(info.Component)})})
	return ctx
}
func (c *eventCallbacks) OnEnd(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackOutput) context.Context {
	if info != nil && info.Component != "Graph" {
		c.events.Publish(Event{RunID: c.runID, Kind: EventStepFinished, Data: mustJSON(map[string]string{"label": info.Name, "kind": string(info.Component)})})
	}
	return ctx
}
func (c *eventCallbacks) OnError(ctx context.Context, _ *callbacks.RunInfo, _ error) context.Context {
	return ctx
}
func (c *eventCallbacks) OnStartWithStreamInput(ctx context.Context, _ *callbacks.RunInfo, in *schema.StreamReader[callbacks.CallbackInput]) context.Context {
	in.Close()
	return ctx
}
func (c *eventCallbacks) OnEndWithStreamOutput(ctx context.Context, info *callbacks.RunInfo, out *schema.StreamReader[callbacks.CallbackOutput]) context.Context {
	if info == nil || info.Component != "ChatModel" {
		out.Close()
		return ctx
	}
	go func() {
		defer out.Close()
		for {
			chunk, err := out.Recv()
			if err != nil {
				return
			}
			if mo := model.ConvCallbackOutput(chunk); mo != nil && mo.Message != nil {
				c.events.Publish(Event{RunID: c.runID, Kind: EventAgentToken, Data: mustJSON(map[string]string{"label": info.Name, "delta": mo.Message.Content})})
			}
		}
	}()
	return ctx
}
