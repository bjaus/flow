package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/app/agent"
	"github.com/bjaus/flow/app/server"
	"github.com/bjaus/flow/app/tools"
	"github.com/bjaus/flow/app/tui"
	"github.com/bjaus/flow/app/web"
	"github.com/bjaus/flow/engine"
	"github.com/bjaus/flow/internal/ir"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
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
	Agents        []string
	Skills        []string
	ConfigFiles   []string
	Listen        string
	DrainTimeout  time.Duration
	DrainOnly     bool
	// Tools maps tool names to the executable tools personas may be granted.
	// When nil, New defaults it to tools.Default("."), the built-in bash and
	// file tools confined to the current working directory.
	Tools map[string]tool.BaseTool
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

type reloadableRegistry interface {
	AgentRegistry
	Reload() error
	Watch(context.Context) error
	Status() ConfigStatus
	SetOnChange(func(ConfigStatus))
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
	draining  atomic.Bool
}

// New constructs a runtime, applying local defaults for omitted ports.
func New(cfg Config) (*App, error) {
	agentPathsExplicit := len(cfg.Agents) > 0 || len(cfg.Skills) > 0 || len(cfg.ConfigFiles) > 0
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
	if cfg.Tools == nil {
		cfg.Tools = tools.Default(".")
	}
	if cfg.Listen == "" {
		cfg.Listen = ":7788"
	}
	if cfg.AgentRegistry == nil {
		var loader interface {
			AgentRegistry
			Empty() bool
		}
		var err error
		if len(cfg.Agents) > 0 || len(cfg.Skills) > 0 {
			loader, err = MarkdownRegistry(cfg.Agents, cfg.Skills)
		} else {
			loader, err = ConfiguredMarkdownRegistry(cfg.ConfigFiles...)
		}
		if err != nil {
			if owned {
				_ = cfg.Store.Close()
			}
			return nil, fmt.Errorf("agent registry: %w", err)
		}
		if agentPathsExplicit || !loader.Empty() {
			cfg.AgentRegistry = loader
		}
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	a := &App{cfg: cfg, checkpoint: cfg.Checkpoint, runs: cfg.RunStore, events: cfg.Events, provider: cfg.Provider, registry: cfg.AgentRegistry, tracer: cfg.Tracer, workflows: make(map[string]*registeredWorkflow), wake: make(chan struct{}, 1)}
	if registry, ok := a.registry.(reloadableRegistry); ok {
		registry.SetOnChange(func(status ConfigStatus) { a.events.Publish(Event{Kind: EventConfigChanged, Data: mustJSON(status)}) })
	}
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

// Serve constructs a default App, registers workflows, and blocks until ctx is canceled.
func Serve(ctx context.Context, workflows ...AnyWorkflow) error {
	a, err := New(Config{})
	if err != nil {
		return err
	}
	for _, wf := range workflows {
		if err := a.Register(wf); err != nil {
			return err
		}
	}
	return a.Serve(ctx)
}

// Register validates and registers a workflow by name.
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
	for _, personaName := range wf.AgentNames() {
		if a.registry != nil {
			if _, ok := a.registry.Persona(personaName); !ok {
				return fmt.Errorf("register %q: persona %q not found", name, personaName)
			}
		}
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

func (a *App) RegisteredWebWorkflows() []web.WorkflowInfo {
	workflows := a.Workflows()
	out := make([]web.WorkflowInfo, len(workflows))
	for i, wf := range workflows {
		out[i] = web.WorkflowInfo{Name: wf.Name, Description: wf.Description, InputType: wf.InputType, OutputType: wf.OutputType, Fingerprint: wf.Fingerprint}
	}
	return out
}

// Handler returns the daemon's JSON API and embedded web application.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/", server.New(a))
	webHandler, err := web.New(a)
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		})
	}
	mux.Handle("/", webHandler)
	return mux
}

// TUI runs the terminal client against endpoint.
func (a *App) TUI(ctx context.Context, endpoint string) error { return tui.Run(ctx, endpoint) }

func (a *App) ListRuns(ctx context.Context, filter RunFilter) ([]*Run, error) {
	return a.runs.List(ctx, filter)
}

func (a *App) GetRun(ctx context.Context, id string) (*Run, error) { return a.runs.Get(ctx, id) }
func (a *App) EventSink() EventSink                                { return a.events }
func (a *App) ConfigStatus() ConfigStatus {
	if registry, ok := a.registry.(reloadableRegistry); ok {
		return registry.Status()
	}
	return ConfigStatus{}
}
func (a *App) ReloadConfig(_ context.Context) error {
	registry, ok := a.registry.(reloadableRegistry)
	if !ok {
		return errors.New("configuration is not reloadable")
	}
	if err := registry.Reload(); err != nil {
		return err
	}
	a.events.Publish(Event{Kind: EventConfigReloaded, Data: mustJSON(registry.Status())})
	return nil
}

func fingerprint(root *ir.Node) string {
	h := sha256.New()
	var walk func(*ir.Node)
	walk = func(n *ir.Node) {
		if n == nil {
			_, _ = io.WriteString(h, "nil;")
			return
		}
		_, _ = fmt.Fprintf(h, "%d|%s|%s|%s|%d{", n.Kind, n.ID, typeName(n.In), typeName(n.Out), len(n.Steps))
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
			_, _ = io.WriteString(h, key)
			walk(n.Cases[key])
		}
		walk(n.Default)
		_, _ = io.WriteString(h, "}")
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
	if a.cfg.DrainOnly {
		return "", errors.New("daemon is in drain-only mode")
	}
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
	if registry, ok := a.registry.(reloadableRegistry); ok {
		go func() { _ = registry.Watch(ctx) }()
	}
	httpServer := &http.Server{Addr: a.cfg.Listen, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	listener, err := net.Listen("tcp", a.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", a.cfg.Listen, err)
	}
	httpErr := make(chan error, 1)
	go func() {
		err := httpServer.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpErr <- err
	}()
	claimCtx, stopClaiming := context.WithCancel(context.Background())
	defer stopClaiming()
	workerErr := make(chan error, 1)
	go func() { workerErr <- a.work(claimCtx) }()
	select {
	case err := <-httpErr:
		stopClaiming()
		select {
		case <-workerErr:
		case <-time.After(a.cfg.DrainTimeout):
		}
		return err
	case err := <-workerErr:
		_ = httpServer.Close()
		select {
		case <-httpErr:
		case <-time.After(a.cfg.DrainTimeout):
		}
		return err
	case <-ctx.Done():
		a.draining.Store(true)
		stopClaiming()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.DrainTimeout)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		select {
		case err := <-workerErr:
			return err
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
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
			if a.cfg.DrainOnly {
				a.mu.RLock()
				wf := a.workflows[r.Workflow]
				a.mu.RUnlock()
				if wf == nil || wf.info.Fingerprint != r.Fingerprint {
					r.Status = StatusQueued
					if err := a.runs.Save(context.Background(), r); err != nil {
						return err
					}
					break
				}
			}
			if err := a.execute(context.Background(), r); err != nil && ctx.Err() == nil {
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

func (a *App) engineRegistry(ctx context.Context) engine.Registry {
	return engine.RegistryFunc(func(name string) (engine.Persona, error) {
		p := Persona{Name: name, Model: name}
		if a.registry != nil {
			var ok bool
			p, ok = a.registry.Persona(name)
			if !ok {
				return engine.Persona{}, fmt.Errorf("persona %q not found", name)
			}
		}
		modelNames := append([]string{p.Model}, p.FallbackModels...)
		models := make([]model.BaseChatModel, 0, len(modelNames))
		for _, modelName := range modelNames {
			candidate := p
			candidate.Model = modelName
			m, err := a.provider.Model(ctx, candidate)
			if err != nil {
				return engine.Persona{}, err
			}
			models = append(models, m)
		}
		m := models[0]
		if len(models) > 1 {
			m = fallbackModel{models: models}
		}
		permissions := p.ToolPermissions
		if len(permissions) == 0 {
			permissions = append([]string(nil), p.Tools...)
		}
		tools := make([]tool.BaseTool, 0, len(p.Tools))
		for _, toolName := range p.Tools {
			executable, ok := a.cfg.Tools[toolName]
			if !ok {
				return engine.Persona{}, fmt.Errorf("persona %q: tool %q is not registered", name, toolName)
			}
			guarded, guardErr := agent.GuardTool(executable, permissions)
			if guardErr != nil {
				return engine.Persona{}, fmt.Errorf("persona %q: %w", name, guardErr)
			}
			tools = append(tools, guarded)
		}
		return engine.Persona{Model: m, System: p.SystemInstruction, Tools: tools}, nil
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
	runCtx, cancelRun := context.WithCancel(runCtx)
	defer cancelRun()
	a.events.Publish(Event{RunID: run.ID, Kind: EventRunStarted})
	cb := &eventCallbacks{runID: run.ID, events: a.events, tracer: a.tracer, runs: a.runs, cancel: cancelRun, draining: &a.draining}
	opts := []compose.Option{compose.WithCheckPointID(run.ID), compose.WithCallbacks(cb)}
	if run.Decision != nil && run.InterruptID != "" {
		runCtx = compose.ResumeWithData(runCtx, run.InterruptID, flow.Decision{Approved: run.Decision.Approved, Feedback: run.Decision.Feedback})
		run.Decision = nil
	}
	var out any
	for attempt := 0; attempt < 2; attempt++ {
		out = nil
		sr, streamErr := runnable.Stream(runCtx, in, opts...)
		err = streamErr
		if err == nil {
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
			sr.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "agent output did not match") {
			break
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
		if a.draining.Load() {
			run.Status = StatusParked
			if saveErr := a.runs.Save(context.Background(), run); saveErr != nil {
				return saveErr
			}
			a.events.Publish(Event{RunID: run.ID, Kind: EventRunParked})
			return nil
		}
		latest, getErr := a.runs.Get(context.Background(), run.ID)
		if getErr == nil && latest.CancelPending {
			now := time.Now().UTC()
			latest.Status, latest.CancelPending, latest.FinishedAt = StatusCanceled, false, &now
			if saveErr := a.runs.Save(context.Background(), latest); saveErr != nil {
				return saveErr
			}
			a.events.Publish(Event{RunID: run.ID, Kind: EventRunFinished, Data: mustJSON(map[string]any{"status": StatusCanceled})})
			return nil
		}
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
	runID    string
	events   EventSink
	tracer   Tracer
	runs     RunStore
	cancel   context.CancelFunc
	draining *atomic.Bool
}
type callbackSpanKey struct{}

func (c *eventCallbacks) OnStart(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackInput) context.Context {
	if info == nil || info.Component == "Graph" {
		return ctx
	}
	c.events.Publish(Event{RunID: c.runID, Kind: EventStepStarted, Data: mustJSON(map[string]string{"label": info.Name, "kind": string(info.Component)})})
	var sp Span
	if info.Component == "ChatModel" {
		ctx, sp = c.tracer.StartAgent(ctx, c.runID, info.Name)
	} else {
		ctx, sp = c.tracer.StartStep(ctx, c.runID, info.Name, string(info.Component))
	}
	return context.WithValue(ctx, callbackSpanKey{}, sp)
}
func (c *eventCallbacks) OnEnd(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackOutput) context.Context {
	if info != nil && info.Component != "Graph" {
		c.events.Publish(Event{RunID: c.runID, Kind: EventStepFinished, Data: mustJSON(map[string]string{"label": info.Name, "kind": string(info.Component)})})
	}
	if sp, ok := ctx.Value(callbackSpanKey{}).(Span); ok {
		sp.End(nil)
	}
	if c.draining != nil && c.draining.Load() {
		c.cancel()
	}
	if c.runs != nil {
		if run, err := c.runs.Get(context.Background(), c.runID); err == nil && run.CancelPending {
			c.cancel()
		}
	}
	return ctx
}
func (c *eventCallbacks) OnError(ctx context.Context, _ *callbacks.RunInfo, err error) context.Context {
	if sp, ok := ctx.Value(callbackSpanKey{}).(Span); ok {
		sp.End(err)
	}
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
	defer out.Close()
	for {
		chunk, err := out.Recv()
		if err != nil {
			break
		}
		if mo := model.ConvCallbackOutput(chunk); mo != nil && mo.Message != nil {
			c.events.Publish(Event{RunID: c.runID, Kind: EventAgentToken, Data: mustJSON(map[string]string{"label": info.Name, "delta": mo.Message.Content})})
		}
	}
	return ctx
}
