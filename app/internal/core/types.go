package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cloudwego/eino/components/model"
)

type Status string

const (
	StatusQueued         Status = "queued"
	StatusRunning        Status = "running"
	StatusAwaitingReview Status = "awaiting_review"
	StatusParked         Status = "parked"
	StatusNeedsMigration Status = "needs_migration"
	StatusSucceeded      Status = "succeeded"
	StatusFailed         Status = "failed"
	StatusCanceled       Status = "canceled"
)

func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCanceled
}

type Run struct {
	ID            string          `json:"id"`
	Workflow      string          `json:"workflow"`
	Fingerprint   string          `json:"fingerprint"`
	Status        Status          `json:"status"`
	Trigger       string          `json:"trigger,omitempty"`
	ParentID      string          `json:"parent_id,omitempty"`
	Input         json.RawMessage `json:"input"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         string          `json:"error,omitempty"`
	InterruptID   string          `json:"interrupt_id,omitempty"`
	GatePrompt    string          `json:"gate_prompt,omitempty"`
	Decision      *Decision       `json:"decision,omitempty"`
	CancelPending bool            `json:"cancel_pending,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	StartedAt     *time.Time      `json:"started_at,omitempty"`
	FinishedAt    *time.Time      `json:"finished_at,omitempty"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type Decision struct {
	Approved bool   `json:"approved"`
	Feedback string `json:"feedback,omitempty"`
}

type ConfigStatus struct {
	Dirty     bool       `json:"dirty"`
	LoadedAt  time.Time  `json:"loaded_at"`
	ChangedAt *time.Time `json:"changed_at,omitempty"`
	Error     string     `json:"error,omitempty"`
	Files     []string   `json:"files"`
}

type RunFilter struct {
	Workflow string
	Status   Status
	ParentID string
}

type EventKind string

const (
	EventRunStarted      EventKind = "run.started"
	EventRunFinished     EventKind = "run.finished"
	EventStepStarted     EventKind = "step.started"
	EventStepFinished    EventKind = "step.finished"
	EventAgentToken      EventKind = "agent.token"
	EventGateReached     EventKind = "gate.reached"
	EventDecisionApplied EventKind = "decision.applied"
	EventRunParked       EventKind = "run.parked"
	EventRunResumed      EventKind = "run.resumed"
	EventConfigChanged   EventKind = "config.changed"
	EventConfigReloaded  EventKind = "config.reloaded"
	EventTriggerSkipped  EventKind = "trigger.skipped"
)

type Event struct {
	RunID string          `json:"run_id"`
	Seq   int64           `json:"seq"`
	Kind  EventKind       `json:"kind"`
	At    time.Time       `json:"at"`
	Data  json.RawMessage `json:"data,omitempty"`
}

type Persona struct {
	Name              string   `json:"name"`
	Model             string   `json:"model"`
	FallbackModels    []string `json:"fallback_models,omitempty"`
	Profile           string   `json:"profile,omitempty"`
	Roles             []string `json:"roles,omitempty"`
	Tools             []string `json:"tools,omitempty"`
	ToolPermissions   []string `json:"tool_permissions,omitempty"`
	Skills            []string `json:"skills,omitempty"`
	SystemInstruction string   `json:"system_instruction"`
}

type Attr struct {
	Key   string
	Value any
}

type CheckpointStore interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
	Delete(context.Context, string) error
}

type RunStore interface {
	Save(context.Context, *Run) error
	Get(context.Context, string) (*Run, error)
	List(context.Context, RunFilter) ([]*Run, error)
	Claim(context.Context) (*Run, error)
}

type EventSink interface {
	Publish(Event)
	Subscribe(string) (<-chan Event, func())
}

type Provider interface {
	Model(context.Context, Persona) (model.BaseChatModel, error)
}

type AgentRegistry interface {
	Persona(string) (Persona, bool)
}

type Tracer interface {
	StartRun(context.Context, *Run) (context.Context, Span)
	StartStep(context.Context, string, string, string) (context.Context, Span)
	StartAgent(context.Context, string, string) (context.Context, Span)
}

type Span interface {
	Set(...Attr)
	End(error)
}

type noopTracer struct{}
type noopSpan struct{}

func NoopTracer() Tracer { return noopTracer{} }
func (noopTracer) StartRun(ctx context.Context, _ *Run) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopTracer) StartStep(ctx context.Context, _, _, _ string) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopTracer) StartAgent(ctx context.Context, _, _ string) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopSpan) Set(...Attr) {}
func (noopSpan) End(error)   {}
