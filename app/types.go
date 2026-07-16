package app

import (
	"context"

	"github.com/bjaus/flow/app/agent"
	"github.com/bjaus/flow/app/internal/core"
	"github.com/bjaus/flow/app/observe"
	"github.com/bjaus/flow/app/provider"
	"github.com/bjaus/flow/app/store"
)

type Status = core.Status

const (
	StatusQueued         = core.StatusQueued
	StatusRunning        = core.StatusRunning
	StatusAwaitingReview = core.StatusAwaitingReview
	StatusParked         = core.StatusParked
	StatusNeedsMigration = core.StatusNeedsMigration
	StatusSucceeded      = core.StatusSucceeded
	StatusFailed         = core.StatusFailed
	StatusCanceled       = core.StatusCanceled
)

type Run = core.Run
type Decision = core.Decision
type RunFilter = core.RunFilter
type EventKind = core.EventKind

type Event = core.Event

type Persona = core.Persona
type Attr = core.Attr

type CheckpointStore = core.CheckpointStore
type RunStore = core.RunStore
type EventSink = core.EventSink
type Provider = core.Provider
type AgentRegistry = core.AgentRegistry
type Tracer = core.Tracer
type Span = core.Span

const (
	EventRunStarted      = core.EventRunStarted
	EventRunFinished     = core.EventRunFinished
	EventStepStarted     = core.EventStepStarted
	EventStepFinished    = core.EventStepFinished
	EventAgentToken      = core.EventAgentToken
	EventGateReached     = core.EventGateReached
	EventDecisionApplied = core.EventDecisionApplied
	EventRunParked       = core.EventRunParked
	EventRunResumed      = core.EventRunResumed
)

type Stores struct {
	Checkpoint CheckpointStore
	Runs       RunStore
	Events     EventSink
	close      func() error
}

func SQLite(path string) (*Stores, error) {
	s, err := store.OpenSQLite(path)
	if err != nil {
		return nil, err
	}
	return &Stores{Checkpoint: s, Runs: s.Runs(), Events: s, close: s.Close}, nil
}

func (s *Stores) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close()
}

type FakeScript = provider.Script

func FakeProvider(script FakeScript) *provider.Fake    { return provider.NewFake(script) }
func Gateway(baseURL string) *provider.GatewayProvider { return provider.NewGateway(baseURL) }
func MarkdownRegistry(agentsDir, skillsDir string) (*agent.Loader, error) {
	return agent.New(agentsDir, skillsDir)
}
func NoopTracer() Tracer { return core.NoopTracer() }

// OTLPTracer builds the OpenTelemetry tracer configured by standard OTLP environment variables.
func OTLPTracer(ctx context.Context) (Tracer, func(context.Context) error, error) {
	return observe.NewOTLP(ctx)
}
