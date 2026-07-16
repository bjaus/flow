package flow

import (
	"context"

	"github.com/bjaus/flow/internal/ir"
)

// RouterConfig is the dynamic-control escape hatch: each turn a selector picks the next participant from a fixed
// set (over the shared typed state), looping until done or the cap. Expresses supervisor, static network,
// swarm, selector group-chat, and blackboard control. Runtime *membership* change is the actor tier
// (Converse/Network), built on the same dispatcher in the backend.
type RouterConfig[S any] struct {
	Participants map[string]Step[S, S]
	Select       func(S) string // names the next participant from Participants
	Done         func(S) bool   // termination
	Max          int            // safety cap on turns
}

func Router[S any](name string, cfg RouterConfig[S]) Step[S, S] {
	return Step[S, S]{n: &ir.Node{
		Kind: ir.KRouter, Name: "router:" + name, In: typeOf[S](), Out: typeOf[S](),
		Cases:    casesOf(cfg.Participants),
		Classify: func(s any) string { return cfg.Select(s.(S)) },
		Until:    func(_ context.Context, s any) (bool, error) { return cfg.Done(s.(S)), nil },
		Max:      cfg.Max,
	}}
}

// Network is the actor tier: a mesh whose MEMBERSHIP changes at runtime. Members are ids in the state's own
// registry (Next reads it; actors spawn/remove peers by mutating it); each turn Next schedules the next
// actor or signals drain. It lowers to the same in-node dispatcher as Router — the one genuinely non-static
// case that can't be a static graph — so membership lives in checkpointed state and the scope drains cleanly.
type NetworkConfig[S any] struct {
	Actors map[string]Step[S, S]             // behaviors, keyed by name
	Next   func(S) (actor string, more bool) // schedule the next actor; more=false drains the scope
	Max    int
}

func Network[S any](name string, cfg NetworkConfig[S]) Step[S, S] {
	return Step[S, S]{n: &ir.Node{
		Kind: ir.KRouter, Name: "network:" + name, In: typeOf[S](), Out: typeOf[S](),
		Cases:    casesOf(cfg.Actors),
		Classify: func(s any) string { a, _ := cfg.Next(s.(S)); return a },
		Until:    func(_ context.Context, s any) (bool, error) { _, more := cfg.Next(s.(S)); return !more, nil },
		Max:      cfg.Max,
	}}
}
