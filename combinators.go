package flow

import (
	"context"

	"github.com/bjaus/flow/internal/ir"
)

// Then wires a's output into b's input — typed dataflow, checked by the compiler on both sides.
func Then[A, B, C any](a Step[A, B], b Step[B, C]) Step[A, C] {
	return Step[A, C]{n: &ir.Node{
		Kind: ir.KThen, Name: a.n.Name + "→" + b.n.Name, In: typeOf[A](), Out: typeOf[C](),
		Steps: []*ir.Node{a.n, b.n},
	}}
}

// Seq composes same-typed steps (the state-transformer spine). Flat and fully typed.
func Seq[S any](steps ...Step[S, S]) Step[S, S] {
	return Step[S, S]{n: &ir.Node{
		Kind: ir.KSeq, Name: "seq", In: typeOf[S](), Out: typeOf[S](), Steps: nodesOf(steps),
	}}
}

// Parallel fans one input to N branches concurrently, collecting typed outputs.
func Parallel[In, Out any](branches ...Step[In, Out]) Step[In, []Out] {
	return Step[In, []Out]{n: &ir.Node{
		Kind: ir.KParallel, Name: "parallel", In: typeOf[In](), Out: typeOf[[]Out](), Steps: nodesOf(branches),
	}}
}

// Map fans a runtime-sized list out to a body per item, collecting typed outputs.
func Map[In, Out any](each Step[In, Out]) Step[[]In, []Out] {
	return Step[[]In, []Out]{n: &ir.Node{
		Kind: ir.KMap, Name: "map", In: typeOf[[]In](), Out: typeOf[[]Out](), Body: each.n,
	}}
}

// Reduce folds a fan-out's typed outputs into one value.
func Reduce[In, Out any](s Step[In, []Out], fold func([]Out) Out) Step[In, Out] {
	return Step[In, Out]{n: &ir.Node{
		Kind: ir.KReduce, Name: "reduce", In: typeOf[In](), Out: typeOf[Out](), Over: s.n,
		Fold: func(_ context.Context, in any) (any, error) { return fold(in.([]Out)), nil },
	}}
}

// Route classifies with `by`, then dispatches to a matching case (falling back to Default if set).
func Route[In, Out any](by func(In) string, cases map[string]Step[In, Out]) Step[In, Out] {
	return Step[In, Out]{n: &ir.Node{
		Kind: ir.KRoute, Name: "route", In: typeOf[In](), Out: typeOf[Out](),
		Cases:    casesOf(cases),
		Classify: func(in any) string { return by(in.(In)) },
	}}
}

// Default sets a route's fallback branch.
func (s Step[In, Out]) Default(alt Step[In, Out]) Step[In, Out] { s.n.Default = alt.n; return s }

// Gate is a pass/fail check over a typed value (a state predicate here; shell/judge/human are backend forms).
type Gate[T any] struct{ check ir.Predicate }

// StateGate passes when a predicate over the typed value is true.
func StateGate[T any](pred func(T) bool) Gate[T] {
	return Gate[T]{check: func(_ context.Context, in any) (bool, error) { return pred(in.(T)), nil }}
}

// Guard is a standalone pass/fail check: it passes the value through if the gate holds, else fails the run
// (guardrails / tripwires).
func Guard[T any](name string, gate Gate[T]) Step[T, T] {
	return Step[T, T]{n: &ir.Node{
		Kind: ir.KGate, Name: "gate:" + name, In: typeOf[T](), Out: typeOf[T](), Until: gate.check,
	}}
}

// Loop repeats a same-typed body until the gate passes or the cap is hit.
func Loop[T any](name string, body Step[T, T], until Gate[T], max int) Step[T, T] {
	return Step[T, T]{n: &ir.Node{
		Kind: ir.KLoop, Name: "loop:" + name, In: typeOf[T](), Out: typeOf[T](),
		Body: body.n, Until: until.check, Max: max,
	}}
}

// Bind lifts a pure typed Step[In,Out] into the state spine via read/write lenses.
func Bind[S, In, Out any](s Step[In, Out], read func(S) In, write func(S, Out) S) Step[S, S] {
	return Step[S, S]{n: &ir.Node{
		Kind: ir.KBind, Name: "bind:" + s.n.Name, In: typeOf[S](), Out: typeOf[S](), Body: s.n,
		Read:  func(st any) any { return read(st.(S)) },
		Write: func(st any, out any) any { return write(st.(S), out.(Out)) },
	}}
}

func nodesOf[In, Out any](steps []Step[In, Out]) []*ir.Node {
	ns := make([]*ir.Node, len(steps))
	for i, s := range steps {
		ns[i] = s.n
	}
	return ns
}

func casesOf[In, Out any](cases map[string]Step[In, Out]) map[string]*ir.Node {
	cs := make(map[string]*ir.Node, len(cases))
	for k, v := range cases {
		cs[k] = v.n
	}
	return cs
}
