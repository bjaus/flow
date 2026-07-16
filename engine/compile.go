package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/internal/ir"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/flow/agent/react"
	"github.com/cloudwego/eino/schema"
)

// Runnable is a compiled workflow: a native eino graph. Invoke runs it to a typed result; Stream runs it so
// Agent nodes stream token-by-token (wire WithCallbacks to feed a TUI). A human gate returns an interrupt;
// resume with compose.ResumeWithData + the same checkpoint id.
type Runnable[In, Out any] struct {
	r compose.Runnable[any, any]
}

// DynamicRunnable is the type-erased runtime form used by the app module after it has decoded a run's
// input using the workflow definition's reflected type.
type DynamicRunnable struct {
	r compose.Runnable[any, any]
}

func (r DynamicRunnable) Invoke(ctx context.Context, in any, opts ...compose.Option) (any, error) {
	return r.r.Invoke(ctx, in, opts...)
}

func (r DynamicRunnable) Stream(ctx context.Context, in any, opts ...compose.Option) (*schema.StreamReader[any], error) {
	return r.r.Stream(ctx, in, opts...)
}

func (a Runnable[In, Out]) Invoke(ctx context.Context, in In, opts ...compose.Option) (Out, error) {
	out, err := a.r.Invoke(ctx, in, opts...)
	if err != nil {
		var zero Out
		return zero, err
	}
	return out.(Out), nil
}

// Stream runs the workflow in streaming mode; Agent nodes drive their model's Stream lane. The returned
// reader yields the final step's output chunks; per-agent token feeds come from a streaming callback.
func (a Runnable[In, Out]) Stream(ctx context.Context, in In, opts ...compose.Option) (*schema.StreamReader[any], error) {
	return a.r.Stream(ctx, in, opts...)
}

// Collect runs the workflow with a STREAMING input, returning the final typed value (eino's Collect mode).
func (a Runnable[In, Out]) Collect(ctx context.Context, in *schema.StreamReader[In], opts ...compose.Option) (Out, error) {
	out, err := a.r.Collect(ctx, schema.StreamReaderWithConvert(in, func(v In) (any, error) { return v, nil }), opts...)
	if err != nil {
		var zero Out
		return zero, err
	}
	return out.(Out), nil
}

// Transform runs the workflow with a STREAMING input and a STREAMING output (eino's Transform mode).
func (a Runnable[In, Out]) Transform(ctx context.Context, in *schema.StreamReader[In], opts ...compose.Option) (*schema.StreamReader[any], error) {
	return a.r.Transform(ctx, schema.StreamReaderWithConvert(in, func(v In) (any, error) { return v, nil }), opts...)
}

// Underlying returns the compiled eino runnable, so a flow workflow can be embedded into a hand-written eino
// graph (wrap it in a compose lambda). Composability stays open in BOTH directions: eino into flow (via a Do
// leaf that calls an eino component), and flow back into eino (via this accessor).
func (a Runnable[In, Out]) Underlying() compose.Runnable[any, any] { return a.r }

// Compile lowers a workflow to a native eino graph. Pass a checkpoint store to enable human gates. Extra
// compose.GraphCompileOption values are appended after the defaults, so a caller can override them or add eino
// options flow does not set (a custom serializer, interrupt-before/after breakpoints, a DAG trigger mode,
// compile callbacks): flow reduces eino's boilerplate without hiding eino's compile seam.
func Compile[In, Out any](ctx context.Context, wf flow.Workflow[In, Out], reg Registry, store compose.CheckPointStore, opts ...compose.GraphCompileOption) (Runnable[In, Out], error) {
	// Auto-register the workflow's In/Out types (buffered edge values). A Human step registers its own
	// carried type at construction (flow.Human), so intermediate structs crossing a gate are covered too.
	safeRegister[In]()
	safeRegister[Out]()
	r, err := compileDefinition(ctx, wf.Name, wf.Definition(), reg, store, opts...)
	if err != nil {
		return Runnable[In, Out]{}, err
	}
	return Runnable[In, Out]{r: r}, nil
}

// CompileDefinition compiles the erased definition carried by any flow.Workflow. Most users should call
// Compile; the app runtime uses this form to register workflows whose generic types differ. Extra compile
// options are honored the same way as Compile.
func CompileDefinition(ctx context.Context, name string, definition *ir.Node, reg Registry, store compose.CheckPointStore, opts ...compose.GraphCompileOption) (DynamicRunnable, error) {
	r, err := compileDefinition(ctx, name, definition, reg, store, opts...)
	if err != nil {
		return DynamicRunnable{}, err
	}
	return DynamicRunnable{r: r}, nil
}

func compileDefinition(ctx context.Context, name string, definition *ir.Node, reg Registry, store compose.CheckPointStore, opts ...compose.GraphCompileOption) (compose.Runnable[any, any], error) {
	safeRegister[*fanState]() // the composite state a fan-out checkpoints when a nested Human interrupts
	c := &compiler{reg: reg, store: store}
	c.g = c.newGraph(definition)
	entry, exit, err := c.lower(definition)
	if err != nil {
		return nil, fmt.Errorf("lower %q: %w", name, err)
	}
	if err := c.g.AddEdge(compose.START, entry); err != nil {
		return nil, err
	}
	if err := c.g.AddEdge(exit, compose.END); err != nil {
		return nil, err
	}
	base := []compose.GraphCompileOption{compose.WithGraphName(name), compose.WithMaxRunSteps(200)}
	if store != nil {
		base = append(base, compose.WithCheckPointStore(store))
	}
	base = append(base, opts...) // caller options come last, so they can override the defaults
	r, err := c.g.Compile(ctx, base...)
	if err != nil {
		return nil, err
	}
	return r, nil
}

type compiler struct {
	g     *compose.Graph[any, any]
	reg   Registry
	store compose.CheckPointStore
	seq   int
}

// stateBox holds a workflow's optional shared state (flow.StateDo). It is a pointer in graph-local state so
// mutations persist across steps; V is any so the DSL stays generic without threading a state type param.
type stateBox struct{ V any }

// newGraph builds an eino graph, enabling shared graph-local state only when the (sub)definition actually uses
// it (a StateDo leaf). Workflows that don't use state keep their exact prior behavior, and a stateless human
// gate never has to serialize a state box.
func (c *compiler) newGraph(def *ir.Node) *compose.Graph[any, any] {
	if usesState(def) {
		safeRegister[*stateBox]()
		return compose.NewGraph[any, any](compose.WithGenLocalState(func(context.Context) *stateBox { return &stateBox{} }))
	}
	return compose.NewGraph[any, any]()
}

func usesState(n *ir.Node) bool {
	used := false
	ir.Walk(n, func(m *ir.Node) {
		if m.StateInvoke != nil {
			used = true
		}
	})
	return used
}

func (c *compiler) fresh(prefix string) string {
	c.seq++
	return fmt.Sprintf("%s#%d", prefix, c.seq)
}

// lower adds a node's native structure and returns its entry and exit node keys.
func (c *compiler) lower(n *ir.Node) (entry, exit string, err error) {
	switch n.Kind {
	case ir.KAction:
		return c.lowerAction(n)
	case ir.KAgent:
		return c.lowerAgent(n)
	case ir.KHuman:
		return c.lowerHuman(n)
	case ir.KThen, ir.KSeq:
		return c.lowerChainSteps(n.Steps)
	case ir.KRoute:
		return c.lowerRoute(n)
	case ir.KLoop:
		return c.lowerLoop(n)
	case ir.KGate:
		return c.lowerGate(n)
	case ir.KParallel:
		return c.lowerParallel(n)
	case ir.KMap:
		return c.lowerMap(n)
	case ir.KReduce:
		return c.lowerReduce(n)
	case ir.KBind:
		return c.lowerBind(n)
	case ir.KRouter:
		return c.lowerRouter(n)
	default:
		return "", "", fmt.Errorf("node kind %d not lowered", int(n.Kind))
	}
}

func (c *compiler) lowerAction(n *ir.Node) (string, string, error) {
	key := c.nodeKey(n, label(n))
	if n.StateInvoke != nil {
		return c.lowerStateAction(n, key)
	}
	fn := n.Invoke
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		return fn(ctx, in)
	}), compose.WithNodeName(label(n)))
	return key, key, err
}

// nodeKey returns the author-controlled node key (Step.ID) when set, so per-node run options can target it
// (e.g. compose.WithChatModelOption(model.WithTemperature(0.2)).DesignateNode(id)); otherwise a fresh key.
// Duplicate ids are rejected by Workflow.Validate, so an author-set id is unique across the graph.
func (c *compiler) nodeKey(n *ir.Node, prefix string) string {
	if n.ID != "" {
		return n.ID
	}
	return c.fresh(prefix)
}

// lowerStateAction lowers a StateDo leaf: a lambda that reads/writes the graph's shared state box (via eino's
// concurrency-safe ProcessState) and hands the step get/set accessors.
func (c *compiler) lowerStateAction(n *ir.Node, key string) (string, string, error) {
	fn := n.StateInvoke
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		get := func() any {
			var v any
			_ = compose.ProcessState(ctx, func(_ context.Context, b *stateBox) error { v = b.V; return nil })
			return v
		}
		set := func(v any) {
			_ = compose.ProcessState(ctx, func(_ context.Context, b *stateBox) error { b.V = v; return nil })
		}
		return fn(ctx, in, get, set)
	}), compose.WithNodeName(label(n)))
	return key, key, err
}

// lowerAgent makes a REAL reasoning node: render(In -> messages) -> {ChatModel | ReAct loop} -> parse(-> Out).
// With no tools it is a single traced, streaming ChatModel node; with tools it is a native ReAct agent
// (ChatModel ⇄ ToolsNode) embedded as a sub-graph, so the agent actually calls tools and loops.
func (c *compiler) lowerAgent(n *ir.Node) (string, string, error) {
	p, err := c.reg.Persona(n.Persona)
	if err != nil {
		return "", "", fmt.Errorf("persona %q: %w", n.Persona, err)
	}
	sys, render, outType := p.System, n.Render, n.Out
	renderKey := c.fresh("render_" + n.Persona)
	modelKey := c.nodeKey(n, n.Persona) // stable when the author set Step.ID, so per-node options can target it
	parseKey := c.fresh("parse_" + n.Persona)

	if err := c.g.AddLambdaNode(renderKey, compose.InvokableLambda(func(_ context.Context, in any) ([]*schema.Message, error) {
		msgs := make([]*schema.Message, 0, 2)
		if sys != "" {
			msgs = append(msgs, schema.SystemMessage(sys))
		}
		return append(msgs, schema.UserMessage(render(in))), nil
	})); err != nil {
		return "", "", err
	}
	if len(p.Tools) == 0 {
		if err := c.g.AddChatModelNode(modelKey, p.Model, compose.WithNodeName(n.Persona)); err != nil {
			return "", "", err
		}
	} else {
		tcm, ok := p.Model.(model.ToolCallingChatModel)
		if !ok {
			return "", "", fmt.Errorf("persona %q declares tools but its model is not a model.ToolCallingChatModel", n.Persona)
		}
		ra, err := react.NewAgent(context.Background(), &react.AgentConfig{
			ToolCallingModel: tcm,
			ToolsConfig:      compose.ToolsNodeConfig{Tools: p.Tools},
		})
		if err != nil {
			return "", "", fmt.Errorf("persona %q react agent: %w", n.Persona, err)
		}
		sub, addOpts := ra.ExportGraph()
		if err := c.g.AddGraphNode(modelKey, sub, append(addOpts, compose.WithNodeName(n.Persona))...); err != nil {
			return "", "", err
		}
	}
	if err := c.g.AddLambdaNode(parseKey, compose.InvokableLambda(func(_ context.Context, m *schema.Message) (any, error) {
		return parseInto(m.Content, outType)
	})); err != nil {
		return "", "", err
	}
	if err := c.g.AddEdge(renderKey, modelKey); err != nil {
		return "", "", err
	}
	if err := c.g.AddEdge(modelKey, parseKey); err != nil {
		return "", "", err
	}
	return renderKey, parseKey, nil
}

// lowerHuman makes a real node that natively interrupts and resumes, preserving its typed input across the
// checkpoint (StatefulInterrupt) and applying the operator's decision on resume.
func (c *compiler) lowerHuman(n *ir.Node) (string, string, error) {
	key := c.fresh("human_" + n.Name)
	apply, render := n.Apply, n.Render
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		if was, has, saved := compose.GetInterruptState[any](ctx); was && has {
			_, _, dec := compose.GetResumeContext[flow.Decision](ctx)
			return apply(saved, dec), nil
		}
		return nil, compose.StatefulInterrupt(ctx, render(in), in)
	}), compose.WithNodeName(n.Name))
	return key, key, err
}

func (c *compiler) lowerChainSteps(steps []*ir.Node) (string, string, error) {
	if len(steps) == 0 {
		return "", "", fmt.Errorf("chain has no steps")
	}
	var first, prevExit string
	for i, s := range steps {
		e, x, err := c.lower(s)
		if err != nil {
			return "", "", err
		}
		if i == 0 {
			first = e
		} else if err := c.g.AddEdge(prevExit, e); err != nil {
			return "", "", err
		}
		prevExit = x
	}
	return first, prevExit, nil
}

func (c *compiler) passthrough(prefix string) (string, error) {
	key := c.fresh(prefix)
	return key, c.g.AddLambdaNode(key, compose.InvokableLambda(func(_ context.Context, in any) (any, error) { return in, nil }))
}

// compileSub lowers a subtree into its OWN compiled runnable — used by the fan-out/dispatch combinators so
// each branch runs (concurrently) as an independent unit. The checkpoint store is threaded in, so a Human (or
// any interrupt) nested inside a branch checkpoints rather than erroring; the fan-out node then propagates a
// CompositeInterrupt (see durableFanOut) and eino resumes the exact branch.
func (c *compiler) compileSub(n *ir.Node) (compose.Runnable[any, any], error) {
	sub := &compiler{reg: c.reg, store: c.store}
	sub.g = sub.newGraph(n)
	e, x, err := sub.lower(n)
	if err != nil {
		return nil, err
	}
	if err := sub.g.AddEdge(compose.START, e); err != nil {
		return nil, err
	}
	if err := sub.g.AddEdge(x, compose.END); err != nil {
		return nil, err
	}
	opts := []compose.GraphCompileOption{compose.WithMaxRunSteps(200)}
	if c.store != nil {
		opts = append(opts, compose.WithCheckPointStore(c.store))
	}
	return sub.g.Compile(context.Background(), opts...)
}

// lowerRoute: a branch that dispatches to one case; cases fan into a join (native).
func (c *compiler) lowerRoute(n *ir.Node) (string, string, error) {
	entry, err := c.passthrough("route")
	if err != nil {
		return "", "", err
	}
	join, err := c.passthrough("route_join")
	if err != nil {
		return "", "", err
	}
	cases := map[string]string{} // case key -> entry node
	targets := map[string]bool{}
	lowerCase := func(key string, cn *ir.Node) error {
		ce, cx, err := c.lower(cn)
		if err != nil {
			return err
		}
		cases[key] = ce
		targets[ce] = true
		return c.g.AddEdge(cx, join)
	}
	for k, cn := range n.Cases {
		if err := lowerCase(k, cn); err != nil {
			return "", "", err
		}
	}
	if n.Default != nil {
		if err := lowerCase("\x00default", n.Default); err != nil {
			return "", "", err
		}
	}
	classify := n.Classify
	branch := compose.NewGraphBranch(func(_ context.Context, in any) (string, error) {
		if e, ok := cases[classify(in)]; ok {
			return e, nil
		}
		if e, ok := cases["\x00default"]; ok {
			return e, nil
		}
		return "", fmt.Errorf("route %q: no case for %q and no default", n.Name, classify(in))
	}, targets)
	if err := c.g.AddBranch(entry, branch); err != nil {
		return "", "", err
	}
	return entry, join, nil
}

// lowerLoop: a Pregel cycle — body, then a branch that loops back or exits per the gate (native).
func (c *compiler) lowerLoop(n *ir.Node) (string, string, error) {
	be, bx, err := c.lower(n.Body)
	if err != nil {
		return "", "", err
	}
	exit, err := c.passthrough("loop_exit")
	if err != nil {
		return "", "", err
	}
	until := n.Until
	branch := compose.NewGraphBranch(func(ctx context.Context, in any) (string, error) {
		pass, err := until(ctx, in)
		if err != nil {
			return "", err
		}
		if pass {
			return exit, nil
		}
		return be, nil
	}, map[string]bool{be: true, exit: true})
	if err := c.g.AddBranch(bx, branch); err != nil {
		return "", "", err
	}
	return be, exit, nil
}

// lowerGate: pass the value through iff the gate holds, else fail (native).
func (c *compiler) lowerGate(n *ir.Node) (string, string, error) {
	key := c.fresh("gate_" + n.Name)
	until := n.Until
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		pass, err := until(ctx, in)
		if err != nil {
			return nil, err
		}
		if !pass {
			return nil, fmt.Errorf("gate %q rejected", n.Name)
		}
		return in, nil
	}), compose.WithNodeName("gate:"+n.Name))
	return key, key, err
}

// lowerParallel: run branches CONCURRENTLY (goroutines) and collect a typed []Out.
func (c *compiler) lowerParallel(n *ir.Node) (string, string, error) {
	subs := make([]compose.Runnable[any, any], len(n.Steps))
	for i, b := range n.Steps {
		r, err := c.compileSub(b)
		if err != nil {
			return "", "", err
		}
		subs[i] = r
	}
	outType := n.Out
	key := c.fresh("parallel")
	durable := c.store != nil
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		inputs := make([]any, len(subs))
		for i := range inputs {
			inputs[i] = in
		}
		results, err := durableFanOut(ctx, key, durable, func(i int) compose.Runnable[any, any] { return subs[i] }, inputs)
		if err != nil {
			return nil, err
		}
		return toTypedSlice(outType, results), nil
	}), compose.WithNodeName("parallel"))
	return key, key, err
}

// lowerMap: fan a runtime list out to a body per item, concurrently, collect typed []Out.
func (c *compiler) lowerMap(n *ir.Node) (string, string, error) {
	body, err := c.compileSub(n.Body)
	if err != nil {
		return "", "", err
	}
	outType := n.Out
	key := c.fresh("map")
	durable := c.store != nil
	err = c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		var inputs []any
		if rv := reflect.ValueOf(in); rv.IsValid() && rv.Kind() == reflect.Slice {
			inputs = make([]any, rv.Len())
			for i := range inputs {
				inputs[i] = rv.Index(i).Interface()
			}
		}
		results, err := durableFanOut(ctx, key, durable, func(int) compose.Runnable[any, any] { return body }, inputs)
		if err != nil {
			return nil, err
		}
		return toTypedSlice(outType, results), nil
	}), compose.WithNodeName("map"))
	return key, key, err
}

// lowerReduce: fold a fan-out's []Out into one Out (native edge to a fold lambda).
func (c *compiler) lowerReduce(n *ir.Node) (string, string, error) {
	oe, ox, err := c.lower(n.Over)
	if err != nil {
		return "", "", err
	}
	foldKey := c.fresh("reduce")
	fold := n.Fold
	if err := c.g.AddLambdaNode(foldKey, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		return fold(ctx, in)
	}), compose.WithNodeName("reduce")); err != nil {
		return "", "", err
	}
	if err := c.g.AddEdge(ox, foldKey); err != nil {
		return "", "", err
	}
	return oe, foldKey, nil
}

// lowerBind: lift a typed sub-step into the state spine via read/write lenses (one dispatch node).
func (c *compiler) lowerBind(n *ir.Node) (string, string, error) {
	inner, err := c.compileSub(n.Body)
	if err != nil {
		return "", "", err
	}
	read, write := n.Read, n.Write
	key := c.fresh("bind")
	durable := c.store != nil
	err = c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, s any) (any, error) {
		if !durable {
			out, err := inner.Invoke(ctx, read(s))
			if err != nil {
				return nil, err
			}
			return write(s, out), nil
		}
		// resume: restore the outer state saved when the inner sub-workflow paused at a Human, so the write
		// lens still runs against the right state after the operator's decision flows through.
		if was, has, saved := compose.GetInterruptState[any](ctx); was && has {
			s = saved
		}
		sctx := compose.AppendAddressSegment(ctx, addrFanOut, "bind")
		out, err := inner.Invoke(sctx, read(s), compose.WithCheckPointID(key+"/bind"))
		if err != nil {
			if isInterrupt(err) {
				return nil, compose.CompositeInterrupt(ctx, nil, s, err)
			}
			return nil, err
		}
		return write(s, out), nil
	}), compose.WithNodeName("bind"))
	return key, key, err
}

// lowerRouter: dynamic dispatch — each turn a selector picks a participant. A Human participant suspends the
// mesh DURABLY: the dispatcher checkpoints the mesh STATE (which carries its own turn) and, on resume,
// applies the operator's decision and continues — the selector re-drives from the recovered state, so
// completed turns don't re-run (the actor that suspended must have advanced the state). Non-human
// participants run as (streaming) sub-runnables.
func (c *compiler) lowerRouter(n *ir.Node) (string, string, error) {
	subs := map[string]compose.Runnable[any, any]{}
	humans := map[string]*ir.Node{}
	for k, cn := range n.Cases {
		if cn.Kind == ir.KHuman {
			humans[k] = cn
			continue
		}
		r, err := c.compileSub(cn)
		if err != nil {
			return "", "", err
		}
		subs[k] = r
	}
	sel, done, max := n.Classify, n.Until, n.Max
	key := c.fresh("router_" + n.Name)
	durable := c.store != nil
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, s any) (any, error) {
		cur := s
		// resume: recover the checkpointed mesh state and apply the human's decision at the suspended turn.
		if was, has, saved := compose.GetInterruptState[any](ctx); was && has {
			cur = saved
			if h, ok := humans[sel(cur)]; ok {
				_, _, dec := compose.GetResumeContext[flow.Decision](ctx)
				cur = h.Apply(cur, dec)
			}
		}
		for range max {
			if d, err := done(ctx, cur); err != nil {
				return nil, err
			} else if d {
				break
			}
			who := sel(cur)
			if h, isHuman := humans[who]; isHuman {
				return nil, compose.StatefulInterrupt(ctx, h.Render(cur), cur)
			}
			r, ok := subs[who]
			if !ok {
				return nil, fmt.Errorf("dispatcher %q: no participant %q", n.Name, who)
			}
			var next any
			var err error
			if durable {
				sctx := compose.AppendAddressSegment(ctx, addrFanOut, who)
				next, err = streamToValue(sctx, r, cur, compose.WithCheckPointID(key+"/"+who))
			} else {
				next, err = streamToValue(ctx, r, cur)
			}
			if err != nil {
				if durable && isInterrupt(err) {
					// a Human nested INSIDE a participant subtree: checkpoint the mesh state and, on resume,
					// re-run this participant so its inner gate applies the decision (sel(cur) is not a human,
					// so the resume block above skips straight to re-running the participant).
					return nil, compose.CompositeInterrupt(ctx, nil, cur, err)
				}
				return nil, err
			}
			cur = next
		}
		return cur, nil
	}), compose.WithNodeName("router:"+n.Name))
	return key, key, err
}

// addrFanOut is the address-segment type flow gives each fan-out branch (and each bind/dispatch sub-run), so
// eino can route a resume decision to the exact branch that paused.
const addrFanOut compose.AddressSegmentType = "flow_fanout"

// fanState is a fan-out node's durable state: the branch inputs (restored on resume, since edge values are
// not), the results already completed, and the branch indices still interrupted. Registered for checkpointing
// in compileDefinition.
type fanState struct {
	Inputs    []any
	Completed map[int]any
	Pending   []int
}

// durableFanOut runs one sub-runnable per input, STREAMING each branch (so agent tokens surface to callbacks)
// and, if a branch pauses at a Human, propagating a CompositeInterrupt so the whole fan-out checkpoints; on
// resume it restores completed branches and re-runs only the interrupted ones, routing the operator's decision
// to the right branch via a per-index address segment + checkpoint id.
func durableFanOut(ctx context.Context, seg string, durable bool, sub func(i int) compose.Runnable[any, any], inputs []any) ([]any, error) {
	completed := map[int]any{}
	var pending []int
	if was, has, st := compose.GetInterruptState[*fanState](ctx); was && has && st != nil {
		inputs = st.Inputs
		for i, v := range st.Completed {
			completed[i] = v
		}
		pending = st.Pending
	} else {
		for i := range inputs {
			pending = append(pending, i)
		}
	}
	results := make([]any, len(inputs))
	for i, v := range completed {
		results[i] = v
	}

	type res struct {
		i   int
		out any
		err error
	}
	ch := make(chan res, len(pending))
	var wg sync.WaitGroup
	for _, idx := range pending {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// A per-branch address + checkpoint id lets eino route a resume to the exact branch — but only
			// when a store is configured; passing a checkpoint id without one is an error.
			sctx, opts := ctx, []compose.Option(nil)
			if durable {
				sctx = compose.AppendAddressSegment(ctx, addrFanOut, strconv.Itoa(i))
				opts = []compose.Option{compose.WithCheckPointID(seg + "/" + strconv.Itoa(i))}
			}
			out, err := streamToValue(sctx, sub(i), inputs[i], opts...)
			ch <- res{i: i, out: out, err: err}
		}(idx)
	}
	go func() { wg.Wait(); close(ch) }()

	var normalErr error
	var interruptErrs []error
	var stillPending []int
	for r := range ch {
		switch {
		case r.err == nil:
			results[r.i] = r.out
			completed[r.i] = r.out
		case isInterrupt(r.err):
			interruptErrs = append(interruptErrs, r.err)
			stillPending = append(stillPending, r.i)
		case normalErr == nil:
			normalErr = r.err
		}
	}
	if normalErr != nil {
		return nil, normalErr
	}
	if len(interruptErrs) > 0 {
		return nil, compose.CompositeInterrupt(ctx, nil, &fanState{Inputs: inputs, Completed: completed, Pending: stillPending}, interruptErrs...)
	}
	return results, nil
}

func isInterrupt(err error) bool {
	_, ok := compose.ExtractInterruptInfo(err)
	return ok
}

// streamToValue runs a sub-runnable in streaming mode (so its model nodes stream + emit callbacks) and
// returns its final output value (the last chunk — our branches end in a typed value, i.e. one chunk).
func streamToValue(ctx context.Context, r compose.Runnable[any, any], in any, opts ...compose.Option) (any, error) {
	sr, err := r.Stream(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	var last any
	for {
		chunk, err := sr.Recv()
		if err == io.EOF {
			return last, nil
		}
		if err != nil {
			return nil, err
		}
		last = chunk
	}
}

// toTypedSlice builds a typed []Out (sliceType) from erased element values.
func toTypedSlice(sliceType reflect.Type, items []any) any {
	s := reflect.MakeSlice(sliceType, len(items), len(items))
	for i, it := range items {
		if it != nil {
			s.Index(i).Set(reflect.ValueOf(it))
		}
	}
	return s.Interface()
}

// parseInto decodes a model's text output into the step's declared Out type.
func parseInto(content string, out reflect.Type) (any, error) {
	if out.Kind() == reflect.String {
		return content, nil
	}
	v := reflect.New(out)
	if err := json.Unmarshal([]byte(content), v.Interface()); err != nil {
		return nil, fmt.Errorf("agent output did not match %s: %w", out, err)
	}
	return v.Elem().Interface(), nil
}

func label(n *ir.Node) string {
	if n.Name != "" {
		return n.Name
	}
	return "step"
}
