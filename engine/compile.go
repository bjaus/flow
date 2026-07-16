package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/internal/ir"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// Runnable is a compiled workflow: a native eino graph. Invoke runs it to a typed result; Stream runs it so
// Agent nodes stream token-by-token (wire WithCallbacks to feed a TUI). A human gate returns an interrupt;
// resume with compose.ResumeWithData + the same checkpoint id.
type Runnable[In, Out any] struct {
	r compose.Runnable[any, any]
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

// Compile lowers a workflow to a native eino graph. Pass a checkpoint store to enable human gates.
func Compile[In, Out any](ctx context.Context, wf flow.Workflow[In, Out], reg Registry, store compose.CheckPointStore) (Runnable[In, Out], error) {
	// Auto-register the workflow's In/Out types (buffered edge values). A Human step registers its own
	// carried type at construction (flow.Human), so intermediate structs crossing a gate are covered too.
	safeRegister[In]()
	safeRegister[Out]()
	g := compose.NewGraph[any, any]()
	c := &compiler{g: g, reg: reg}
	entry, exit, err := c.lower(wf.Definition())
	if err != nil {
		return Runnable[In, Out]{}, fmt.Errorf("lower %q: %w", wf.Name, err)
	}
	if err := g.AddEdge(compose.START, entry); err != nil {
		return Runnable[In, Out]{}, err
	}
	if err := g.AddEdge(exit, compose.END); err != nil {
		return Runnable[In, Out]{}, err
	}
	opts := []compose.GraphCompileOption{compose.WithGraphName(wf.Name), compose.WithMaxRunSteps(200)}
	if store != nil {
		opts = append(opts, compose.WithCheckPointStore(store))
	}
	r, err := g.Compile(ctx, opts...)
	if err != nil {
		return Runnable[In, Out]{}, err
	}
	return Runnable[In, Out]{r: r}, nil
}

type compiler struct {
	g   *compose.Graph[any, any]
	reg Registry
	seq int
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
	key := c.fresh(label(n))
	fn := n.Invoke
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		return fn(ctx, in)
	}), compose.WithNodeName(label(n)))
	return key, key, err
}

// lowerAgent makes a REAL model node: render(In -> messages) -> ChatModel(streams, traced) -> parse(-> Out).
func (c *compiler) lowerAgent(n *ir.Node) (string, string, error) {
	cm, sys, err := c.reg.Persona(n.Persona)
	if err != nil {
		return "", "", fmt.Errorf("persona %q: %w", n.Persona, err)
	}
	renderKey, modelKey, parseKey := c.fresh("render_"+n.Persona), c.fresh(n.Persona), c.fresh("parse_"+n.Persona)
	render, outType := n.Render, n.Out

	if err := c.g.AddLambdaNode(renderKey, compose.InvokableLambda(func(_ context.Context, in any) ([]*schema.Message, error) {
		msgs := make([]*schema.Message, 0, 2)
		if sys != "" {
			msgs = append(msgs, schema.SystemMessage(sys))
		}
		return append(msgs, schema.UserMessage(render(in))), nil
	})); err != nil {
		return "", "", err
	}
	if err := c.g.AddChatModelNode(modelKey, cm, compose.WithNodeName(n.Persona)); err != nil {
		return "", "", err
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
// each branch runs (concurrently) as an independent unit. (Nodes inside a sub-runnable still emit tracing
// callbacks; per-agent token streaming inside a fan-out is a later enhancement.)
func (c *compiler) compileSub(n *ir.Node) (compose.Runnable[any, any], error) {
	sub := &compiler{g: compose.NewGraph[any, any](), reg: c.reg}
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
	return sub.g.Compile(context.Background(), compose.WithMaxRunSteps(200))
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
	err := c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		results, err := runConcurrent(ctx, subs, func(_ int) any { return in })
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
	err = c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, in any) (any, error) {
		v := reflect.ValueOf(in)
		subs := make([]compose.Runnable[any, any], v.Len())
		for i := range subs {
			subs[i] = body
		}
		results, err := runConcurrent(ctx, subs, func(i int) any { return v.Index(i).Interface() })
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
	err = c.g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, s any) (any, error) {
		out, err := inner.Invoke(ctx, read(s))
		if err != nil {
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
			var err error
			if cur, err = streamToValue(ctx, r, cur); err != nil {
				return nil, err
			}
		}
		return cur, nil
	}), compose.WithNodeName("router:"+n.Name))
	return key, key, err
}

// runConcurrent runs each sub-runnable on its input concurrently and returns results in order. It STREAMS
// each branch (not Invoke) so agent nodes inside a fan-out drive their model's Stream lane and emit
// streaming callbacks — surfacing each branch's tokens to the observability sink — then collects the final
// value from each branch's output stream.
func runConcurrent(ctx context.Context, subs []compose.Runnable[any, any], input func(i int) any) ([]any, error) {
	results := make([]any, len(subs))
	errs := make([]error, len(subs))
	var wg sync.WaitGroup
	for i, r := range subs {
		wg.Add(1)
		go func(i int, r compose.Runnable[any, any]) {
			defer wg.Done()
			results[i], errs[i] = streamToValue(ctx, r, input(i))
		}(i, r)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	return results, nil
}

// streamToValue runs a sub-runnable in streaming mode (so its model nodes stream + emit callbacks) and
// returns its final output value (the last chunk — our branches end in a typed value, i.e. one chunk).
func streamToValue(ctx context.Context, r compose.Runnable[any, any], in any) (any, error) {
	sr, err := r.Stream(ctx, in)
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
