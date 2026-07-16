package observe

import (
	"context"
	"testing"

	"github.com/bjaus/flow/app/internal/core"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSpanTreeAndAttributes(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tr := &Tracer{tracer: provider.Tracer("test")}
	ctx, run := tr.StartRun(context.Background(), &core.Run{ID: "r1", Workflow: "wf", Fingerprint: "fp"})
	ctx, step := tr.StartStep(ctx, "r1", "draft", "action")
	_, agent := tr.StartAgent(ctx, "r1", "writer")
	agent.Set(core.Attr{Key: "gen_ai.request.model", Value: "local"})
	agent.End(nil)
	step.End(nil)
	run.End(nil)
	spans := rec.Ended()
	require.Len(t, spans, 3)
	require.Equal(t, spans[0].Parent().SpanID(), spans[1].SpanContext().SpanID())
	require.Equal(t, spans[1].Parent().SpanID(), spans[2].SpanContext().SpanID())
}
func TestNewOTLPRequiresEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	_, _, err := NewOTLP(context.Background())
	require.ErrorContains(t, err, "not set")
}
