// Package observe provides OpenTelemetry tracing for flow runs.
package observe

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bjaus/flow/app/internal/core"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Tracer struct{ tracer trace.Tracer }
type span struct{ span trace.Span }

// NewOTLP builds an OTLP/HTTP tracer from OTEL_EXPORTER_OTLP_ENDPOINT and
// OTEL_EXPORTER_OTLP_HEADERS. The returned shutdown function flushes spans.
func NewOTLP(ctx context.Context) (*Tracer, func(context.Context) error, error) {
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		return nil, nil, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT is not set")
	}
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(endpoint)}
	if raw := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"); raw != "" {
		headers := map[string]string{}
		for _, entry := range strings.Split(raw, ",") {
			k, v, ok := strings.Cut(entry, "=")
			if ok {
				headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
		opts = append(opts, otlptracehttp.WithHeaders(headers))
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes("", attribute.String("service.name", "flow")))
	if err != nil {
		return nil, nil, err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	return &Tracer{tracer: provider.Tracer("github.com/bjaus/flow/app")}, provider.Shutdown, nil
}
func (t *Tracer) StartRun(ctx context.Context, r *core.Run) (context.Context, core.Span) {
	ctx, s := t.tracer.Start(ctx, "flow.run "+r.Workflow, trace.WithAttributes(attribute.String("flow.run.id", r.ID), attribute.String("flow.workflow", r.Workflow), attribute.String("flow.workflow.fingerprint", r.Fingerprint)))
	return ctx, span{s}
}
func (t *Tracer) StartStep(ctx context.Context, runID, label, kind string) (context.Context, core.Span) {
	ctx, s := t.tracer.Start(ctx, "flow.step "+label, trace.WithAttributes(attribute.String("flow.run.id", runID), attribute.String("flow.step.label", label), attribute.String("flow.step.kind", kind)))
	return ctx, span{s}
}
func (t *Tracer) StartAgent(ctx context.Context, runID, persona string) (context.Context, core.Span) {
	ctx, s := t.tracer.Start(ctx, "flow.agent "+persona, trace.WithAttributes(attribute.String("flow.run.id", runID), attribute.String("gen_ai.agent.name", persona)))
	return ctx, span{s}
}
func (s span) Set(attrs ...core.Attr) {
	converted := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		switch v := a.Value.(type) {
		case string:
			converted = append(converted, attribute.String(a.Key, v))
		case int:
			converted = append(converted, attribute.Int(a.Key, v))
		case int64:
			converted = append(converted, attribute.Int64(a.Key, v))
		case bool:
			converted = append(converted, attribute.Bool(a.Key, v))
		default:
			converted = append(converted, attribute.String(a.Key, fmt.Sprint(v)))
		}
	}
	s.span.SetAttributes(converted...)
}
func (s span) End(err error) {
	if err != nil {
		s.span.RecordError(err)
	}
	s.span.End()
}
