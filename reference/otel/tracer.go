package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer provides distributed tracing capabilities for the CIRIYC PRO framework.
// It wraps OpenTelemetry tracing with framework-specific conveniences.
type Tracer struct {
	// tracer underlying OpenTelemetry tracer
	tracer trace.Tracer

	// provider trace provider
	provider *sdktrace.TracerProvider

	// serviceName service identifier
	serviceName string

	// config tracer configuration
	config TracerConfig
}

// TracerConfig holds tracer configuration.
type TracerConfig struct {
	// ServiceName identifies the service
	ServiceName string

	// ServiceVersion service version
	ServiceVersion string

	// Environment deployment environment (dev, staging, prod)
	Environment string

	// Exporter trace exporter configuration
	Exporter ExporterConfig

	// Sampling sampling configuration
	Sampling SamplingConfig

	// Attributes default attributes to add to all spans
	Attributes map[string]string
}

// ExporterConfig configures the trace exporter.
type ExporterConfig struct {
	// Type exporter type ("jaeger", "tempo", "zipkin", "otlp")
	Type string

	// Endpoint exporter endpoint
	Endpoint string

	// Headers additional headers for exporter
	Headers map[string]string

	// TLS TLS configuration
	TLS TLSConfig

	// BatchTimeout batch timeout duration
	BatchTimeout time.Duration

	// MaxBatchSize maximum batch size
	MaxBatchSize int
}

// SamplingConfig configures trace sampling.
type SamplingConfig struct {
	// Strategy sampling strategy ("always", "never", "ratio", "parent_based")
	Strategy string

	// Ratio sampling ratio for ratio-based sampling (0.0 - 1.0)
	Ratio float64

	// RateLimiter rate limiter for rate-based sampling
	RateLimiter *RateLimiterConfig
}

// RateLimiterConfig configures rate-based sampling.
type RateLimiterConfig struct {
	// MaxTracesPerSecond maximum traces per second
	MaxTracesPerSecond int
}

// TLSConfig specifies TLS parameters.
type TLSConfig struct {
	// Enabled turns on TLS
	Enabled bool

	// CACert path to CA certificate
	CACert string

	// ClientCert path to client certificate
	ClientCert string

	// ClientKey path to client private key
	ClientKey string

	// InsecureSkipVerify skips certificate validation
	InsecureSkipVerify bool
}

// NewTracer creates a new tracer instance.
func NewTracer(config TracerConfig) (*Tracer, error) {
	// Create resource with service information
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(config.ServiceName),
			semconv.ServiceVersion(config.ServiceVersion),
			semconv.DeploymentEnvironment(config.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace exporter
	exporter, err := createExporter(config.Exporter)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	// Create sampler
	sampler := createSampler(config.Sampling)

	// Create trace provider
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sampler),
	)

	// Set global provider
	otel.SetTracerProvider(provider)

	// Set global propagator
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	// Create tracer
	tracer := provider.Tracer(
		config.ServiceName,
		trace.WithInstrumentationVersion(config.ServiceVersion),
	)

	return &Tracer{
		tracer:      tracer,
		provider:    provider,
		serviceName: config.ServiceName,
		config:      config,
	}, nil
}

// StartSpan starts a new span.
func (t *Tracer) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, name, opts...)
}

// StartAgentSpan starts a span for agent execution.
func (t *Tracer) StartAgentSpan(ctx context.Context, agentID, agentType, operation string) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("agent.%s.%s", agentType, operation)
	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindInternal),
	)

	// Add agent-specific attributes
	span.SetAttributes(
		attribute.String("agent.id", agentID),
		attribute.String("agent.type", agentType),
		attribute.String("agent.operation", operation),
	)

	return ctx, span
}

// StartMessageSpan starts a span for message processing.
func (t *Tracer) StartMessageSpan(ctx context.Context, messageID, messageType, direction string) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("message.%s.%s", messageType, direction)
	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
	)

	span.SetAttributes(
		attribute.String("message.id", messageID),
		attribute.String("message.type", messageType),
		attribute.String("message.direction", direction),
	)

	return ctx, span
}

// StartDBSpan starts a span for database operations.
func (t *Tracer) StartDBSpan(ctx context.Context, operation, table string) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("db.%s.%s", operation, table)
	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
	)

	span.SetAttributes(
		attribute.String("db.operation", operation),
		attribute.String("db.table", table),
	)

	return ctx, span
}

// StartHTTPSpan starts a span for HTTP requests.
func (t *Tracer) StartHTTPSpan(ctx context.Context, method, url string) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("http.%s", method)
	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
	)

	span.SetAttributes(
		attribute.String("http.method", method),
		attribute.String("http.url", url),
	)

	return ctx, span
}

// RecordError records an error on the current span.
func (t *Tracer) RecordError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// AddEvent adds an event to the current span.
func (t *Tracer) AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// SetAttributes sets attributes on the current span.
func (t *Tracer) SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetAttributes(attrs...)
	}
}

// InjectContext injects trace context into a carrier (for propagation).
func (t *Tracer) InjectContext(ctx context.Context, carrier propagation.TextMapCarrier) {
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, carrier)
}

// ExtractContext extracts trace context from a carrier.
func (t *Tracer) ExtractContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, carrier)
}

// Shutdown gracefully shuts down the tracer.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t.provider != nil {
		return t.provider.Shutdown(ctx)
	}
	return nil
}

// Helper functions

func createExporter(config ExporterConfig) (sdktrace.SpanExporter, error) {
	// In a real implementation, this would create the appropriate exporter
	// based on config.Type (jaeger, tempo, zipkin, otlp)
	// For this listing, we'll return a no-op implementation
	return &noopExporter{}, nil
}

func createSampler(config SamplingConfig) sdktrace.Sampler {
	switch config.Strategy {
	case "always":
		return sdktrace.AlwaysSample()
	case "never":
		return sdktrace.NeverSample()
	case "ratio":
		return sdktrace.TraceIDRatioBased(config.Ratio)
	case "parent_based":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.Ratio))
	default:
		return sdktrace.AlwaysSample()
	}
}

// noopExporter is a no-op exporter for demonstration.
type noopExporter struct{}

func (e *noopExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e *noopExporter) Shutdown(ctx context.Context) error {
	return nil
}

// SpanContext holds trace context information.
type SpanContext struct {
	// TraceID trace identifier
	TraceID string

	// SpanID span identifier
	SpanID string

	// TraceFlags trace flags
	TraceFlags byte

	// TraceState trace state
	TraceState string
}

// ExtractSpanContext extracts span context from a context.
func ExtractSpanContext(ctx context.Context) *SpanContext {
	span := trace.SpanFromContext(ctx)
	if span == nil {
		return nil
	}

	sc := span.SpanContext()
	return &SpanContext{
		TraceID:    sc.TraceID().String(),
		SpanID:     sc.SpanID().String(),
		TraceFlags: byte(sc.TraceFlags()),
		TraceState: sc.TraceState().String(),
	}
}

// TraceableFunc wraps a function with tracing.
type TraceableFunc func(ctx context.Context) error

// WithTracing wraps a function with automatic span creation.
func (t *Tracer) WithTracing(name string, fn TraceableFunc) TraceableFunc {
	return func(ctx context.Context) error {
		ctx, span := t.StartSpan(ctx, name)
		defer span.End()

		err := fn(ctx)
		if err != nil {
			t.RecordError(ctx, err)
		}

		return err
	}
}

// Middleware creates HTTP middleware for tracing.
func (t *Tracer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract trace context from headers
		ctx := t.ExtractContext(r.Context(), propagation.HeaderCarrier(r.Header))

		// Start span
		ctx, span := t.StartHTTPSpan(ctx, r.Method, r.URL.Path)
		defer span.End()

		// Add request attributes
		span.SetAttributes(
			attribute.String("http.host", r.Host),
			attribute.String("http.scheme", r.URL.Scheme),
			attribute.String("http.user_agent", r.UserAgent()),
		)

		// Call next handler
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TracerRegistry manages multiple tracers.
type TracerRegistry struct {
	tracers map[string]*Tracer
	mu      sync.RWMutex
}

// NewTracerRegistry creates a new tracer registry.
func NewTracerRegistry() *TracerRegistry {
	return &TracerRegistry{
		tracers: make(map[string]*Tracer),
	}
}

// Register registers a tracer.
func (r *TracerRegistry) Register(name string, tracer *Tracer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.tracers[name] = tracer
}

// Get retrieves a tracer by name.
func (r *TracerRegistry) Get(name string) (*Tracer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tracer, exists := r.tracers[name]
	if !exists {
		return nil, fmt.Errorf("tracer %s not found", name)
	}

	return tracer, nil
}

// ShutdownAll shuts down all tracers.
func (r *TracerRegistry) ShutdownAll(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, tracer := range r.tracers {
		if err := tracer.Shutdown(ctx); err != nil {
			return err
		}
	}

	return nil
}

import (
	"net/http"
	"sync"
)
