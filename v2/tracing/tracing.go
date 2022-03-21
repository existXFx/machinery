package tracing

import (
	"encoding/json"

	"github.com/RichardKnop/machinery/v2/tasks"

	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// opentracing tags
var (
	defaultTextMapPropagator = propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
)

// StartSpanFromHeaders will extract a span from the signature headers
// and start a new span with the given operation name.
func StartSpanFromHeaders(headers tasks.Headers, operationName string) (context.Context, trace.Span) {
	carrier := propagation.MapCarrier{}
	for k, v := range headers {
		if strValue, ok := v.(string); ok {
			carrier.Set(k, strValue)
		}
	}
	ctx := defaultTextMapPropagator.Extract(context.TODO(), carrier)

	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, operationName)

	return ctx, span
}

func ConstructContextFromHeaders(headers tasks.Headers) context.Context {
	carrier := propagation.MapCarrier{}
	for k, v := range headers {
		if strValue, ok := v.(string); ok {
			carrier.Set(k, strValue)
		}
	}

	return defaultTextMapPropagator.Extract(context.TODO(), carrier)
}

// HeadersWithContext will inject a context into the signature headers
func HeadersWithContext(headers tasks.Headers, ctx context.Context) tasks.Headers {
	// check if the headers aren't nil
	if headers == nil {
		headers = make(tasks.Headers)
	}

	carrier := propagation.MapCarrier{}
	defaultTextMapPropagator.Inject(ctx, carrier)

	for _, k := range carrier.Keys() {
		headers[k] = carrier.Get(k)
	}

	return headers
}

// AnnotateSpanWithSignatureInfo ...
func AnnotateSpanWithSignatureInfo(ctx context.Context, signature *tasks.Signature) {
	span := trace.SpanFromContext(ctx)

	span.SetAttributes(attribute.String("signature.name", signature.Name))
	span.SetAttributes(attribute.String("signature.uuid", signature.UUID))

	if signature.GroupUUID != "" {
		span.SetAttributes(attribute.String("signature.group.uuid", signature.GroupUUID))
	}

	if signature.ChordCallback != nil {
		span.SetAttributes(attribute.String("signature.chord.callback.uuid", signature.ChordCallback.UUID))
		span.SetAttributes(attribute.String("signature.chord.callback.name", signature.ChordCallback.Name))
	}
}

// AnnotateSpanWithChainInfo ...
func AnnotateSpanWithChainInfo(ctx context.Context, chain *tasks.Chain) {
	span := trace.SpanFromContext(ctx)
	// tag the span with some info about the chain
	span.SetAttributes(attribute.Int("chain.tasks.length", len(chain.Tasks)))

	// inject the tracing span into the tasks signature headers
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithContext(signature.Headers, ctx)
	}
}

// AnnotateSpanWithGroupInfo ...
func AnnotateSpanWithGroupInfo(ctx context.Context, group *tasks.Group, sendConcurrency int) {
	span := trace.SpanFromContext(ctx)

	// tag the span with some info about the group
	span.SetAttributes(attribute.String("group.uuid", group.GroupUUID))
	span.SetAttributes(attribute.Int("group.tasks.length", len(group.Tasks)))
	span.SetAttributes(attribute.Int("group.concurrency", sendConcurrency))

	// encode the task uuids to json, if that fails just dump it in
	if taskUUIDs, err := json.Marshal(group.GetUUIDs()); err == nil {
		span.SetAttributes(attribute.String("group.tasks", string(taskUUIDs)))
	} else {
		span.SetAttributes(attribute.StringSlice("group.tasks", group.GetUUIDs()))
	}

	// inject the tracing span into the tasks signature headers
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithContext(signature.Headers, ctx)
	}
}

// AnnotateSpanWithChordInfo ...
func AnnotateSpanWithChordInfo(ctx context.Context, chord *tasks.Chord, sendConcurrency int) {
	span := trace.SpanFromContext(ctx)

	// tag the span with chord specific info
	span.SetAttributes(attribute.String("chord.callback.uuid", chord.Callback.UUID))

	// inject the tracing span into the callback signature
	chord.Callback.Headers = HeadersWithContext(chord.Callback.Headers, ctx)

	// tag the span for the group part of the chord
	AnnotateSpanWithGroupInfo(ctx, chord.Group, sendConcurrency)
}
