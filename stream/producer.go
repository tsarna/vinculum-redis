package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tsarna/go2cty2go"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/zclconf/go-cty/cty"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// DefaultStreamTransform controls what happens when the stream expression
// fails to produce a usable name (empty result or evaluation error).
type DefaultStreamTransform int

const (
	// DefaultStreamError returns the error from OnEvent (default).
	DefaultStreamError DefaultStreamTransform = iota

	// DefaultStreamIgnore silently drops the event.
	DefaultStreamIgnore
)

// FieldsMode controls how the vinculum fields map is written onto the
// stream entry.
type FieldsMode int

const (
	// FieldsFlat writes each entry from the fields map as a sibling field
	// on the stream entry alongside payload/topic/content-type.
	FieldsFlat FieldsMode = iota

	// FieldsNested writes the fields map as a single JSON-encoded entry
	// under the "fields" key.
	FieldsNested

	// FieldsOmit drops the fields map entirely.
	FieldsOmit
)

// StreamFunc resolves the stream name for an outbound event. Returning ""
// signals "use the default transform" (i.e. fall back to the vinculum topic
// with "/" → ":" substitution).
type StreamFunc func(topic string, msg any, fields map[string]string) (string, error)

// RedisStreamProducer implements bus.Subscriber by issuing XADD against a
// stream resolved per message.
type RedisStreamProducer struct {
	bus.BaseSubscriber

	name              string
	client            goredis.UniversalClient
	streamFunc        StreamFunc
	defaultXform      DefaultStreamTransform
	maxLen            int64
	approximateMaxLen bool
	payloadField      string
	topicField        string
	contentTypeField  string
	fieldsMode        FieldsMode
	logger            *zap.Logger
	metrics           *streamMetrics
	tracerProvider    trace.TracerProvider
}

func (p *RedisStreamProducer) tracer() trace.Tracer {
	tp := p.tracerProvider
	if tp == nil {
		tp = noop.NewTracerProvider()
	}
	return tp.Tracer("github.com/tsarna/vinculum-redis/stream")
}

// reservedFields are the entry field names Vinculum writes itself. User
// fields colliding with these (in flat mode) are dropped with a warning;
// the reserved value always wins.
var reservedFields = map[string]struct{}{
	"data":            {},
	"topic":           {},
	"datacontenttype": {},
	"fields":          {},
	"traceparent":     {},
	"tracestate":      {},
}

// IsReservedField reports whether name is a Vinculum-reserved stream entry
// field name. Exposed so config validation can warn at load time.
func IsReservedField(name string) bool {
	_, ok := reservedFields[name]
	return ok
}

// OnEvent serializes the event and issues XADD.
func (p *RedisStreamProducer) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	streamName, ok, err := p.resolveStream(topic, msg, fields)
	if err != nil {
		p.metrics.RecordError(ctx, "publish", "resolve_stream")
		return fmt.Errorf("redis_stream producer %q: %w", p.name, err)
	}
	if !ok {
		return nil
	}

	values, err := p.buildValues(topic, msg, fields)
	if err != nil {
		p.metrics.RecordError(ctx, "publish", "build_entry")
		return fmt.Errorf("redis_stream producer %q: build entry: %w", p.name, err)
	}

	// Inject W3C trace context (traceparent/tracestate) as entry fields.
	// These are part of the reserved-field set so they always win over
	// user-supplied fields.
	carrier := mapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		values[k] = v
	}

	ctx, span := p.tracer().Start(ctx, "send "+streamName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("messaging.destination.name", streamName),
			attribute.String("messaging.operation.name", "publish"),
		),
	)
	defer span.End()

	args := &goredis.XAddArgs{
		Stream: streamName,
		Values: values,
	}
	if p.maxLen > 0 {
		if p.approximateMaxLen {
			args.MaxLen = p.maxLen
			args.Approx = true
		} else {
			args.MaxLen = p.maxLen
		}
	}

	start := time.Now()
	if _, err := p.client.XAdd(ctx, args).Result(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.metrics.RecordError(ctx, "publish", "xadd")
		return fmt.Errorf("redis_stream producer %q: XADD %q: %w", p.name, streamName, err)
	}
	_ = start
	p.metrics.RecordSent(ctx, streamName)
	return nil
}

func (p *RedisStreamProducer) resolveStream(topic string, msg any, fields map[string]string) (string, bool, error) {
	if p.streamFunc != nil {
		s, err := p.streamFunc(topic, msg, fields)
		if err != nil {
			if p.defaultXform == DefaultStreamIgnore {
				return "", false, nil
			}
			return "", false, err
		}
		if s != "" {
			return s, true, nil
		}
		// Empty string from the expression → treat as "no name" and fall
		// through to the default transform.
	}

	switch p.defaultXform {
	case DefaultStreamIgnore:
		return "", false, nil
	default:
		return defaultStreamName(topic), true, nil
	}
}

// defaultStreamName converts a vinculum topic to the conventional Redis
// stream name by replacing slash separators with colons. Empty topic
// becomes "events" so XADD always has a target.
func defaultStreamName(topic string) string {
	if topic == "" {
		return "events"
	}
	return strings.ReplaceAll(topic, "/", ":")
}

func (p *RedisStreamProducer) buildValues(topic string, msg any, fields map[string]string) (map[string]interface{}, error) {
	payload, err := serializePayload(msg)
	if err != nil {
		return nil, err
	}

	values := make(map[string]interface{}, 4+len(fields))
	if p.payloadField != "" {
		values[p.payloadField] = payload
	}
	if p.topicField != "" {
		values[p.topicField] = topic
	}
	if p.contentTypeField != "" {
		values[p.contentTypeField] = "application/json"
	}

	switch p.fieldsMode {
	case FieldsOmit:
		// nothing
	case FieldsNested:
		if len(fields) > 0 {
			b, err := json.Marshal(fields)
			if err != nil {
				return nil, fmt.Errorf("encode fields: %w", err)
			}
			values["fields"] = b
		}
	default: // FieldsFlat
		for k, v := range fields {
			if _, reserved := reservedFields[k]; reserved {
				p.logger.Warn("redis_stream: dropping field that collides with reserved name",
					zap.String("producer", p.name),
					zap.String("field", k))
				continue
			}
			values[k] = v
		}
	}

	return values, nil
}

// serializePayload mirrors the pubsub publisher path:
//
//   - []byte    → pass through unchanged
//   - cty.Value → go2cty2go.CtyToAny → json.Marshal
//   - nil       → nil
//   - any other → json.Marshal
func serializePayload(msg any) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	if val, ok := msg.(cty.Value); ok {
		var err error
		msg, err = go2cty2go.CtyToAny(val)
		if err != nil {
			return nil, fmt.Errorf("cty conversion: %w", err)
		}
	}
	if b, ok := msg.([]byte); ok {
		return b, nil
	}
	return json.Marshal(msg)
}

var _ bus.Subscriber = (*RedisStreamProducer)(nil)
