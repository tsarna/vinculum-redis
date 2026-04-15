package pubsub

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// pubsubMetrics holds the OTel messaging-semconv instruments shared by the
// pub/sub publisher and subscriber. All writes carry
// messaging.system = "redis" and vinculum.client.name = <block>.
type pubsubMetrics struct {
	sent       metric.Int64Counter
	consumed   metric.Int64Counter
	errors     metric.Int64Counter
	opDuration metric.Float64Histogram
	procDur    metric.Float64Histogram
	connected  metric.Int64UpDownCounter
	clientTag  attribute.KeyValue
}

func newPubsubMetrics(clientName string, mp metric.MeterProvider) *pubsubMetrics {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	m := mp.Meter("github.com/tsarna/vinculum-redis/pubsub")
	sent, _ := m.Int64Counter("messaging.client.sent.messages",
		metric.WithDescription("Number of Redis channel messages successfully published"))
	consumed, _ := m.Int64Counter("messaging.client.consumed.messages",
		metric.WithDescription("Number of Redis channel messages delivered to a subscriber"))
	errors, _ := m.Int64Counter("vinculum.messaging.errors",
		metric.WithDescription("Errors during publish/receive, labeled by error.type and operation"))
	opDur, _ := m.Float64Histogram("messaging.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("PUBLISH/receive round-trip duration, seconds"))
	procDur, _ := m.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Subscriber action/delivery duration, seconds"))
	connected, _ := m.Int64UpDownCounter("vinculum.messaging.connected",
		metric.WithDescription("1 while a subscriber connection is live, 0 otherwise"))
	return &pubsubMetrics{
		sent:       sent,
		consumed:   consumed,
		errors:     errors,
		opDuration: opDur,
		procDur:    procDur,
		connected:  connected,
		clientTag:  attribute.String("vinculum.client.name", clientName),
	}
}

func (m *pubsubMetrics) RecordSent(ctx context.Context, channel string) {
	m.sent.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", channel),
		attribute.String("messaging.operation.name", "publish"),
		m.clientTag,
	))
}

func (m *pubsubMetrics) RecordConsumed(ctx context.Context, channel string) {
	m.consumed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", channel),
		attribute.String("messaging.operation.name", "receive"),
		m.clientTag,
	))
}

func (m *pubsubMetrics) RecordError(ctx context.Context, operation, errType string) {
	m.errors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.operation.name", operation),
		attribute.String("error.type", errType),
		m.clientTag,
	))
}

func (m *pubsubMetrics) RecordPublishDuration(ctx context.Context, channel string, seconds float64) {
	m.opDuration.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", channel),
		attribute.String("messaging.operation.name", "publish"),
		m.clientTag,
	))
}

func (m *pubsubMetrics) RecordProcessDuration(ctx context.Context, channel string, seconds float64) {
	m.procDur.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", channel),
		attribute.String("messaging.operation.name", "process"),
		m.clientTag,
	))
}

func (m *pubsubMetrics) AddConnected(ctx context.Context, delta int64) {
	m.connected.Add(ctx, delta, metric.WithAttributes(m.clientTag))
}
