package stream

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// streamMetrics holds the OTel messaging-semconv instruments shared by the
// stream producer and consumer. All writes carry
// vinculum.client.name = <block>.
type streamMetrics struct {
	sent         metric.Int64Counter
	consumed     metric.Int64Counter
	errors       metric.Int64Counter
	reclaimed    metric.Int64Counter
	deadLettered metric.Int64Counter
	opDuration   metric.Float64Histogram
	procDur      metric.Float64Histogram
	connected    metric.Int64UpDownCounter
	pending      metric.Int64UpDownCounter
	clientTag    attribute.KeyValue
}

func newStreamMetrics(clientName string, mp metric.MeterProvider) *streamMetrics {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	m := mp.Meter("github.com/tsarna/vinculum-redis/stream")
	sent, _ := m.Int64Counter("messaging.client.sent.messages",
		metric.WithDescription("XADD count"))
	consumed, _ := m.Int64Counter("messaging.client.consumed.messages",
		metric.WithDescription("XREADGROUP entries delivered"))
	errors, _ := m.Int64Counter("vinculum.messaging.errors",
		metric.WithDescription("Errors during produce/consume, labeled by error.type and operation"))
	reclaimed, _ := m.Int64Counter("vinculum.messaging.stream.reclaimed",
		metric.WithDescription("Pending entries claimed on startup"))
	deadLettered, _ := m.Int64Counter("vinculum.messaging.stream.dead_lettered",
		metric.WithDescription("Entries moved to the dead-letter stream"))
	opDur, _ := m.Float64Histogram("messaging.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("XADD/XREADGROUP round-trip duration, seconds"))
	procDur, _ := m.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Consumer action/delivery duration, seconds"))
	connected, _ := m.Int64UpDownCounter("vinculum.messaging.connected",
		metric.WithDescription("1 while a consumer poll loop is running, 0 otherwise"))
	pending, _ := m.Int64UpDownCounter("vinculum.messaging.stream.pending",
		metric.WithDescription("Unacked entries currently owned by this consumer"))
	return &streamMetrics{
		sent:         sent,
		consumed:     consumed,
		errors:       errors,
		reclaimed:    reclaimed,
		deadLettered: deadLettered,
		opDuration:   opDur,
		procDur:      procDur,
		connected:    connected,
		pending:      pending,
		clientTag:    attribute.String("vinculum.client.name", clientName),
	}
}

func (m *streamMetrics) RecordSent(ctx context.Context, stream string) {
	m.sent.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.operation.name", "publish"),
		m.clientTag,
	))
}

func (m *streamMetrics) RecordConsumed(ctx context.Context, stream, group, consumer string) {
	m.consumed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.consumer.group.name", group),
		attribute.String("messaging.destination.subscription.name", consumer),
		attribute.String("messaging.operation.name", "receive"),
		m.clientTag,
	))
}

func (m *streamMetrics) RecordError(ctx context.Context, operation, errType string) {
	m.errors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.operation.name", operation),
		attribute.String("error.type", errType),
		m.clientTag,
	))
}

func (m *streamMetrics) RecordReclaimed(ctx context.Context, stream, group string, n int64) {
	if n <= 0 {
		return
	}
	m.reclaimed.Add(ctx, n, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.consumer.group.name", group),
		m.clientTag,
	))
}

func (m *streamMetrics) RecordDeadLettered(ctx context.Context, stream, dlq string) {
	m.deadLettered.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("vinculum.dead_letter.stream", dlq),
		m.clientTag,
	))
}

func (m *streamMetrics) RecordPublishDuration(ctx context.Context, stream string, seconds float64) {
	m.opDuration.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.operation.name", "publish"),
		m.clientTag,
	))
}

func (m *streamMetrics) RecordReceiveDuration(ctx context.Context, stream string, seconds float64) {
	m.opDuration.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.operation.name", "receive"),
		m.clientTag,
	))
}

func (m *streamMetrics) RecordProcessDuration(ctx context.Context, stream, group string, seconds float64) {
	m.procDur.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.consumer.group.name", group),
		attribute.String("messaging.operation.name", "process"),
		m.clientTag,
	))
}

func (m *streamMetrics) AddConnected(ctx context.Context, stream, group string, delta int64) {
	m.connected.Add(ctx, delta, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.consumer.group.name", group),
		m.clientTag,
	))
}

func (m *streamMetrics) AddPending(ctx context.Context, stream, group string, delta int64) {
	m.pending.Add(ctx, delta, metric.WithAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.destination.name", stream),
		attribute.String("messaging.consumer.group.name", group),
		m.clientTag,
	))
}
