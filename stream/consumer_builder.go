package stream

import (
	"time"

	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ConsumerBuilder constructs a RedisStreamConsumer.
type ConsumerBuilder struct {
	name             string
	clientName       string
	client           goredis.UniversalClient
	streamName       string
	group            string
	consumerName     string
	batchSize        int64
	blockTimeout     time.Duration
	autoAck          bool
	groupCreate      GroupCreatePolicy
	target           bus.Subscriber
	topicFunc        VinculumTopicFromStreamFunc
	payloadField     string
	topicField       string
	contentTypeField string
	fieldsMode       FieldsMode
	wireFormat       wire.WireFormat
	reclaimPending   bool
	reclaimMinIdle   time.Duration
	deadLetterStream string
	deadLetterAfter  int64
	logger           *zap.Logger
	meterProvider    metric.MeterProvider
	tracerProvider   trace.TracerProvider
}

// NewConsumer returns a builder with the spec defaults: batch_size=10,
// block_timeout=2s, auto_ack=true, group_create=create_if_missing, and
// symmetric field-name defaults matching the producer.
func NewConsumer(name string, client goredis.UniversalClient) *ConsumerBuilder {
	return &ConsumerBuilder{
		name:             name,
		client:           client,
		batchSize:        10,
		blockTimeout:     2 * time.Second,
		autoAck:          true,
		groupCreate:      GroupCreateIfMissing,
		payloadField:     "data",
		topicField:       "topic",
		contentTypeField: "datacontenttype",
		fieldsMode:       FieldsFlat,
		reclaimPending:   true,
		reclaimMinIdle:   5 * time.Minute,
		logger:           zap.NewNop(),
	}
}

func (b *ConsumerBuilder) WithReclaimPending(v bool) *ConsumerBuilder {
	b.reclaimPending = v
	return b
}

func (b *ConsumerBuilder) WithReclaimMinIdle(d time.Duration) *ConsumerBuilder {
	b.reclaimMinIdle = d
	return b
}

func (b *ConsumerBuilder) WithDeadLetterStream(s string) *ConsumerBuilder {
	b.deadLetterStream = s
	return b
}

func (b *ConsumerBuilder) WithDeadLetterAfter(n int64) *ConsumerBuilder {
	b.deadLetterAfter = n
	return b
}

func (b *ConsumerBuilder) WithStream(s string) *ConsumerBuilder {
	b.streamName = s
	return b
}

func (b *ConsumerBuilder) WithGroup(g string) *ConsumerBuilder {
	b.group = g
	return b
}

func (b *ConsumerBuilder) WithConsumerName(n string) *ConsumerBuilder {
	b.consumerName = n
	return b
}

func (b *ConsumerBuilder) WithBatchSize(n int64) *ConsumerBuilder {
	if n > 0 {
		b.batchSize = n
	}
	return b
}

func (b *ConsumerBuilder) WithBlockTimeout(d time.Duration) *ConsumerBuilder {
	b.blockTimeout = d
	return b
}

func (b *ConsumerBuilder) WithAutoAck(v bool) *ConsumerBuilder {
	b.autoAck = v
	return b
}

func (b *ConsumerBuilder) WithGroupCreatePolicy(p GroupCreatePolicy) *ConsumerBuilder {
	b.groupCreate = p
	return b
}

func (b *ConsumerBuilder) WithTarget(t bus.Subscriber) *ConsumerBuilder {
	b.target = t
	return b
}

func (b *ConsumerBuilder) WithTopicFunc(fn VinculumTopicFromStreamFunc) *ConsumerBuilder {
	b.topicFunc = fn
	return b
}

func (b *ConsumerBuilder) WithPayloadField(s string) *ConsumerBuilder {
	b.payloadField = s
	return b
}

func (b *ConsumerBuilder) WithTopicField(s string) *ConsumerBuilder {
	b.topicField = s
	return b
}

func (b *ConsumerBuilder) WithContentTypeField(s string) *ConsumerBuilder {
	b.contentTypeField = s
	return b
}

// WithWireFormat sets the wire format used to deserialize inbound payloads.
func (b *ConsumerBuilder) WithWireFormat(f wire.WireFormat) *ConsumerBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
func (b *ConsumerBuilder) WithWireFormatName(name string) *ConsumerBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

func (b *ConsumerBuilder) WithFieldsMode(m FieldsMode) *ConsumerBuilder {
	b.fieldsMode = m
	return b
}

func (b *ConsumerBuilder) WithLogger(l *zap.Logger) *ConsumerBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

func (b *ConsumerBuilder) WithMeterProvider(mp metric.MeterProvider) *ConsumerBuilder {
	b.meterProvider = mp
	return b
}

func (b *ConsumerBuilder) WithTracerProvider(tp trace.TracerProvider) *ConsumerBuilder {
	b.tracerProvider = tp
	return b
}

// WithClientName sets the vinculum client block name used as a metric label.
func (b *ConsumerBuilder) WithClientName(name string) *ConsumerBuilder {
	b.clientName = name
	return b
}

func (b *ConsumerBuilder) Build() *RedisStreamConsumer {
	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}
	return &RedisStreamConsumer{
		name:             b.name,
		client:           b.client,
		streamName:       b.streamName,
		group:            b.group,
		consumerName:     b.consumerName,
		batchSize:        b.batchSize,
		blockTimeout:     b.blockTimeout,
		autoAck:          b.autoAck,
		groupCreate:      b.groupCreate,
		target:           b.target,
		topicFunc:        b.topicFunc,
		payloadField:     b.payloadField,
		topicField:       b.topicField,
		contentTypeField: b.contentTypeField,
		fieldsMode:       b.fieldsMode,
		wireFormat:       wf,
		reclaimPending:   b.reclaimPending,
		reclaimMinIdle:   b.reclaimMinIdle,
		deadLetterStream: b.deadLetterStream,
		deadLetterAfter:  b.deadLetterAfter,
		logger:           b.logger,
		metrics:          newStreamMetrics(b.clientName, b.meterProvider),
		tracerProvider:   b.tracerProvider,
	}
}
