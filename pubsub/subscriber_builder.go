package pubsub

import (
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// SubscriberBuilder constructs a RedisPubSubSubscriber.
type SubscriberBuilder struct {
	name           string
	clientName     string
	client         goredis.UniversalClient
	subscriptions  []ChannelSubscription
	target         bus.Subscriber
	wireFormat     wire.WireFormat
	onDecodeError  wire.DecodeErrorHook
	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
}

func NewSubscriber(name string, client goredis.UniversalClient) *SubscriberBuilder {
	return &SubscriberBuilder{
		name:   name,
		client: client,
		logger: zap.NewNop(),
	}
}

func (b *SubscriberBuilder) WithSubscription(s ChannelSubscription) *SubscriberBuilder {
	b.subscriptions = append(b.subscriptions, s)
	return b
}

func (b *SubscriberBuilder) WithTarget(t bus.Subscriber) *SubscriberBuilder {
	b.target = t
	return b
}

func (b *SubscriberBuilder) WithLogger(l *zap.Logger) *SubscriberBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

func (b *SubscriberBuilder) WithMeterProvider(mp metric.MeterProvider) *SubscriberBuilder {
	b.meterProvider = mp
	return b
}

// WithClientName sets the vinculum client block name used as a metric label.
func (b *SubscriberBuilder) WithClientName(name string) *SubscriberBuilder {
	b.clientName = name
	return b
}

// WithWireFormat sets the wire format used to deserialize inbound payloads.
func (b *SubscriberBuilder) WithWireFormat(f wire.WireFormat) *SubscriberBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
func (b *SubscriberBuilder) WithWireFormatName(name string) *SubscriberBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithDecodeErrorHook sets an observer invoked when an inbound payload fails
// to deserialize. The hook cannot suppress the failure: the message is
// dropped either way. nil (the default) means no observer.
func (b *SubscriberBuilder) WithDecodeErrorHook(h wire.DecodeErrorHook) *SubscriberBuilder {
	b.onDecodeError = h
	return b
}

// WithTracerProvider attaches an OTel TracerProvider.
func (b *SubscriberBuilder) WithTracerProvider(tp trace.TracerProvider) *SubscriberBuilder {
	b.tracerProvider = tp
	return b
}

func (b *SubscriberBuilder) Build() *RedisPubSubSubscriber {
	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}
	return &RedisPubSubSubscriber{
		name:           b.name,
		client:         b.client,
		subscriptions:  b.subscriptions,
		target:         b.target,
		wireFormat:     wf,
		onDecodeError:  b.onDecodeError,
		logger:         b.logger,
		metrics:        newPubsubMetrics(b.clientName, b.meterProvider),
		tracerProvider: b.tracerProvider,
	}
}
