package pubsub

import (
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
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

// WithTracerProvider attaches an OTel TracerProvider.
func (b *SubscriberBuilder) WithTracerProvider(tp trace.TracerProvider) *SubscriberBuilder {
	b.tracerProvider = tp
	return b
}

func (b *SubscriberBuilder) Build() *RedisPubSubSubscriber {
	return &RedisPubSubSubscriber{
		name:           b.name,
		client:         b.client,
		subscriptions:  b.subscriptions,
		target:         b.target,
		logger:         b.logger,
		metrics:        newPubsubMetrics(b.clientName, b.meterProvider),
		tracerProvider: b.tracerProvider,
	}
}
