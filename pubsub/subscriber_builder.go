package pubsub

import (
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
)

// SubscriberBuilder constructs a RedisPubSubSubscriber.
type SubscriberBuilder struct {
	name          string
	client        goredis.UniversalClient
	subscriptions []ChannelSubscription
	target        bus.Subscriber
	logger        *zap.Logger
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

func (b *SubscriberBuilder) Build() *RedisPubSubSubscriber {
	return &RedisPubSubSubscriber{
		name:          b.name,
		client:        b.client,
		subscriptions: b.subscriptions,
		target:        b.target,
		logger:        b.logger,
	}
}
