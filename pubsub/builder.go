package pubsub

import (
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// PublisherBuilder constructs a RedisPubSubPublisher.
type PublisherBuilder struct {
	name         string
	client       goredis.UniversalClient
	mappings     []ChannelMapping
	xformFunc    ChannelFunc
	defaultXform DefaultChannelTransform
	logger       *zap.Logger
}

// NewPublisher returns a PublisherBuilder with default_channel_transform=verbatim.
func NewPublisher(name string, client goredis.UniversalClient) *PublisherBuilder {
	return &PublisherBuilder{
		name:         name,
		client:       client,
		defaultXform: DefaultChannelVerbatim,
		logger:       zap.NewNop(),
	}
}

// WithChannelMapping appends a channel mapping. Mappings are evaluated in
// order; first match wins.
func (b *PublisherBuilder) WithChannelMapping(m ChannelMapping) *PublisherBuilder {
	b.mappings = append(b.mappings, m)
	return b
}

// WithChannelTransform sets a block-level expression applied when no
// channel_mapping matches. When set, this takes precedence over
// default_channel_transform.
func (b *PublisherBuilder) WithChannelTransform(fn ChannelFunc) *PublisherBuilder {
	b.xformFunc = fn
	return b
}

// WithDefaultTransform sets the final fallback behavior when neither a
// channel_mapping nor a channel_transform applies.
func (b *PublisherBuilder) WithDefaultTransform(t DefaultChannelTransform) *PublisherBuilder {
	b.defaultXform = t
	return b
}

// WithLogger sets the logger.
func (b *PublisherBuilder) WithLogger(l *zap.Logger) *PublisherBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// Build returns a RedisPubSubPublisher ready to accept OnEvent calls.
func (b *PublisherBuilder) Build() *RedisPubSubPublisher {
	return &RedisPubSubPublisher{
		name:         b.name,
		client:       b.client,
		mappings:     b.mappings,
		xformFunc:    b.xformFunc,
		defaultXform: b.defaultXform,
		logger:       b.logger,
	}
}
