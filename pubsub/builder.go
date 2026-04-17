package pubsub

import (
	goredis "github.com/redis/go-redis/v9"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// PublisherBuilder constructs a RedisPubSubPublisher.
type PublisherBuilder struct {
	name           string
	clientName     string
	client         goredis.UniversalClient
	mappings       []ChannelMapping
	xformFunc      ChannelFunc
	defaultXform   DefaultChannelTransform
	wireFormat     wire.WireFormat
	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
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

// WithWireFormat sets the wire format used to serialize outbound payloads.
func (b *PublisherBuilder) WithWireFormat(f wire.WireFormat) *PublisherBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
func (b *PublisherBuilder) WithWireFormatName(name string) *PublisherBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithLogger sets the logger.
func (b *PublisherBuilder) WithLogger(l *zap.Logger) *PublisherBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// WithMeterProvider attaches an OTel MeterProvider. nil disables metrics.
func (b *PublisherBuilder) WithMeterProvider(mp metric.MeterProvider) *PublisherBuilder {
	b.meterProvider = mp
	return b
}

// WithClientName sets the vinculum client block name used as a metric
// label (vinculum.client.name). Empty is fine if metrics aren't wired.
func (b *PublisherBuilder) WithClientName(name string) *PublisherBuilder {
	b.clientName = name
	return b
}

// WithTracerProvider attaches an OTel TracerProvider. nil uses the global
// provider via trace/noop fallback.
func (b *PublisherBuilder) WithTracerProvider(tp trace.TracerProvider) *PublisherBuilder {
	b.tracerProvider = tp
	return b
}

// Build returns a RedisPubSubPublisher ready to accept OnEvent calls.
func (b *PublisherBuilder) Build() *RedisPubSubPublisher {
	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}
	return &RedisPubSubPublisher{
		name:           b.name,
		client:         b.client,
		mappings:       b.mappings,
		xformFunc:      b.xformFunc,
		defaultXform:   b.defaultXform,
		wireFormat:     wf,
		logger:         b.logger,
		metrics:        newPubsubMetrics(b.clientName, b.meterProvider),
		tracerProvider: b.tracerProvider,
	}
}
