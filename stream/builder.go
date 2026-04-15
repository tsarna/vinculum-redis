package stream

import (
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ProducerBuilder constructs a RedisStreamProducer.
type ProducerBuilder struct {
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
}

// NewProducer returns a builder with sensible defaults: error on
// unresolvable streams, approximate MAXLEN, and the spec's standard
// field names (data/topic/datacontenttype) in flat mode.
func NewProducer(name string, client goredis.UniversalClient) *ProducerBuilder {
	return &ProducerBuilder{
		name:              name,
		client:            client,
		defaultXform:      DefaultStreamError,
		approximateMaxLen: true,
		payloadField:      "data",
		topicField:        "topic",
		contentTypeField:  "datacontenttype",
		fieldsMode:        FieldsFlat,
		logger:            zap.NewNop(),
	}
}

// WithStreamFunc sets the per-message stream-name resolver. nil means the
// default transform (vinculum topic with "/" → ":") is used.
func (b *ProducerBuilder) WithStreamFunc(fn StreamFunc) *ProducerBuilder {
	b.streamFunc = fn
	return b
}

func (b *ProducerBuilder) WithDefaultTransform(t DefaultStreamTransform) *ProducerBuilder {
	b.defaultXform = t
	return b
}

func (b *ProducerBuilder) WithMaxLen(n int64) *ProducerBuilder {
	b.maxLen = n
	return b
}

func (b *ProducerBuilder) WithApproximateMaxLen(approx bool) *ProducerBuilder {
	b.approximateMaxLen = approx
	return b
}

// WithPayloadField overrides the default "data" entry field. Empty string
// suppresses writing the payload field.
func (b *ProducerBuilder) WithPayloadField(name string) *ProducerBuilder {
	b.payloadField = name
	return b
}

// WithTopicField overrides the default "topic" entry field. Empty string
// suppresses it.
func (b *ProducerBuilder) WithTopicField(name string) *ProducerBuilder {
	b.topicField = name
	return b
}

// WithContentTypeField overrides the default "datacontenttype" entry field.
// Empty string suppresses it.
func (b *ProducerBuilder) WithContentTypeField(name string) *ProducerBuilder {
	b.contentTypeField = name
	return b
}

func (b *ProducerBuilder) WithFieldsMode(m FieldsMode) *ProducerBuilder {
	b.fieldsMode = m
	return b
}

func (b *ProducerBuilder) WithLogger(l *zap.Logger) *ProducerBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

func (b *ProducerBuilder) Build() *RedisStreamProducer {
	return &RedisStreamProducer{
		name:              b.name,
		client:            b.client,
		streamFunc:        b.streamFunc,
		defaultXform:      b.defaultXform,
		maxLen:            b.maxLen,
		approximateMaxLen: b.approximateMaxLen,
		payloadField:      b.payloadField,
		topicField:        b.topicField,
		contentTypeField:  b.contentTypeField,
		fieldsMode:        b.fieldsMode,
		logger:            b.logger,
	}
}
