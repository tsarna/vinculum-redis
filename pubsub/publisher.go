package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amir-yaghoubi/mqttpattern"
	goredis "github.com/redis/go-redis/v9"
	"github.com/tsarna/go2cty2go"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/zclconf/go-cty/cty"
	"go.uber.org/zap"
)

// DefaultChannelTransform controls what happens when no channel_mapping
// matches and no block-level channel_transform is provided.
type DefaultChannelTransform int

const (
	// DefaultChannelVerbatim uses the vinculum topic as the Redis channel.
	DefaultChannelVerbatim DefaultChannelTransform = iota

	// DefaultChannelError returns an error from OnEvent.
	DefaultChannelError

	// DefaultChannelIgnore silently drops the event.
	DefaultChannelIgnore
)

// ChannelFunc resolves the Redis channel for an outbound event. The caller
// merges pattern-extracted fields into fields before invocation. Returning
// an empty string signals "use the vinculum topic verbatim".
type ChannelFunc func(topic string, msg any, fields map[string]string) (string, error)

// ChannelMapping remaps a vinculum MQTT-style topic pattern to a Redis channel.
type ChannelMapping struct {
	// Pattern is a vinculum-topic pattern (supports + and # wildcards with
	// optional capture names, e.g. "device/+deviceId/status").
	Pattern string

	// ChannelFunc resolves the Redis channel per message. nil means use the
	// vinculum topic verbatim.
	ChannelFunc ChannelFunc
}

// RedisPubSubPublisher implements bus.Subscriber by publishing received
// vinculum events to a Redis channel via PUBLISH.
type RedisPubSubPublisher struct {
	bus.BaseSubscriber

	name         string
	client       goredis.UniversalClient
	mappings     []ChannelMapping
	xformFunc    ChannelFunc
	defaultXform DefaultChannelTransform
	logger       *zap.Logger
	metrics      *pubsubMetrics
}

// OnEvent resolves the target Redis channel, serializes the payload, and
// issues a PUBLISH.
func (p *RedisPubSubPublisher) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	channel, ok, err := p.resolveChannel(topic, msg, fields)
	if err != nil {
		return fmt.Errorf("redis_pubsub publisher %q: %w", p.name, err)
	}
	if !ok {
		return nil
	}

	payload, err := serializePayload(msg)
	if err != nil {
		return fmt.Errorf("redis_pubsub publisher %q: serialize: %w", p.name, err)
	}

	if err := p.client.Publish(ctx, channel, payload).Err(); err != nil {
		p.metrics.RecordError(ctx, "publish", "publish")
		return fmt.Errorf("redis_pubsub publisher %q: publish to %q: %w", p.name, channel, err)
	}
	p.metrics.RecordSent(ctx, channel)
	return nil
}

// resolveChannel applies channel_mapping (first match wins), then the
// block-level channel_transform, then the default_channel_transform. The
// bool return distinguishes "drop silently" from "empty result".
func (p *RedisPubSubPublisher) resolveChannel(topic string, msg any, fields map[string]string) (string, bool, error) {
	for _, m := range p.mappings {
		if !mqttpattern.Matches(m.Pattern, topic) {
			continue
		}
		merged := mergeFields(fields, mqttpattern.Extract(m.Pattern, topic))
		if m.ChannelFunc == nil {
			return topic, true, nil
		}
		ch, err := m.ChannelFunc(topic, msg, merged)
		if err != nil {
			return "", false, err
		}
		if ch == "" {
			ch = topic
		}
		return ch, true, nil
	}

	if p.xformFunc != nil {
		ch, err := p.xformFunc(topic, msg, fields)
		if err != nil {
			return "", false, err
		}
		if ch == "" {
			ch = topic
		}
		return ch, true, nil
	}

	switch p.defaultXform {
	case DefaultChannelIgnore:
		return "", false, nil
	case DefaultChannelError:
		return "", false, fmt.Errorf("no channel_mapping matched for topic %q and default_channel_transform is error", topic)
	default:
		return topic, true, nil
	}
}

func mergeFields(a, b map[string]string) map[string]string {
	if len(b) == 0 {
		return a
	}
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// serializePayload mirrors vinculum-mqtt/publisher.serializePayload:
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

var _ bus.Subscriber = (*RedisPubSubPublisher)(nil)
