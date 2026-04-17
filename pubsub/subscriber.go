package pubsub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/amir-yaghoubi/mqttpattern"
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// VinculumTopicFunc resolves the vinculum topic for an inbound Redis message.
// The matched channel, payload, and extracted pattern fields are passed in.
// Returning "" signals "use the channel as the vinculum topic verbatim".
type VinculumTopicFunc func(channel string, msg any, fields map[string]string) (string, error)

// ChannelSubscription maps one Redis channel (exact or glob-pattern) to a
// vinculum topic. Patterns containing Redis glob metacharacters (*, ?, [)
// use PSUBSCRIBE; all others use SUBSCRIBE.
type ChannelSubscription struct {
	// Channel is the Redis channel name or pattern.
	Channel string

	// VinculumTopicFunc resolves the vinculum topic per message. nil means
	// pass the channel name through unchanged.
	VinculumTopicFunc VinculumTopicFunc
}

// IsPattern reports whether the channel contains Redis glob metacharacters
// (as understood by PSUBSCRIBE).
func (s ChannelSubscription) IsPattern() bool {
	return strings.ContainsAny(s.Channel, "*?[")
}

// RedisPubSubSubscriber owns a *redis.PubSub connection and a goroutine
// that delivers received channel messages to a bus.Subscriber.
type RedisPubSubSubscriber struct {
	name          string
	client        goredis.UniversalClient
	subscriptions  []ChannelSubscription
	target         bus.Subscriber
	wireFormat     wire.WireFormat
	logger         *zap.Logger
	metrics        *pubsubMetrics
	tracerProvider trace.TracerProvider

	mu      sync.Mutex
	ps      *goredis.PubSub
	cancel  context.CancelFunc
	running sync.WaitGroup
}

// Start opens the PubSub connection, issues SUBSCRIBE/PSUBSCRIBE for the
// configured subscriptions, fires on_connect, and launches the receive loop.
func (s *RedisPubSubSubscriber) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ps != nil {
		return fmt.Errorf("redis_pubsub subscriber %q: already started", s.name)
	}

	exact, patterns := splitSubscriptions(s.subscriptions)

	// go-redis takes the channel list up-front and returns a live *PubSub.
	// If both exact and pattern subscriptions are needed, we open one and
	// add the other via PSubscribe/Subscribe on the same connection.
	var ps *goredis.PubSub
	switch {
	case len(exact) > 0 && len(patterns) > 0:
		ps = s.client.Subscribe(ctx, exact...)
		if err := ps.PSubscribe(ctx, patterns...); err != nil {
			_ = ps.Close()
			return fmt.Errorf("redis_pubsub subscriber %q: psubscribe: %w", s.name, err)
		}
	case len(patterns) > 0:
		ps = s.client.PSubscribe(ctx, patterns...)
	default:
		ps = s.client.Subscribe(ctx, exact...)
	}

	// Receive() blocks until the server acknowledges the first subscription
	// (or errors). This gives us a single, reliable "connected" signal.
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return fmt.Errorf("redis_pubsub subscriber %q: subscribe confirm: %w", s.name, err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	s.ps = ps
	s.cancel = cancel

	s.metrics.AddConnected(ctx, 1)

	s.running.Add(1)
	go s.run(runCtx, ps)

	return nil
}

func (s *RedisPubSubSubscriber) Stop() error {
	s.mu.Lock()
	ps := s.ps
	cancel := s.cancel
	s.ps = nil
	s.cancel = nil
	s.mu.Unlock()
	if ps == nil {
		return nil
	}
	cancel()
	_ = ps.Close()
	s.running.Wait()
	s.metrics.AddConnected(context.Background(), -1)
	return nil
}

func (s *RedisPubSubSubscriber) run(ctx context.Context, ps *goredis.PubSub) {
	defer s.running.Done()
	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if err := s.deliver(ctx, msg); err != nil {
				s.logger.Warn("redis_pubsub: deliver failed",
					zap.String("subscriber", s.name),
					zap.String("channel", msg.Channel),
					zap.Error(err))
			}
		}
	}
}

func (s *RedisPubSubSubscriber) deliver(ctx context.Context, msg *goredis.Message) error {
	matchedPattern, fields := s.extractFields(msg)
	var payload any
	if msg.Payload != "" {
		var deserErr error
		payload, deserErr = s.wireFormat.Deserialize([]byte(msg.Payload))
		if deserErr != nil {
			s.logger.Warn("redis_pubsub subscriber: deserialize failed, passing raw bytes",
				zap.String("channel", msg.Channel),
				zap.Error(deserErr))
			payload = []byte(msg.Payload)
		}
	}

	topic := msg.Channel
	if fn := s.topicFuncFor(msg, matchedPattern); fn != nil {
		out, err := fn(msg.Channel, payload, fields)
		if err != nil {
			return fmt.Errorf("vinculum_topic: %w", err)
		}
		if out != "" {
			topic = out
		}
	}

	// Pub/sub has no header mechanism for trace propagation and the spec
	// explicitly opts out, so we start a fresh root consumer span per
	// delivered message rather than linking to a producer context.
	tp := s.tracerProvider
	if tp == nil {
		tp = noop.NewTracerProvider()
	}
	ctx, span := tp.Tracer("github.com/tsarna/vinculum-redis/pubsub").
		Start(ctx, "process "+msg.Channel,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithNewRoot(),
			trace.WithAttributes(
				attribute.String("messaging.system", "redis"),
				attribute.String("messaging.destination.name", msg.Channel),
				attribute.String("messaging.operation.name", "process"),
			),
		)
	defer span.End()

	start := time.Now()
	err := s.target.OnEvent(ctx, topic, payload, fields)
	s.metrics.RecordProcessDuration(ctx, msg.Channel, time.Since(start).Seconds())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.metrics.RecordError(ctx, "process", "deliver")
		return err
	}
	s.metrics.RecordConsumed(ctx, msg.Channel)
	return nil
}

// topicFuncFor finds the VinculumTopicFunc for the subscription that this
// message came from. Exact-channel subscriptions match by msg.Channel;
// pattern subscriptions match by msg.Pattern.
func (s *RedisPubSubSubscriber) topicFuncFor(msg *goredis.Message, matchedPattern string) VinculumTopicFunc {
	for _, sub := range s.subscriptions {
		if sub.IsPattern() {
			if sub.Channel == matchedPattern {
				return sub.VinculumTopicFunc
			}
		} else if sub.Channel == msg.Channel {
			return sub.VinculumTopicFunc
		}
	}
	return nil
}

// extractFields returns the pattern that matched (for PSUBSCRIBE messages;
// empty for exact subscriptions) and any named-capture fields extracted by
// interpreting the Redis pattern as an mqttpattern.
func (s *RedisPubSubSubscriber) extractFields(msg *goredis.Message) (string, map[string]string) {
	if msg.Pattern == "" {
		return "", nil
	}
	// Redis glob → mqttpattern: only the named-capture form `+name` needs
	// translation. `*` in Redis is the segment-wildcard analogue of MQTT's
	// `+`, but without a capture name we leave it alone and return no
	// fields — users who want captures write `+name` in the pattern
	// directly and accept that those patterns pass through to Redis as-is
	// (matching `+name` literally in channel names, which is fine because
	// they are not doing that).
	if strings.Contains(msg.Pattern, "+") {
		fields := mqttpattern.Extract(msg.Pattern, msg.Channel)
		if len(fields) == 0 {
			return msg.Pattern, nil
		}
		return msg.Pattern, fields
	}
	return msg.Pattern, nil
}

// splitSubscriptions partitions subscriptions into exact channels (for
// SUBSCRIBE) and patterns (for PSUBSCRIBE).
func splitSubscriptions(subs []ChannelSubscription) (exact, patterns []string) {
	for _, s := range subs {
		if s.IsPattern() {
			patterns = append(patterns, s.Channel)
		} else {
			exact = append(exact, s.Channel)
		}
	}
	return
}

