package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
)

// GroupCreatePolicy controls whether Start() creates the consumer group.
type GroupCreatePolicy int

const (
	// GroupCreateIfMissing calls XGROUP CREATE with MKSTREAM starting
	// from "$" (only new entries) if the group does not yet exist.
	GroupCreateIfMissing GroupCreatePolicy = iota

	// GroupRequireExisting fails Start() if the group does not exist.
	GroupRequireExisting

	// GroupCreateFromStart creates the group reading from ID "0" so the
	// consumer replays all historical entries.
	GroupCreateFromStart
)

// VinculumTopicFromStreamFunc resolves the vinculum topic for a stream
// entry. Returning "" passes the stream name through unchanged.
type VinculumTopicFromStreamFunc func(stream string, entryID string, msg any, fields map[string]string) (string, error)

// RedisStreamConsumer reads from a single Redis stream via XREADGROUP and
// delivers each entry onto a bus.Subscriber.
type RedisStreamConsumer struct {
	name             string
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
	logger           *zap.Logger

	mu      sync.Mutex
	cancel  context.CancelFunc
	running sync.WaitGroup
}

// Start creates the group per policy, then launches the poll loop.
func (c *RedisStreamConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return fmt.Errorf("redis_stream consumer %q: already started", c.name)
	}

	if err := c.ensureGroup(ctx); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("redis_stream consumer %q: %w", c.name, err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.mu.Unlock()

	c.running.Add(1)
	go c.runLoop(runCtx)
	return nil
}

func (c *RedisStreamConsumer) Stop() error {
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	c.running.Wait()
	return nil
}

func (c *RedisStreamConsumer) ensureGroup(ctx context.Context) error {
	startID := "$"
	if c.groupCreate == GroupCreateFromStart {
		startID = "0"
	}

	if c.groupCreate == GroupRequireExisting {
		// Use XInfoGroups to verify the group exists.
		groups, err := c.client.XInfoGroups(ctx, c.streamName).Result()
		if err != nil {
			return fmt.Errorf("xinfo groups: %w", err)
		}
		for _, g := range groups {
			if g.Name == c.group {
				return nil
			}
		}
		return fmt.Errorf("consumer group %q does not exist on stream %q", c.group, c.streamName)
	}

	err := c.client.XGroupCreateMkStream(ctx, c.streamName, c.group, startID).Err()
	if err != nil && !isBusyGroupErr(err) {
		return fmt.Errorf("xgroup create %q/%q: %w", c.streamName, c.group, err)
	}
	return nil
}

// isBusyGroupErr matches the BUSYGROUP error returned when the group
// already exists — a no-op for the create-if-missing path.
func isBusyGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func (c *RedisStreamConsumer) runLoop(ctx context.Context) {
	defer c.running.Done()
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumerName,
			Streams:  []string{c.streamName, ">"},
			Count:    c.batchSize,
			Block:    c.blockTimeout,
		}).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if err == goredis.Nil {
				// Block timeout with no entries — just poll again.
				continue
			}
			c.logger.Warn("redis_stream consumer: xreadgroup",
				zap.String("consumer", c.name),
				zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		for _, s := range streams {
			for _, entry := range s.Messages {
				if err := c.deliver(ctx, s.Stream, entry); err != nil {
					c.logger.Warn("redis_stream consumer: deliver",
						zap.String("consumer", c.name),
						zap.String("stream", s.Stream),
						zap.String("id", entry.ID),
						zap.Error(err))
					// auto_ack only on successful delivery.
					continue
				}
				if c.autoAck {
					if err := c.client.XAck(ctx, s.Stream, c.group, entry.ID).Err(); err != nil {
						c.logger.Warn("redis_stream consumer: xack",
							zap.String("consumer", c.name),
							zap.String("stream", s.Stream),
							zap.String("id", entry.ID),
							zap.Error(err))
					}
				}
			}
		}
	}
}

func (c *RedisStreamConsumer) deliver(ctx context.Context, streamName string, entry goredis.XMessage) error {
	msg, fields, err := c.parseEntry(entry)
	if err != nil {
		return err
	}

	topic := streamName
	if c.topicFunc != nil {
		out, err := c.topicFunc(streamName, entry.ID, msg, fields)
		if err != nil {
			return fmt.Errorf("vinculum_topic: %w", err)
		}
		if out != "" {
			topic = out
		}
	}

	return c.target.OnEvent(ctx, topic, msg, fields)
}

// parseEntry extracts payload + fields from a stream entry using the
// symmetric payload_field / topic_field / fields_mode configuration.
func (c *RedisStreamConsumer) parseEntry(entry goredis.XMessage) (any, map[string]string, error) {
	var payload any
	fields := make(map[string]string)

	for k, v := range entry.Values {
		if c.payloadField != "" && k == c.payloadField {
			payload = deserializePayload(asBytes(v))
			continue
		}
		if c.topicField != "" && k == c.topicField {
			continue // not a user field; origin topic is the Vinculum producer's hint
		}
		if c.contentTypeField != "" && k == c.contentTypeField {
			continue
		}
		switch c.fieldsMode {
		case FieldsOmit:
			// drop
		case FieldsNested:
			if k == "fields" {
				if err := json.Unmarshal(asBytes(v), &fields); err != nil {
					return nil, nil, fmt.Errorf("decode fields: %w", err)
				}
			}
		default: // FieldsFlat
			if k == "fields" {
				continue
			}
			fields[k] = asString(v)
		}
	}

	if len(fields) == 0 {
		fields = nil
	}
	return payload, fields, nil
}

// asBytes coerces a stream-entry value to []byte. go-redis returns strings
// by default; callers that passed raw []byte in XAdd get a string back
// when reading (Redis is string-typed on the wire).
func asBytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	default:
		return nil
	}
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}

// deserializePayload mirrors vinculum-mqtt/subscriber.deserializePayload:
// valid JSON → decoded any; anything else → raw bytes.
func deserializePayload(payload []byte) any {
	if payload == nil {
		return nil
	}
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return payload
	}
	return v
}
