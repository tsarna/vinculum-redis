package stream

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
)

// entrySettleOps settles one stream entry. A consumer builds one per delivery
// and puts the settler it wraps on the delivery's context, so anything
// downstream — past transforms, an async queue, and any number of bus hops —
// can acknowledge the entry without knowing it came from Redis.
type entrySettleOps struct {
	consumer *RedisStreamConsumer
	stream   string
	id       string
}

// Ack issues XACK, which is what removes the entry from this group's pending
// entries list.
func (o *entrySettleOps) Ack(ctx context.Context) error {
	return o.consumer.ackEntry(ctx, o.stream, o.id)
}

// Nack sends nothing. A Redis Streams entry is not-acknowledged by remaining in
// the pending entries list, where the consumer's own reclaim_min_idle and
// dead_letter_after decide what becomes of it — which is the receiver's
// configured policy, and deliberately not the caller's choice.
//
// The reason therefore reaches the log and nowhere else. Annotating the
// dead-letter entry with it would mean holding the reason until some later poll
// finds the retry budget exhausted, possibly in a different process, which is a
// durability problem out of proportion to an advisory string.
func (o *entrySettleOps) Nack(_ context.Context, reason string) error {
	o.consumer.logger.Info("redis_stream consumer: entry nacked, left pending",
		zap.String("consumer", o.consumer.name),
		zap.String("stream", o.stream),
		zap.String("id", o.id),
		zap.String("reason", reason))
	return nil
}

// Keepalive re-claims the entry for this same consumer, which resets its idle
// time. That is the lease Redis Streams has: an entry idle longer than a
// consumer's reclaim_min_idle is fair game for another consumer to claim, so
// resetting the clock is what says "still working on it".
//
// XCLAIM reports which entries it actually claimed, and an entry already
// acknowledged or trimmed away is not among them — so an empty result is an
// honest "nothing was extended" rather than an error.
func (o *entrySettleOps) Keepalive(ctx context.Context) (bool, error) {
	claimed, err := o.consumer.client.XClaimJustID(ctx, &goredis.XClaimArgs{
		Stream:   o.stream,
		Group:    o.consumer.group,
		Consumer: o.consumer.consumerName,
		MinIdle:  0,
		Messages: []string{o.id},
	}).Result()
	if err != nil {
		return false, fmt.Errorf("redis_stream consumer %q: xclaim %s: %w", o.consumer.name, o.id, err)
	}
	return len(claimed) > 0, nil
}

// Valid always reports true: a Redis entry ID identifies the entry itself, so a
// late XACK cannot acknowledge the wrong one. It is idempotent, and against an
// entry another consumer has since claimed it is a harmless no-op.
//
// This is the one place the protocols genuinely differ. An AMQP delivery tag is
// channel-scoped and re-pointed by a reconnect, so using a stale one there
// acknowledges a *different* message; an SQS receipt handle expires with its
// visibility window. Redis has no equivalent hazard, and asking XPENDING
// whether the entry is still ours would be a round trip per settle bought with
// nothing.
func (o *entrySettleOps) Valid() (bool, string) { return true, "" }

// newSettler returns the settler for one delivery of entry id on stream.
//
// Under auto_ack the settler is marked as settled by the framework, which is
// the same boolean this consumer has always carried and a different thing to do
// with it. It used to mean "acknowledge once delivery returns", which is exact
// only while delivery is synchronous — a queue or a bus hop downstream returns
// as soon as the message is enqueued. Now it means "whoever finishes the work
// settles this", and the acknowledgement follows the work however many hops
// away it happens.
func (c *RedisStreamConsumer) newSettler(stream, id string) bus.Settler {
	ops := &entrySettleOps{consumer: c, stream: stream, id: id}
	if c.autoAck {
		return bus.NewSettler(ops, bus.AutoSettle())
	}
	return bus.NewSettler(ops)
}

// ackEntry is the one XACK in this package: the manual path, the automatic
// path, and the exported Ack all reach the broker through here.
func (c *RedisStreamConsumer) ackEntry(ctx context.Context, stream, id string) error {
	if err := c.client.XAck(ctx, stream, c.group, id).Err(); err != nil {
		return fmt.Errorf("redis_stream consumer %q: xack %s: %w", c.name, id, err)
	}
	c.metrics.AddPending(ctx, stream, c.group, -1)
	return nil
}
