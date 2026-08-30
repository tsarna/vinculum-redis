package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-redis/stream"
)

// settleFixture starts a consumer over miniredis with the given options and
// returns the redis client, the recorder, and a producer that writes one entry
// onto the stream the consumer reads.
func settleFixture(t *testing.T, autoAck bool) (*goredis.Client, *recorder, func(payload any)) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	recv := &recorder{}
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithAutoAck(autoAck).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	t.Cleanup(func() { _ = cons.Stop() })

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()

	return c, recv, func(payload any) {
		require.NoError(t, p.OnEvent(context.Background(), "x", payload, nil))
	}
}

func pendingCount(t *testing.T, c *goredis.Client) int64 {
	t.Helper()
	pending, err := c.XPending(context.Background(), "events", "g").Result()
	require.NoError(t, err)
	return pending.Count
}

// The delivery carries its own acknowledgement. Nothing in `fields` says how to
// settle the entry and nothing needs to: the settler rides the context, so a
// subscriber that never learns it is reading Redis can still acknowledge what
// it handled.
func TestDeliveryCarriesASettler(t *testing.T) {
	c, recv, produce := settleFixture(t, false)
	produce("hi")

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)
	require.EqualValues(t, 1, pendingCount(t, c), "the entry should be pending before anything settles it")

	settler := bus.SettlerFromContext(evs[0].ctx)
	require.NotNil(t, settler, "the delivery context should carry a settler")

	settled, err := settler.Ack(context.Background())
	require.NoError(t, err)
	assert.True(t, settled)
	assert.EqualValues(t, 0, pendingCount(t, c), "acking through the settler should clear the entry")
}

// Nacking sends nothing to Redis: an entry is not-acknowledged by remaining in
// the pending entries list, where reclaim_min_idle and dead_letter_after decide
// what becomes of it.
func TestNackLeavesTheEntryPending(t *testing.T) {
	c, recv, produce := settleFixture(t, false)
	produce("hi")

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)

	settled, err := bus.SettlerFromContext(evs[0].ctx).Nack(context.Background(), "schema rejected it")
	require.NoError(t, err)
	assert.True(t, settled)
	assert.EqualValues(t, 1, pendingCount(t, c), "a nacked entry stays pending for the receiver's own policy")
}

// Keepalive re-claims the entry for the same consumer, resetting its idle time
// — the lease Redis Streams has. It reports what XCLAIM actually claimed, so an
// entry already acknowledged is an honest "nothing extended" rather than an
// error.
func TestKeepaliveReclaimsForTheSameConsumer(t *testing.T) {
	c, recv, produce := settleFixture(t, false)
	produce("hi")

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)
	settler := bus.SettlerFromContext(evs[0].ctx)

	extended, err := settler.Keepalive(context.Background())
	require.NoError(t, err)
	assert.True(t, extended, "a pending entry should be extendable")

	pending, err := c.XPendingExt(context.Background(), &goredis.XPendingExtArgs{
		Stream: "events", Group: "g", Start: "-", End: "+", Count: 10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "c", pending[0].Consumer, "the entry should still be held by this consumer")

	settled, err := settler.Ack(context.Background())
	require.NoError(t, err)
	require.True(t, settled)

	extended, err = settler.Keepalive(context.Background())
	require.NoError(t, err)
	assert.False(t, extended, "there is no lease left to extend once the entry is acknowledged")
}

// acker acknowledges every delivery through the settler on its context, the way
// a manual-settle configuration does.
type acker struct {
	recorder
	settled []bool
}

func (a *acker) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	settled, err := bus.SettlerFromContext(ctx).Ack(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.settled = append(a.settled, settled)
	a.mu.Unlock()
	return a.recorder.OnEvent(ctx, topic, msg, fields)
}

// Automatic acknowledgement runs through the same settler, so a configuration
// that acknowledged the entry itself while auto_ack was on does not acknowledge
// it twice. Auto is a policy over the one mechanism rather than a second path
// to the broker.
func TestAutoAckDoesNotSettleWhatTheSubscriberAlreadySettled(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &acker{}
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithAutoAck(true).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", nil))

	recv.wait(t, 1)

	recv.mu.Lock()
	settled := append([]bool(nil), recv.settled...)
	recv.mu.Unlock()
	require.Equal(t, []bool{true}, settled, "the subscriber should have been the one that settled it")

	assert.EqualValues(t, 0, pendingCount(t, c))
}
