package stream_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-bus/subutils"
	"github.com/tsarna/vinculum-redis/stream"
)

// gatedTarget holds the goroutine that delivers to it until released, so a test
// can look at the pending entries list while the work is provably still going.
type gatedTarget struct {
	bus.BaseSubscriber
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	err         error
}

func newGatedTarget() *gatedTarget {
	return &gatedTarget{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (g *gatedTarget) Release() { g.releaseOnce.Do(func() { close(g.release) }) }

func (g *gatedTarget) OnEvent(context.Context, string, any, map[string]string) error {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	return g.err
}

// queuedFixture puts an async queue between the consumer and the target, which
// is what a `queue_size` on the receiver builds. That queue is the reason this
// whole change exists: its OnEvent returns the moment the entry is enqueued.
func queuedFixture(t *testing.T, target bus.Subscriber) (*goredis.Client, func(payload any)) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	queue := subutils.NewAsyncQueueingSubscriber(target, 10).Start()
	t.Cleanup(func() { queue.Close() })

	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithAutoAck(true).
		WithTarget(queue).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	t.Cleanup(func() { _ = cons.Stop() })

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()

	return c, func(payload any) {
		require.NoError(t, p.OnEvent(context.Background(), "x", payload, nil))
	}
}

// pendingNow reports the pending count without failing the test, so it is safe
// to poll from assert.Never and assert.Eventually, which run their condition on
// another goroutine.
func pendingNow(c *goredis.Client) int64 {
	pending, err := c.XPending(context.Background(), "events", "g").Result()
	if err != nil {
		return -1
	}
	return pending.Count
}

// The defect, and the reason `queue_size` alongside `ack = "auto"` used to be
// refused outright. Delivery into the queue returns as soon as the entry is
// enqueued, so acknowledging on that return told Redis the entry was handled
// before anything had handled it — and a handler failure then had nothing left
// to redeliver.
func TestAutoAckWaitsForTheWorkBehindAQueue(t *testing.T) {
	target := newGatedTarget()
	defer target.Release()

	c, produce := queuedFixture(t, target)
	produce("hi")

	<-target.entered

	// Never, not once. Acknowledging on the enqueue's return happens within
	// microseconds of it, so a single check just after the target is entered is
	// a race that the wrong behaviour can win. Holding the assertion open for
	// as long as the target is gated is what makes this deterministic.
	assert.Never(t, func() bool { return pendingNow(c) == 0 },
		250*time.Millisecond, 25*time.Millisecond,
		"the target is still working; the entry must stay pending")

	target.Release()

	assert.Eventually(t, func() bool { return pendingNow(c) == 0 },
		3*time.Second, 20*time.Millisecond,
		"the acknowledgement should follow the work out of the queue")
}

// The half that matters for at-least-once. A handler that fails behind a queue
// leaves the entry pending, so Redis can hand it to another consumer — where
// before it was acknowledged at the enqueue and the failure lost the message.
func TestAFailureBehindAQueueLeavesTheEntryPending(t *testing.T) {
	target := newGatedTarget()
	target.err = assert.AnError
	defer target.Release()

	c, produce := queuedFixture(t, target)
	produce("hi")

	<-target.entered
	target.Release()

	// Give the drain goroutine time to finish and settle, then assert that what
	// it settled was a nack — for Redis, leaving the entry in the PEL.
	time.Sleep(200 * time.Millisecond)
	assert.EqualValues(t, 1, pendingCount(t, c),
		"a failed handler must leave the entry for redelivery, not acknowledge it")
}
