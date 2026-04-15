package pubsub_test

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
	"github.com/tsarna/vinculum-redis/pubsub"
)

// recordingSub is a trivial bus.Subscriber that buffers received events.
type recordingSub struct {
	bus.BaseSubscriber
	mu     sync.Mutex
	events []event
}

type event struct {
	topic  string
	msg    any
	fields map[string]string
}

func (r *recordingSub) OnEvent(_ context.Context, topic string, msg any, fields map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event{topic: topic, msg: msg, fields: fields})
	return nil
}

func (r *recordingSub) wait(t *testing.T, n int) []event {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		got := len(r.events)
		r.mu.Unlock()
		if got >= n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]event, len(r.events))
	copy(out, r.events)
	return out
}

func newSubClient(t *testing.T) (*miniredis.Miniredis, goredis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

func TestSubscriberExactChannel(t *testing.T) {
	_, c := newSubClient(t)
	rec := &recordingSub{}

	sub := pubsub.NewSubscriber("main", c).
		WithSubscription(pubsub.ChannelSubscription{Channel: "alerts"}).
		WithTarget(rec).
		Build()

	require.NoError(t, sub.Start(context.Background()))
	defer sub.Stop()

	require.NoError(t, c.Publish(context.Background(), "alerts", `{"level":"high"}`).Err())

	evs := rec.wait(t, 1)
	require.Len(t, evs, 1)
	assert.Equal(t, "alerts", evs[0].topic)
	assert.Equal(t, map[string]any{"level": "high"}, evs[0].msg)
}

func TestSubscriberPattern(t *testing.T) {
	_, c := newSubClient(t)
	rec := &recordingSub{}

	sub := pubsub.NewSubscriber("main", c).
		WithSubscription(pubsub.ChannelSubscription{Channel: "devices.*"}).
		WithTarget(rec).
		Build()

	require.NoError(t, sub.Start(context.Background()))
	defer sub.Stop()

	require.NoError(t, c.Publish(context.Background(), "devices.abc", "up").Err())
	require.NoError(t, c.Publish(context.Background(), "devices.xyz", "down").Err())

	evs := rec.wait(t, 2)
	require.Len(t, evs, 2)
	// Topic defaults to matched channel (not the pattern).
	topics := []string{evs[0].topic, evs[1].topic}
	assert.Contains(t, topics, "devices.abc")
	assert.Contains(t, topics, "devices.xyz")
}

func TestSubscriberVinculumTopicRemap(t *testing.T) {
	_, c := newSubClient(t)
	rec := &recordingSub{}

	sub := pubsub.NewSubscriber("main", c).
		WithSubscription(pubsub.ChannelSubscription{
			Channel: "alerts",
			VinculumTopicFunc: func(_ string, _ any, _ map[string]string) (string, error) {
				return "alerts/redis", nil
			},
		}).
		WithTarget(rec).
		Build()

	require.NoError(t, sub.Start(context.Background()))
	defer sub.Stop()

	require.NoError(t, c.Publish(context.Background(), "alerts", "hi").Err())

	evs := rec.wait(t, 1)
	require.Len(t, evs, 1)
	assert.Equal(t, "alerts/redis", evs[0].topic)
}

func TestSubscriberMixedExactAndPattern(t *testing.T) {
	_, c := newSubClient(t)
	rec := &recordingSub{}

	sub := pubsub.NewSubscriber("main", c).
		WithSubscription(pubsub.ChannelSubscription{Channel: "exact"}).
		WithSubscription(pubsub.ChannelSubscription{Channel: "pat.*"}).
		WithTarget(rec).
		Build()

	require.NoError(t, sub.Start(context.Background()))
	defer sub.Stop()

	require.NoError(t, c.Publish(context.Background(), "exact", "e").Err())
	require.NoError(t, c.Publish(context.Background(), "pat.xyz", "p").Err())

	evs := rec.wait(t, 2)
	require.Len(t, evs, 2)
	topics := []string{evs[0].topic, evs[1].topic}
	assert.Contains(t, topics, "exact")
	assert.Contains(t, topics, "pat.xyz")
}
