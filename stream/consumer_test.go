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
	"github.com/tsarna/vinculum-redis/stream"
)

type recorder struct {
	bus.BaseSubscriber
	mu     sync.Mutex
	events []rec
}
type rec struct {
	topic  string
	msg    any
	fields map[string]string
}

func (r *recorder) OnEvent(_ context.Context, topic string, msg any, fields map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, rec{topic, msg, fields})
	return nil
}
func (r *recorder) wait(t *testing.T, n int) []rec {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
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
	out := make([]rec, len(r.events))
	copy(out, r.events)
	return out
}

func TestConsumerAutoCreateGroupAndRead(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &recorder{}
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g1").
		WithConsumerName("c1").
		WithBlockTimeout(100 * time.Millisecond).
		WithTarget(recv).
		Build()

	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	// Produce an entry via a standalone producer so the consumer reads it.
	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(context.Background(), "t", map[string]any{"k": "v"}, map[string]string{"src": "unit"}))

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)
	assert.Equal(t, map[string]any{"k": "v"}, evs[0].msg)
	assert.Equal(t, "unit", evs[0].fields["src"])
}

func TestConsumerTopicRemap(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &recorder{}
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithTarget(recv).
		WithTopicFunc(func(streamName, id string, _ any, _ map[string]string) (string, error) {
			return "stream/" + streamName, nil
		}).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", nil))

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)
	assert.Equal(t, "stream/events", evs[0].topic)
}

func TestConsumerRequireExistingMissing(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	// Create the stream (but no group) via a single XAdd.
	require.NoError(t, c.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: "events",
		Values: map[string]any{"data": "seed"},
	}).Err())

	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("nope").
		WithConsumerName("c").
		WithGroupCreatePolicy(stream.GroupRequireExisting).
		WithTarget(&recorder{}).
		Build()
	err := cons.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestConsumerAutoAckClearsPending(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &recorder{}
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", nil))

	recv.wait(t, 1)

	// Pending should be empty — auto_ack=true ACKed the entry.
	pending, err := c.XPending(context.Background(), "events", "g").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pending.Count)
}

func TestConsumerAutoAckFalseLeavesPending(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &recorder{}
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithAutoAck(false).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", nil))

	recv.wait(t, 1)

	pending, err := c.XPending(context.Background(), "events", "g").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, pending.Count)
}
