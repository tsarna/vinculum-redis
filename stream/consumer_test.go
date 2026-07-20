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
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type recorder struct {
	bus.BaseSubscriber
	mu     sync.Mutex
	events []rec
}
type rec struct {
	ctx    context.Context
	topic  string
	msg    any
	fields map[string]string
}

func (r *recorder) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, rec{ctx: ctx, topic: topic, msg: msg, fields: fields})
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

func TestProducerInjectsTraceContext(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	// Install the W3C propagator so Inject writes traceparent.
	prevProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(prevProp)

	// Start a span so there is a valid context to inject.
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	ctx, span := tp.Tracer("test").Start(context.Background(), "outer")
	defer span.End()

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(ctx, "x", "hi", nil))

	entries, err := c.XRange(context.Background(), "events", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	tpHeader, ok := entries[0].Values["traceparent"].(string)
	require.True(t, ok, "expected traceparent field in entry values")
	assert.Contains(t, tpHeader, span.SpanContext().TraceID().String())
}

func TestConsumerCarriesInboundBaggage(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	prevProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	defer otel.SetTextMapPropagator(prevProp)

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

	// Produce an entry with baggage set on the context; the producer injects it
	// as an entry field via the global propagator.
	m0, _ := baggage.NewMemberRaw("tenant_id", "acme")
	m1, _ := baggage.NewMemberRaw("secret", "x")
	bg, _ := baggage.New(m0, m1)
	ctx := baggage.ContextWithBaggage(context.Background(), bg)

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(ctx, "x", "hi", nil))

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)
	// The producer's baggage must reach the consumer's OnEvent context (it
	// previously did not — only traceparent/tracestate were extracted).
	got := baggage.FromContext(evs[0].ctx)
	assert.Equal(t, "acme", got.Member("tenant_id").Value())
	assert.Equal(t, "x", got.Member("secret").Value())
	// And it must not leak into the business fields map.
	assert.NotContains(t, evs[0].fields, "baggage")
}

func TestConsumerReclaimPending(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	// Set up group so the consumer can read as a member.
	require.NoError(t, c.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: "events", Values: map[string]any{"data": "seed"},
	}).Err())
	require.NoError(t, c.XGroupCreate(context.Background(), "events", "g", "0").Err())

	// Consumer A reads (no ack) so the entry is pending under A.
	entriesA, err := c.XReadGroup(context.Background(), &goredis.XReadGroupArgs{
		Group: "g", Consumer: "A",
		Streams: []string{"events", ">"},
		Count:   1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, entriesA, 1)
	require.Len(t, entriesA[0].Messages, 1)

	// Consumer B starts with reclaim_pending + zero min_idle → it should
	// steal the pending entry and redeliver it.
	recv := &recorder{}
	cons := stream.NewConsumer("B", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("B").
		WithBlockTimeout(100 * time.Millisecond).
		WithReclaimPending(true).
		WithReclaimMinIdle(0).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)
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

// ── strict decode ─────────────────────────────────────────────────────────────

func TestConsumerDecodeErrorIsFatalAndLeavesPending(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &recorder{}
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithWireFormat(wire.JSON).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	require.NoError(t, c.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: "events",
		Values: map[string]any{"data": "not json {{"},
	}).Err())

	// Give the consumer time to poll and fail the entry.
	require.Eventually(t, func() bool {
		p, err := c.XPending(context.Background(), "events", "g").Result()
		return err == nil && p.Count == 1
	}, 3*time.Second, 20*time.Millisecond,
		"the entry must stay in the PEL, not be ACKed")

	assert.Empty(t, recv.events, "malformed entry must not be delivered")
}

func TestConsumerDecodeErrorInvokesHook(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &recorder{}
	hookCh := make(chan wire.DecodeError, 4)

	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithWireFormat(wire.JSON).
		WithTarget(recv).
		WithDecodeErrorHook(func(_ context.Context, e wire.DecodeError) {
			select {
			case hookCh <- e:
			default: // the entry stays pending and may be retried; keep the first
			}
		}).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	require.NoError(t, c.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: "events",
		Values: map[string]any{"data": "not json {{"},
	}).Err())

	select {
	case got := <-hookCh:
		assert.Equal(t, []byte("not json {{"), got.Raw)
		assert.Equal(t, "json", got.Format)
		assert.Equal(t, "events", got.Topic)
		assert.Equal(t, "events", got.Attrs["stream"])
		assert.Equal(t, "g", got.Attrs["group"])
		assert.Equal(t, "c", got.Attrs["consumer"])
		assert.NotEmpty(t, got.Attrs["entry_id"])
		require.Error(t, got.Err)
	case <-time.After(3 * time.Second):
		t.Fatal("on_decode_error hook was not invoked")
	}

	// The hook observes; it does not suppress.
	assert.Empty(t, recv.events)
}

func TestConsumerAutoWireFormatToleratesNonJSON(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer c.Close()

	recv := &recorder{}
	// "auto" is the documented migration path off the old tolerant
	// behavior: it never fails to decode, yielding a string.
	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(100 * time.Millisecond).
		WithWireFormat(wire.Auto).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	defer cons.Stop()

	require.NoError(t, c.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: "events",
		Values: map[string]any{"data": "not json {{"},
	}).Err())

	evs := recv.wait(t, 1)
	require.Len(t, evs, 1)
	assert.Equal(t, "not json {{", evs[0].msg)
}
