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
	"github.com/tsarna/vinculum-redis/pubsub"
)

func newClient(t *testing.T) (*miniredis.Miniredis, goredis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

// subscribe starts a miniredis subscriber and returns a channel receiving
// received payloads until the test's deadline.
func subscribe(t *testing.T, c goredis.UniversalClient, channels ...string) (<-chan *goredis.Message, func()) {
	t.Helper()
	ps := c.Subscribe(context.Background(), channels...)
	// Drain the initial confirmation before returning so the caller sees only real messages.
	_, err := ps.Receive(context.Background())
	require.NoError(t, err)
	out := make(chan *goredis.Message, 16)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range ps.Channel() {
			out <- msg
		}
	}()
	stop := func() {
		_ = ps.Close()
		wg.Wait()
		close(out)
	}
	return out, stop
}

func recvOne(t *testing.T, ch <-chan *goredis.Message) *goredis.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func TestPublishVerbatim(t *testing.T) {
	_, c := newClient(t)
	recv, stop := subscribe(t, c, "alerts/fire")
	defer stop()

	p := pubsub.NewPublisher("main", c).Build()
	require.NoError(t, p.OnEvent(context.Background(), "alerts/fire", map[string]any{"x": 1}, nil))

	m := recvOne(t, recv)
	assert.Equal(t, "alerts/fire", m.Channel)
	assert.JSONEq(t, `{"x":1}`, m.Payload)
}

func TestPublishBytesPassthrough(t *testing.T) {
	_, c := newClient(t)
	recv, stop := subscribe(t, c, "raw")
	defer stop()

	p := pubsub.NewPublisher("main", c).Build()
	require.NoError(t, p.OnEvent(context.Background(), "raw", []byte{0x00, 'h', 'i'}, nil))

	m := recvOne(t, recv)
	assert.Equal(t, "\x00hi", m.Payload)
}

func TestChannelMappingStatic(t *testing.T) {
	_, c := newClient(t)
	recv, stop := subscribe(t, c, "alerts")
	defer stop()

	// Static channel override: every alerts/# message lands on "alerts".
	p := pubsub.NewPublisher("main", c).
		WithChannelMapping(pubsub.ChannelMapping{
			Pattern: "alerts/#",
			ChannelFunc: func(_ string, _ any, _ map[string]string) (string, error) {
				return "alerts", nil
			},
		}).Build()

	require.NoError(t, p.OnEvent(context.Background(), "alerts/fire/urgent", "hi", nil))
	m := recvOne(t, recv)
	assert.Equal(t, "alerts", m.Channel)
}

func TestChannelMappingDynamicFromPatternFields(t *testing.T) {
	_, c := newClient(t)
	recv, stop := subscribe(t, c, "devices.abc.status")
	defer stop()

	p := pubsub.NewPublisher("main", c).
		WithChannelMapping(pubsub.ChannelMapping{
			Pattern: "device/+deviceId/status",
			ChannelFunc: func(_ string, _ any, fields map[string]string) (string, error) {
				return "devices." + fields["deviceId"] + ".status", nil
			},
		}).Build()

	require.NoError(t, p.OnEvent(context.Background(), "device/abc/status", "up", nil))
	m := recvOne(t, recv)
	assert.Equal(t, "devices.abc.status", m.Channel)
}

func TestDefaultTransformIgnore(t *testing.T) {
	mr, c := newClient(t)
	p := pubsub.NewPublisher("main", c).
		WithDefaultTransform(pubsub.DefaultChannelIgnore).Build()

	// No mapping, ignore → no error and no PUBLISH recorded.
	require.NoError(t, p.OnEvent(context.Background(), "unmatched", "x", nil))
	// miniredis records PUBLISH counts via PubSubNumSub; easier: subscribe and timeout.
	recv := c.Subscribe(context.Background(), "unmatched")
	defer recv.Close()
	_, _ = recv.Receive(context.Background())
	select {
	case <-recv.Channel():
		t.Fatal("expected no message under ignore")
	case <-time.After(50 * time.Millisecond):
	}
	_ = mr
}

func TestDefaultTransformError(t *testing.T) {
	_, c := newClient(t)
	p := pubsub.NewPublisher("main", c).
		WithDefaultTransform(pubsub.DefaultChannelError).Build()
	err := p.OnEvent(context.Background(), "no/match", "x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no channel_mapping matched")
}

func TestChannelTransformFallback(t *testing.T) {
	_, c := newClient(t)
	recv, stop := subscribe(t, c, "a.b.c")
	defer stop()

	// Block-level transform: "/" → "."
	p := pubsub.NewPublisher("main", c).
		WithChannelTransform(func(topic string, _ any, _ map[string]string) (string, error) {
			out := make([]byte, 0, len(topic))
			for _, r := range topic {
				if r == '/' {
					out = append(out, '.')
				} else {
					out = append(out, byte(r))
				}
			}
			return string(out), nil
		}).Build()

	require.NoError(t, p.OnEvent(context.Background(), "a/b/c", "hi", nil))
	m := recvOne(t, recv)
	assert.Equal(t, "a.b.c", m.Channel)
}
