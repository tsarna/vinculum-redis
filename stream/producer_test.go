package stream_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tsarna/vinculum-redis/stream"
)

func newClient(t *testing.T) (*miniredis.Miniredis, goredis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

// readAll fetches every entry on a stream via XRANGE.
func readAll(t *testing.T, c goredis.UniversalClient, streamName string) []goredis.XMessage {
	t.Helper()
	res, err := c.XRange(context.Background(), streamName, "-", "+").Result()
	require.NoError(t, err)
	return res
}

func TestProducerDefaultStreamName(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).Build()
	require.NoError(t, p.OnEvent(context.Background(), "events/foo/bar", map[string]any{"k": 1}, nil))

	entries := readAll(t, c, "events:foo:bar")
	require.Len(t, entries, 1)
	assert.JSONEq(t, `{"k":1}`, string(entries[0].Values["data"].(string)))
	assert.Equal(t, "events/foo/bar", entries[0].Values["topic"])
	assert.Equal(t, "application/json", entries[0].Values["datacontenttype"])
}

func TestProducerExplicitStream(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithStreamFunc(func(_ string, _ any, _ map[string]string) (string, error) {
			return "events", nil
		}).Build()

	require.NoError(t, p.OnEvent(context.Background(), "anything", "hi", nil))
	entries := readAll(t, c, "events")
	require.Len(t, entries, 1)
}

func TestProducerDynamicStreamFromFields(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithStreamFunc(func(_ string, _ any, fields map[string]string) (string, error) {
			return "events:" + fields["region"], nil
		}).Build()

	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", map[string]string{"region": "us-east"}))
	entries := readAll(t, c, "events:us-east")
	require.Len(t, entries, 1)
	// region was written as a sibling field by default (flat mode).
	assert.Equal(t, "us-east", entries[0].Values["region"])
}

func TestProducerFieldsModeNested(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithFieldsMode(stream.FieldsNested).Build()

	require.NoError(t, p.OnEvent(context.Background(), "x", "hi",
		map[string]string{"region": "us-east", "host": "h1"}))
	entries := readAll(t, c, "x")
	require.Len(t, entries, 1)

	raw, ok := entries[0].Values["fields"].(string)
	require.True(t, ok)
	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Equal(t, map[string]string{"region": "us-east", "host": "h1"}, got)
}

func TestProducerFieldsModeOmit(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithFieldsMode(stream.FieldsOmit).Build()

	require.NoError(t, p.OnEvent(context.Background(), "x", "hi",
		map[string]string{"region": "us-east"}))
	entries := readAll(t, c, "x")
	require.Len(t, entries, 1)
	_, has := entries[0].Values["region"]
	assert.False(t, has)
	_, has = entries[0].Values["fields"]
	assert.False(t, has)
}

func TestProducerReservedFieldDropped(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).Build()

	// User attempts to write `data` via fields → reserved wins.
	require.NoError(t, p.OnEvent(context.Background(), "x", map[string]any{"real": 1},
		map[string]string{"data": "hijack", "ok": "yes"}))
	entries := readAll(t, c, "x")
	require.Len(t, entries, 1)
	assert.JSONEq(t, `{"real":1}`, entries[0].Values["data"].(string))
	assert.Equal(t, "yes", entries[0].Values["ok"])
}

func TestProducerMaxLen(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithMaxLen(3).Build()

	for i := 0; i < 10; i++ {
		require.NoError(t, p.OnEvent(context.Background(), "x", i, nil))
	}
	n, err := c.XLen(context.Background(), "x").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 3, n)
}

func TestProducerCustomFieldNames(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithPayloadField("body").
		WithTopicField("").
		WithContentTypeField("").Build()

	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", nil))
	entries := readAll(t, c, "x")
	require.Len(t, entries, 1)
	// auto format passes strings through verbatim (not JSON-encoded)
	assert.Equal(t, "hi", entries[0].Values["body"].(string))
	_, hasTopic := entries[0].Values["topic"]
	_, hasCT := entries[0].Values["datacontenttype"]
	assert.False(t, hasTopic)
	assert.False(t, hasCT)
}

func TestProducerStreamErrorIgnore(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithStreamFunc(func(_ string, _ any, _ map[string]string) (string, error) {
			return "", assert.AnError
		}).
		WithDefaultTransform(stream.DefaultStreamIgnore).Build()

	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", nil))
}

func TestProducerStreamErrorPropagated(t *testing.T) {
	_, c := newClient(t)

	p := stream.NewProducer("out", c).
		WithStreamFunc(func(_ string, _ any, _ map[string]string) (string, error) {
			return "", assert.AnError
		}).Build()

	err := p.OnEvent(context.Background(), "x", "hi", nil)
	assert.Error(t, err)
}
