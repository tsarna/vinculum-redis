# vinculum-redis

Redis/Valkey client packages for [vinculum](https://github.com/tsarna/vinculum),
implemented using [go-redis/v9](https://github.com/redis/go-redis). Works
identically against Redis and Valkey; both speak RESP3 and go-redis handles
standalone, cluster, and sentinel topologies transparently.

Three usage modes are provided as separate packages, each meaningfully
distinct in its semantics:

| Package | Redis commands | Analogue |
| --- | --- | --- |
| `pubsub` | `PUBLISH` / `SUBSCRIBE` / `PSUBSCRIBE` | MQTT-style fire-and-forget |
| `stream` | `XADD` / `XREADGROUP` / `XACK` / `XCLAIM` | Kafka-style persistent log |

Key-value operations (`GET`/`SET`/`INCR`/`HGET`/`HSET`) are implemented directly
in the vinculum main repo (`clients/rediskv`) because they fit the generic
`get()`/`set()`/`increment()` interface and need no messaging-bus glue.

## Packages

### `pubsub`

`RedisPubSubPublisher` implements `bus.Subscriber`. It serializes vinculum
events and publishes them to Redis channels.

- Per-pattern channel mappings (first match wins); MQTT-style `+name`
  captures extract segments into `fields` for the channel expression.
- Block-level `channel_transform` fallback for unmatched topics, plus a
  terminal `default_channel_transform` of `verbatim`, `ignore`, or `error`.
- Payload serialization: `cty.Value` → `go2cty2go` → JSON; `[]byte`
  passthrough; other → JSON (matches `vinculum-mqtt/publisher`).
- Metrics: messages sent, errors, publish duration.
- Tracing: `SpanKindProducer` span per `PUBLISH`.

```go
p := pubsub.NewPublisher("main", redisClient).
    WithChannelMapping(pubsub.ChannelMapping{
        Pattern: "device/+deviceId/status",
        ChannelFunc: func(_ string, _ any, fields map[string]string) (string, error) {
            return "devices." + fields["deviceId"] + ".status", nil
        },
    }).
    Build()
```

`RedisPubSubSubscriber` owns a `*redis.PubSub` connection, issues
`SUBSCRIBE` for exact channels and `PSUBSCRIBE` for glob patterns on the
same connection, and delivers each message to a `bus.Subscriber`.

- Reconnect re-subscribe handled transparently by go-redis.
- Optional `VinculumTopicFunc` for per-message topic remap.
- Deserialization: valid JSON → decoded `any`; otherwise raw `[]byte`.
- Metrics: messages consumed, errors, process duration, connected gauge.
- Tracing: fresh-root `SpanKindConsumer` span per delivered message (no
  propagation — Redis pub/sub has no header mechanism).

```go
s := pubsub.NewSubscriber("main", redisClient).
    WithSubscription(pubsub.ChannelSubscription{Channel: "alerts"}).
    WithSubscription(pubsub.ChannelSubscription{Channel: "devices.*"}).
    WithTarget(myBusSubscriber).
    Build()
if err := s.Start(ctx); err != nil { ... }
defer s.Stop()
```

### `stream`

`RedisStreamProducer` implements `bus.Subscriber` via `XADD`.

- Stream name resolved per-message (`StreamFunc`) or defaulted from the
  vinculum topic with `/` → `:` substitution.
- Optional `MAXLEN` trimming, approximate by default.
- Entry layout: `data` (JSON payload), `topic` (origin), `datacontenttype`
  (`application/json`), plus the vinculum `fields` map in `flat`, `nested`,
  or `omit` mode. Field names are configurable.
- Reserved field names (`data`, `topic`, `datacontenttype`, `fields`,
  `traceparent`, `tracestate`) always win over user-supplied fields.
- Trace context: producer injects `traceparent`/`tracestate` into entry
  fields via the OTel text-map propagator.
- Metrics: XADD count, errors, publish duration.

```go
p := stream.NewProducer("out", redisClient).
    WithStreamFunc(func(_ string, _ any, fields map[string]string) (string, error) {
        return "events:" + fields["region"], nil
    }).
    WithMaxLen(10000).
    Build()
```

`RedisStreamConsumer` reads one stream via `XREADGROUP`, delivers entries
to a `bus.Subscriber`, and acknowledges them.

- Consumer group policies: `create_if_missing` (default, `MKSTREAM`),
  `require_existing`, `create_from_start` (replay history from ID `0`).
- `auto_ack = true` (default) issues `XACK` after successful delivery;
  `auto_ack = false` leaves entries in the PEL for manual `Ack(ctx, id)`.
- Reclaim-on-Start: walks the group's pending list and `XCLAIM`s entries
  idle past `reclaim_min_idle`, then runs them through the delivery path
  (`XREADGROUP >` will not redeliver already-claimed entries).
- Dead-letter: after a delivery failure, when `RetryCount >= dead_letter_after`,
  copy the entry to a DLQ stream (with `_dlq_original_stream` /
  `_dlq_original_id` markers) and `XACK` the original.
- Trace context: extracts `traceparent`/`tracestate`; consumer spans are
  fresh roots with the producer context attached as a **link** (per OTel
  messaging guidance for persistent-log consumers — a stream entry may be
  processed minutes or hours after it was produced).
- Metrics: consumed, errors, receive duration, process duration, connected
  gauge, pending gauge, reclaimed, dead-lettered.

```go
c := stream.NewConsumer("in", redisClient).
    WithStream("events").
    WithGroup("workers").
    WithConsumerName("worker-1").
    WithBlockTimeout(2 * time.Second).
    WithTarget(myBusSubscriber).
    Build()
if err := c.Start(ctx); err != nil { ... }
defer c.Stop()
```

## VCL configuration

When used via vinculum, these packages are wired by `client "redis"`,
`client "redis_pubsub"`, and `client "redis_stream"` blocks. The
`client "redis"` block is a passive connection manager shared by the
others:

```hcl
client "redis" "myredis" {
    address  = "localhost:6379"
    password = env.REDIS_PASSWORD
}

client "redis_pubsub" "rps" {
    connection = client.myredis
    publisher "main" {}
    subscriber "in" {
        subscriber = bus.main
        channel_subscription { channel = "alerts" }
        channel_subscription { channel = "devices.*" }
    }
}

client "redis_stream" "rs" {
    connection = client.myredis

    producer "out" {
        stream = "events:${ctx.fields.region}"
        maxlen = 10000
    }

    consumer "in" {
        stream        = "events"
        group         = "workers"
        block_timeout = "2s"
        subscriber    = bus.main
    }
}
```

The full block surface — cluster and sentinel modes, TLS, manual ack via
`redis_ack()`, and the `redis_kv` key-value client (which lives in the
vinculum main repo) — is documented alongside the other vinculum client
blocks.

## License

BSD 2-Clause. See [LICENSE](LICENSE).
