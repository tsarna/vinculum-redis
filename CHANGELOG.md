# Changelog

## [Unreleased]

## v0.4.0 (2026-07-20)

### Changed

- **BREAKING: deserialize failures are no longer swallowed.** The pub/sub subscriber and
  the stream consumer used to log a warning and pass the **raw bytes** through as the
  message payload when the configured wire format failed to decode. That happened even
  when the caller explicitly configured `wire.JSON`, so there was no way to say "messages
  on this channel must be JSON". A decode failure is now fatal to the message: it never
  reaches the target's `OnEvent`.

  - **Pub/sub**: the message is dropped. Pub/sub has no acknowledgement, so nothing
    accumulates and nothing is redelivered.
  - **Streams**: the entry is **not** `XACK`ed and stays in the pending-entries list.
    Configure `dead_letter_stream` / `dead_letter_after` so a persistently malformed entry
    is moved aside rather than staying pending indefinitely.

  Callers wanting best-effort decoding should use `wire.Auto`, which never fails (it yields
  a `string` for anything it can't parse as JSON). Note that is not an exact replacement:
  the old fallback produced `[]byte`, so a target that type-switches on `[]byte` must be
  adjusted.

- `RedisStreamConsumer.parseEntry` takes a `context.Context` as its first argument, so it
  can record metrics and invoke the decode-error hook. Unexported; no API impact.

- Requires `github.com/tsarna/vinculum-wire` v0.3.0 for the `DecodeError` /
  `DecodeErrorHook` types.

### Added

- `WithDecodeErrorHook(wire.DecodeErrorHook)` on the pub/sub subscriber builder and the
  stream consumer builder. The hook observes a decode failure — it receives the raw
  payload, the error, and the format name, plus the channel and matched pattern (pub/sub)
  or the stream, entry ID, group, and consumer name (streams) — but cannot suppress it.
  nil (the default) means no observer.

  For streams the hook's `Fields` is left nil: fields are accumulated in the same pass
  that decodes the payload, so a partial map would depend on Go's randomized map
  iteration order.

- Deserialize failures are recorded on the error counter with
  `error.type = "deserialize"`. Streams additionally record `error.type = "fields_decode"`
  for a malformed nested `fields` value, which previously failed the entry silently.

## v0.3.1 (2026-06-27)

### Fixed

- **Streams: inbound baggage now reaches `target.OnEvent`.** The stream consumer
  extracted only `traceparent`/`tracestate` from entry fields, dropping the
  `baggage` field the producer already writes, so consumers could not read
  inbound baggage. The consumer now extracts `baggage` too and carries it onto
  the context passed to `OnEvent` (the consumer span remains a new root linked
  to the producer — only baggage rides along), and keeps the `baggage` field out
  of the business `fields` map. `baggage` is also added to the producer's
  reserved-field set.

## v0.3.0 (2026-05-27)

### Changed

- License changed to Apache-2.0

## v0.2.1 (2026-04-23)

### Changed

- **Topic matching routes through `vinculum-bus/topicmatch`** — pubsub publisher channel mapping and subscriber field extraction now honor MQTT 5.0 §4.7.2: filters starting with `+` or `#` no longer match reserved `$`-prefixed topics. Exact and `$`-prefixed patterns are unaffected. Requires vinculum-bus v0.12.0.

## v0.2.0 (2026-04-17)

### Added

- **Pluggable wire format support** — pubsub publisher/subscriber and stream producer/consumer builders now accept `WithWireFormat(wire.WireFormat)` or `WithWireFormatName(name)` to control payload serialization/deserialization. Built-in formats: `auto` (default), `json`, `string`, `bytes`. The default `auto` preserves backward compatibility. Depends on `github.com/tsarna/vinculum-wire` v0.1.0.

### Changed

- **Strings serialize verbatim in auto mode** — the `auto` wire format passes strings through unchanged (not JSON-encoded). Previously, strings were JSON-encoded with quotes. Use `wire_format = "json"` for the old behavior.

### Removed

- **Inline `serializePayload` / `deserializePayload` functions** — replaced by the shared `vinculum-wire` module.
- **`go2cty2go` dependency** — cty conversion now handled by vinculum's `CtyWireFormat` decorator at the config layer.
