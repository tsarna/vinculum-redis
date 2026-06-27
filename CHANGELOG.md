# Changelog

## [Unreleased]

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
