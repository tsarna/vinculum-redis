# Changelog

## [Unreleased]

## v0.2.0 (2026-04-17)

### Added

- **Pluggable wire format support** — pubsub publisher/subscriber and stream producer/consumer builders now accept `WithWireFormat(wire.WireFormat)` or `WithWireFormatName(name)` to control payload serialization/deserialization. Built-in formats: `auto` (default), `json`, `string`, `bytes`. The default `auto` preserves backward compatibility. Depends on `github.com/tsarna/vinculum-wire` v0.1.0.

### Changed

- **Strings serialize verbatim in auto mode** — the `auto` wire format passes strings through unchanged (not JSON-encoded). Previously, strings were JSON-encoded with quotes. Use `wire_format = "json"` for the old behavior.

### Removed

- **Inline `serializePayload` / `deserializePayload` functions** — replaced by the shared `vinculum-wire` module.
- **`go2cty2go` dependency** — cty conversion now handled by vinculum's `CtyWireFormat` decorator at the config layer.
