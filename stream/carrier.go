package stream

// mapCarrier is an OTel TextMapCarrier backed by a plain string map.
// Used to inject/extract W3C trace context (traceparent/tracestate) via
// stream-entry fields — the de-facto pattern for OTel propagation over
// Redis Streams.
type mapCarrier map[string]string

func (c mapCarrier) Get(key string) string { return c[key] }

func (c mapCarrier) Set(key, val string) { c[key] = val }

func (c mapCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}
