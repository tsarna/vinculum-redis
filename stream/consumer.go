package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// GroupCreatePolicy controls whether Start() creates the consumer group.
type GroupCreatePolicy int

const (
	// GroupCreateIfMissing calls XGROUP CREATE with MKSTREAM starting
	// from "$" (only new entries) if the group does not yet exist.
	GroupCreateIfMissing GroupCreatePolicy = iota

	// GroupRequireExisting fails Start() if the group does not exist.
	GroupRequireExisting

	// GroupCreateFromStart creates the group reading from ID "0" so the
	// consumer replays all historical entries.
	GroupCreateFromStart
)

// VinculumTopicFromStreamFunc resolves the vinculum topic for a stream
// entry. Returning "" passes the stream name through unchanged.
type VinculumTopicFromStreamFunc func(stream string, entryID string, msg any, fields map[string]string) (string, error)

// RedisStreamConsumer reads from a single Redis stream via XREADGROUP and
// delivers each entry onto a bus.Subscriber.
type RedisStreamConsumer struct {
	name             string
	client           goredis.UniversalClient
	streamName       string
	group            string
	consumerName     string
	batchSize        int64
	blockTimeout     time.Duration
	autoAck          bool
	groupCreate      GroupCreatePolicy
	target           bus.Subscriber
	topicFunc        VinculumTopicFromStreamFunc
	payloadField     string
	topicField       string
	contentTypeField string
	fieldsMode       FieldsMode
	wireFormat       wire.WireFormat
	onDecodeError    wire.DecodeErrorHook
	logger           *zap.Logger

	reclaimPending   bool
	reclaimMinIdle   time.Duration
	deadLetterStream string
	deadLetterAfter  int64

	metrics        *streamMetrics
	tracerProvider trace.TracerProvider

	// Two cancels, because stopping is two things and a graceful shutdown wants
	// them apart. stopRead ends the XREADGROUP loop and nothing else; stopWork
	// cancels the context every delivery and every settle rides on, and so is
	// the one that ends the consumer. stopRead's context is derived from
	// stopWork's, so cancelling work ends reading too.
	mu       sync.Mutex
	stopRead context.CancelFunc
	stopWork context.CancelFunc
	running  sync.WaitGroup

	// unsettled counts deliveries handed out and not yet acknowledged, nacked,
	// or abandoned. See Unsettled.
	unsettled atomic.Int64

	// stillDelivering is the channel a timed-out Drain was waiting on, closed
	// when the loop finally finishes. Nil until the first drain, and set by
	// every drain rather than only by one that gives up — what makes it answer
	// "no" is the channel being closed, not the field being absent.
	//
	// A channel rather than a flag because the question is asked a phase later
	// and the answer moves in between: teardown runs a whole quiesce between
	// Drain and Stop, so a delivery that overran the drain's deadline by a
	// moment has very likely finished by the time Stop looks. A flag would say
	// otherwise and put an error in the log of a shutdown where nothing went
	// wrong. See stillRunning.
	stillDelivering atomic.Pointer[chan struct{}]
}

// stillRunning reports whether a delivery a drain gave up on is running *now*,
// rather than whether one ever was.
func (c *RedisStreamConsumer) stillRunning() bool {
	ch := c.stillDelivering.Load()
	if ch == nil {
		return false
	}
	select {
	case <-*ch:
		return false
	default:
		return true
	}
}

// Unsettled reports how many deliveries this consumer has handed out that
// nothing has settled yet.
//
// It is not the length of the pending entries list. An entry left pending by a
// nack, or by a delivery that failed before reaching the target, is Redis's
// business — reclaim_min_idle and dead_letter_after decide what becomes of it —
// and nothing in this process is going to acknowledge it. What this counts is
// the narrower thing a shutdown can usefully wait for: settles that are still
// coming.
func (c *RedisStreamConsumer) Unsettled() int { return int(c.unsettled.Load()) }

// Start creates the group per policy, then launches the poll loop.
func (c *RedisStreamConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.stopWork != nil {
		c.mu.Unlock()
		return fmt.Errorf("redis_stream consumer %q: already started", c.name)
	}

	// The loop from the last cycle still owns the WaitGroup. A Stop that gave up
	// on a delivery returns without waiting, so that goroutine never reached its
	// Done — and starting another over the top of it would leave the counter at
	// two, with this cycle's Stop blocking forever on the abandoned half of it.
	// That is the unbounded wait the bound above exists to remove, one cycle
	// later and with nothing left to report it. Checked before the clear below,
	// which would otherwise erase the evidence.
	if c.stillRunning() {
		c.mu.Unlock()
		return fmt.Errorf("redis_stream consumer %q: a previous delivery is still running", c.name)
	}

	if err := c.ensureGroup(ctx); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("redis_stream consumer %q: %w", c.name, err)
	}

	if c.reclaimPending {
		if err := c.reclaimOwn(ctx); err != nil {
			c.logger.Warn("redis_stream consumer: reclaim pending",
				zap.String("consumer", c.name),
				zap.Error(err))
			// Don't fail Start on reclaim errors — the consumer can still
			// make forward progress on new entries.
		}
	}

	workCtx, stopWork := context.WithCancel(context.Background())
	readCtx, stopRead := context.WithCancel(workCtx)
	c.stopWork = stopWork
	c.stopRead = stopRead

	// stillDelivering is deliberately left as it is. The guard above has already
	// established that it is nil or closed, and a closed channel answers
	// stillRunning the same way nil does — until this cycle's own Drain
	// replaces it. Clearing it here would look like the load-bearing step and
	// would not be one.

	// Added under the lock, because Drain takes the same lock before it waits.
	// Outside it there is a window in which a drain landing between the unlock
	// and the Add finds the counter at zero, returns immediately, and reports a
	// clean drain of a loop that had not started — and the Add then races the
	// Wait, which is the one thing a WaitGroup forbids.
	c.running.Add(1)
	c.mu.Unlock()

	c.metrics.AddConnected(ctx, c.streamName, c.group, 1)
	go c.runLoop(readCtx, workCtx)
	return nil
}

// Ack issues XACK for the given entry ID on this consumer's stream and group,
// for a caller holding an entry ID it obtained some other way.
//
// Prefer the settler on a delivery's context, which is how a consumer of this
// package acknowledges what it was handed: it knows the entry without being
// told, and it settles once however many subscribers see the same delivery.
func (c *RedisStreamConsumer) Ack(ctx context.Context, id string) error {
	return c.ackEntry(ctx, c.streamName, id)
}

// Stream returns the stream name this consumer reads from. Used by
// callers that need to pair the consumer with stream-level operations
// (e.g. XPENDING queries in integration tests).
func (c *RedisStreamConsumer) Stream() string { return c.streamName }

// Group returns the consumer group name.
func (c *RedisStreamConsumer) Group() string { return c.group }

// Drain stops reading new entries and waits for the loop to finish the batch it
// is holding. It leaves everything else alone: the Redis client stays open, the
// settlers already handed out stay valid, and a delivery still travelling
// through a queue downstream settles normally when it lands.
//
// That is the whole difference between draining and stopping, and it is what
// lets a shutdown stop consuming first and disconnect last. Between the two, a
// process is finishing work it has already accepted and taking on none.
//
// Bounded by ctx, which the caller sizes: what is being waited for is one
// batch's worth of delivery, and delivery runs user-supplied work.
//
// Safe to call before Start, after Stop, or twice — though a second call after
// one that timed out reports the timeout again rather than a clean drain, since
// the delivery it gave up on is still running.
func (c *RedisStreamConsumer) Drain(ctx context.Context) error {
	c.mu.Lock()
	stopRead := c.stopRead
	if stopRead == nil {
		c.mu.Unlock()
		if c.stillRunning() {
			return fmt.Errorf("redis_stream consumer %q: still delivering", c.name)
		}
		return nil
	}
	c.stopRead = nil
	stopRead()

	done := make(chan struct{})
	go func() {
		c.running.Wait()
		close(done)
	}()

	// Published before the wait, not only when the wait gives up: the waiter
	// outlives this call either way, and while it is open it is the honest
	// answer to "is the loop still delivering" — which is what a later phase
	// asks.
	//
	// Published under the same lock that cleared stopRead, because the two
	// together are what a concurrent second Drain reads. Between them it would
	// see the field already taken and no waiter yet, and report a clean drain
	// that has not happened — which is the whole defect this is here to
	// prevent, surviving in the gap. Nothing under this lock does I/O.
	c.stillDelivering.Store(&done)
	c.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("redis_stream consumer %q: drain: %w", c.name, ctx.Err())
	}
}

// Stop ends the consumer: reading stops if it has not already, and the context
// every in-flight delivery and every outstanding settle rides on is cancelled.
// An acknowledgement arriving after this has nowhere to go, which is why a
// graceful shutdown drains first and gets here only once the pipeline is empty.
//
// It waits for the loop, so a delivery still running finishes and settles
// normally — with one exception. A Drain that timed out has already given that
// delivery a bounded chance to finish, and it did not take it; waiting here
// would hand the same expression a second wait with no bound at all, and this
// time nothing would interrupt it. So Stop cancels and reports rather than
// blocking, because the one thing a stuck action must never be able to do is
// stop the process from exiting.
//
// Whether it is *still* running is checked here rather than remembered from the
// drain. A whole phase separates the two, and a delivery that overran the
// drain's deadline by a moment has usually finished by now — reporting one that
// has not, when it has, is an error in the log of a shutdown that went fine.
//
// Repeated calls repeat the answer, as Drain's do, so a caller that stops twice
// is not told the second time that everything was well.
//
// A consumer stopped this way cannot be started again until that delivery
// finishes, because the goroutine still owns the loop's WaitGroup. Start says
// so rather than queueing up a wait that would never end.
func (c *RedisStreamConsumer) Stop() error {
	c.mu.Lock()
	stopWork := c.stopWork
	c.stopRead, c.stopWork = nil, nil
	c.mu.Unlock()

	if stopWork == nil {
		return c.stoppedWithDeliveryRunning()
	}
	// Cancelling work cancels reading with it: the read context is derived
	// from this one, so a Stop that was not preceded by a Drain still ends
	// the loop.
	stopWork()

	if err := c.stoppedWithDeliveryRunning(); err != nil {
		c.metrics.AddConnected(context.Background(), c.streamName, c.group, -1)
		return err
	}

	c.running.Wait()
	c.metrics.AddConnected(context.Background(), c.streamName, c.group, -1)
	return nil
}

func (c *RedisStreamConsumer) stoppedWithDeliveryRunning() error {
	if !c.stillRunning() {
		return nil
	}
	return fmt.Errorf("redis_stream consumer %q: stopped with a delivery still running", c.name)
}

func (c *RedisStreamConsumer) ensureGroup(ctx context.Context) error {
	startID := "$"
	if c.groupCreate == GroupCreateFromStart {
		startID = "0"
	}

	if c.groupCreate == GroupRequireExisting {
		// Use XInfoGroups to verify the group exists.
		groups, err := c.client.XInfoGroups(ctx, c.streamName).Result()
		if err != nil {
			return fmt.Errorf("xinfo groups: %w", err)
		}
		for _, g := range groups {
			if g.Name == c.group {
				return nil
			}
		}
		return fmt.Errorf("consumer group %q does not exist on stream %q", c.group, c.streamName)
	}

	err := c.client.XGroupCreateMkStream(ctx, c.streamName, c.group, startID).Err()
	if err != nil && !isBusyGroupErr(err) {
		return fmt.Errorf("xgroup create %q/%q: %w", c.streamName, c.group, err)
	}
	return nil
}

// reclaimOwn walks this consumer/group's pending list at startup and
// re-claims entries idle for at least reclaimMinIdle via XCLAIM. The
// claimed entries then appear on the next XREADGROUP poll and are
// delivered normally.
func (c *RedisStreamConsumer) reclaimOwn(ctx context.Context) error {
	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: c.streamName,
		Group:  c.group,
		Start:  "-",
		End:    "+",
		Count:  1000,
	}).Result()
	if err != nil {
		return fmt.Errorf("xpending: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}
	ids := make([]string, 0, len(pending))
	for _, p := range pending {
		if p.Idle >= c.reclaimMinIdle {
			ids = append(ids, p.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	claimed, err := c.client.XClaim(ctx, &goredis.XClaimArgs{
		Stream:   c.streamName,
		Group:    c.group,
		Consumer: c.consumerName,
		MinIdle:  c.reclaimMinIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return fmt.Errorf("xclaim: %w", err)
	}
	c.metrics.RecordReclaimed(ctx, c.streamName, c.group, int64(len(claimed)))
	// Claimed entries are now in our PEL but XREADGROUP > won't redeliver
	// them. Run them through the delivery path directly so subscribers see
	// them on Start without waiting for a new XADD.
	for _, entry := range claimed {
		c.metrics.AddPending(ctx, c.streamName, c.group, 1)
		if err := c.deliver(ctx, c.streamName, entry); err != nil {
			c.logger.Warn("redis_stream consumer: reclaim deliver",
				zap.String("consumer", c.name),
				zap.String("id", entry.ID),
				zap.Error(err))
		}
	}
	return nil
}

// isBusyGroupErr matches the BUSYGROUP error returned when the group
// already exists — a no-op for the create-if-missing path.
func isBusyGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// runLoop reads entries and delivers them until reading is stopped.
//
// The two contexts are the same lifetime until a drain separates them. readCtx
// bounds the XREADGROUP call and decides when the loop exits; workCtx is what
// every delivery, settle and dead-letter runs on, and outlives readCtx by the
// length of the shutdown. Passing readCtx to a delivery would mean draining
// cancelled the work it was waiting for, and cancelled the acknowledgement it
// was waiting for the work to produce.
//
// The batch in hand is finished either way: the exit check is at the top of the
// loop, so a drain that lands mid-batch delivers the rest of it and then stops.
func (c *RedisStreamConsumer) runLoop(readCtx, workCtx context.Context) {
	defer c.running.Done()
	backoff := time.Second
	for {
		if readCtx.Err() != nil {
			return
		}
		start := time.Now()
		streams, err := c.client.XReadGroup(readCtx, &goredis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumerName,
			Streams:  []string{c.streamName, ">"},
			Count:    c.batchSize,
			Block:    c.blockTimeout,
		}).Result()
		c.metrics.RecordReceiveDuration(workCtx, c.streamName, time.Since(start).Seconds())

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if err == goredis.Nil {
				// Block timeout with no entries — just poll again.
				continue
			}
			c.logger.Warn("redis_stream consumer: xreadgroup",
				zap.String("consumer", c.name),
				zap.Error(err))
			select {
			case <-readCtx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		for _, s := range streams {
			for _, entry := range s.Messages {
				// Entry is now in this consumer's PEL until ACK or DLQ.
				c.metrics.AddPending(workCtx, s.Stream, c.group, 1)
				// deliver settles on what the target did; what is left here is
				// the retry budget, which is this loop's business rather than
				// one delivery's.
				if err := c.deliver(workCtx, s.Stream, entry); err != nil {
					c.logger.Warn("redis_stream consumer: deliver",
						zap.String("consumer", c.name),
						zap.String("stream", s.Stream),
						zap.String("id", entry.ID),
						zap.Error(err))
					// Delivery failed: see whether we've exhausted the
					// configured retry budget and should DLQ instead of
					// leaving the entry pending for another attempt.
					if c.deadLetterStream != "" && c.deadLetterAfter > 0 {
						if _, dlErr := c.maybeDeadLetter(workCtx, s.Stream, entry); dlErr != nil {
							c.logger.Warn("redis_stream consumer: dead-letter",
								zap.String("consumer", c.name),
								zap.String("id", entry.ID),
								zap.Error(dlErr))
						}
					}
				}
			}
		}
	}
}

// maybeDeadLetter checks this entry's delivery count via XPENDING and, if
// it has exceeded deadLetterAfter, re-adds it to the dead-letter stream
// and XACKs the original. Returns true when the entry was moved.
func (c *RedisStreamConsumer) maybeDeadLetter(ctx context.Context, streamName string, entry goredis.XMessage) (bool, error) {
	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: streamName,
		Group:  c.group,
		Start:  entry.ID,
		End:    entry.ID,
		Count:  1,
	}).Result()
	if err != nil {
		return false, fmt.Errorf("xpending: %w", err)
	}
	if len(pending) == 0 {
		return false, nil
	}
	// XREADGROUP already incremented the delivery count before we got
	// here, so RetryCount reflects this attempt inclusive. Move once the
	// consumer has tried at least deadLetterAfter times.
	if pending[0].RetryCount < c.deadLetterAfter {
		return false, nil
	}
	values := make(map[string]interface{}, len(entry.Values)+2)
	for k, v := range entry.Values {
		values[k] = v
	}
	values["_dlq_original_stream"] = streamName
	values["_dlq_original_id"] = entry.ID
	if _, err := c.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: c.deadLetterStream,
		Values: values,
	}).Result(); err != nil {
		return false, fmt.Errorf("xadd dlq: %w", err)
	}
	if err := c.client.XAck(ctx, streamName, c.group, entry.ID).Err(); err != nil {
		return false, fmt.Errorf("xack: %w", err)
	}
	c.metrics.AddPending(ctx, streamName, c.group, -1)
	c.metrics.RecordDeadLettered(ctx, streamName, c.deadLetterStream)
	return true, nil
}

// deliver hands one entry to the target and settles it on what the target did.
//
// The settle is here rather than in the callers because this is the only place
// that knows the target's outcome. A caller acknowledging on deliver's return
// would be acknowledging the *enqueue* whenever anything downstream defers —
// a queue_size queue, a bus hop, a state machine — which is the entry being
// reported handled before anything handled it.
//
// Failures before the target is reached are not settled here. The entry is
// still outstanding and the caller's dead-letter budget is what decides its
// fate, which is a policy this function has no business preempting.
func (c *RedisStreamConsumer) deliver(ctx context.Context, streamName string, entry goredis.XMessage) error {
	settler, ops := c.newSettler(streamName, entry.ID)

	msg, fields, err := c.parseEntry(ctx, entry)
	if err != nil {
		// Released, not settled. The entry stays pending — that is the policy
		// this function does not preempt — but no acknowledgement is ever
		// coming for it, so a shutdown has nothing to wait for.
		ops.release()
		return err
	}

	// Extract W3C trace context from the entry's traceparent/tracestate
	// fields (written by the Vinculum producer). Streams are a persistent
	// log — producer and consumer can be minutes or hours apart, possibly
	// across process restarts — so we do NOT make the producer span the
	// parent. Instead we start a fresh root span and attach the producer
	// context as a link. This matches the OTel messaging semconv guidance
	// for batched/async consumers.
	carrier := mapCarrier{}
	for _, k := range []string{"traceparent", "tracestate", "baggage"} {
		if v, ok := entry.Values[k]; ok {
			carrier[k] = asString(v)
		}
	}
	producerCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

	tp := c.tracerProvider
	if tp == nil {
		tp = noop.NewTracerProvider()
	}
	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("messaging.destination.name", streamName),
			attribute.String("messaging.consumer.group.name", c.group),
			attribute.String("messaging.destination.subscription.name", c.consumerName),
			attribute.String("messaging.message.id", entry.ID),
			attribute.String("messaging.operation.name", "process"),
		),
	}
	if psc := trace.SpanContextFromContext(producerCtx); psc.IsValid() {
		startOpts = append(startOpts, trace.WithLinks(trace.Link{SpanContext: psc}))
	}
	ctx, span := tp.Tracer("github.com/tsarna/vinculum-redis/stream").
		Start(ctx, "process "+streamName, startOpts...)
	defer span.End()

	// Carry the producer's baggage onto the processing context so it reaches
	// target.OnEvent and action expressions. The consumer span above stays a new
	// root linked to the producer span — only baggage rides along, not the span
	// parent.
	if bg := baggage.FromContext(producerCtx); bg.Len() > 0 {
		ctx = baggage.ContextWithBaggage(ctx, bg)
	}

	// Acknowledgement is a property of this delivery, and `fields` cannot carry
	// it — the bus rewrites those per subscription. The context can, and it is
	// preserved across the async queue's goroutine hop, so putting the settler
	// here is what lets a subscription several hops downstream acknowledge the
	// entry it handled.
	ctx = bus.WithSettler(ctx, settler)

	topic := streamName
	if c.topicFunc != nil {
		out, err := c.topicFunc(streamName, entry.ID, msg, fields)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			c.metrics.RecordError(ctx, "process", "vinculum_topic")
			ops.release()
			return fmt.Errorf("vinculum_topic: %w", err)
		}
		if out != "" {
			topic = out
		}
	}

	start := time.Now()
	err = c.target.OnEvent(ctx, topic, msg, fields)
	c.metrics.RecordProcessDuration(ctx, streamName, c.group, time.Since(start).Seconds())

	// The settle point. Under auto_ack this acknowledges a target that handled
	// the entry and leaves one that only queued it to settle at its own
	// completion; under manual it does nothing but report a failure, because
	// the configuration asked for the decision.
	bus.SettleOnReturn(ctx, c.target, err)

	// An observing target settles nothing and defers to nobody — it saw the
	// entry go past. SettleOnReturn returns without acting, so no settle is
	// coming from anywhere and this delivery has to be released by hand or the
	// count never comes back down. It is the third of the three paths through
	// here that reach no settler; the other two are the failures above.
	if bus.DispositionOf(c.target) == bus.Observed {
		ops.release()
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.metrics.RecordError(ctx, "process", "deliver")
		return err
	}
	c.metrics.RecordConsumed(ctx, streamName, c.group, c.consumerName)
	return nil
}

// parseEntry extracts payload + fields from a stream entry using the
// symmetric payload_field / topic_field / fields_mode configuration. The
// returned fields map always carries the entry's ID as `$entry_id`.
func (c *RedisStreamConsumer) parseEntry(ctx context.Context, entry goredis.XMessage) (any, map[string]string, error) {
	var payload any
	fields := make(map[string]string)

	for k, v := range entry.Values {
		if c.payloadField != "" && k == c.payloadField {
			raw := asBytes(v)
			if raw != nil {
				var deserErr error
				payload, deserErr = c.wireFormat.Deserialize(raw)
				if deserErr != nil {
					// A decode failure is fatal to the entry: the configured
					// wire format is a contract, so an entry that doesn't
					// satisfy it is not delivered. Use wire format "auto"
					// for best-effort decoding. The entry stays in the PEL
					// and is dead-lettered once dead_letter_after retries
					// are exhausted; without dead-lettering configured it
					// remains pending indefinitely.
					c.logger.Error("redis_stream consumer: deserialize failed",
						zap.String("stream", c.streamName),
						zap.String("entry_id", entry.ID),
						zap.String("wire_format", c.wireFormat.Name()),
						zap.Error(deserErr))
					c.metrics.RecordError(ctx, "process", "deserialize")
					if c.onDecodeError != nil {
						c.onDecodeError(ctx, wire.DecodeError{
							Raw:    raw,
							Err:    deserErr,
							Format: c.wireFormat.Name(),
							Topic:  c.streamName,
							Attrs: map[string]string{
								"stream":   c.streamName,
								"entry_id": entry.ID,
								"group":    c.group,
								"consumer": c.consumerName,
							},
						})
					}
					return nil, nil, fmt.Errorf("redis_stream consumer: deserialize entry %s: %w", entry.ID, deserErr)
				}
			}
			continue
		}
		if c.topicField != "" && k == c.topicField {
			continue // not a user field; origin topic is the Vinculum producer's hint
		}
		if c.contentTypeField != "" && k == c.contentTypeField {
			continue
		}
		if k == "traceparent" || k == "tracestate" || k == "baggage" {
			// Trace context / baggage already consumed by the propagator; keep
			// it out of the business fields map.
			continue
		}
		switch c.fieldsMode {
		case FieldsOmit:
			// drop
		case FieldsNested:
			if k == "fields" {
				if err := json.Unmarshal(asBytes(v), &fields); err != nil {
					c.metrics.RecordError(ctx, "process", "fields_decode")
					return nil, nil, fmt.Errorf("decode fields: %w", err)
				}
			}
		default: // FieldsFlat
			if k == "fields" {
				continue
			}
			fields[k] = asString(v)
		}
	}

	// The entry ID is not one of the entry's own fields, but manual
	// acknowledgement needs it and the delivery path has nowhere else to put
	// it. Expose it under the `$` prefix reserved for system-generated names,
	// as the SQS receiver does with `$receipt_handle`. It is added regardless
	// of fields_mode — that setting governs the entry's own fields — so the
	// map is never empty.
	fields["$entry_id"] = entry.ID

	return payload, fields, nil
}

// asBytes coerces a stream-entry value to []byte. go-redis returns strings
// by default; callers that passed raw []byte in XAdd get a string back
// when reading (Redis is string-typed on the wire).
func asBytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	default:
		return nil
	}
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}
