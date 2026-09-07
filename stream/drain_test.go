package stream_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-redis/stream"
)

// drainFixture is settleFixture plus the consumer itself, which a drain test
// needs to call Drain on. It deliberately does not register a Stop cleanup:
// these tests stop the consumer themselves, at the point in the sequence they
// are about.
func drainFixture(t *testing.T, autoAck bool, target bus.Subscriber) (*goredis.Client, *stream.RedisStreamConsumer, func(payload any)) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	cons := stream.NewConsumer("in", c).
		WithStream("events").
		WithGroup("g").
		WithConsumerName("c").
		WithBlockTimeout(50 * time.Millisecond).
		WithAutoAck(autoAck).
		WithTarget(target).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	t.Cleanup(func() { _ = cons.Stop() })

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()

	return c, cons, func(payload any) {
		require.NoError(t, p.OnEvent(context.Background(), "x", payload, nil))
	}
}

// The point of a drain: entries already read are finished, and no more are
// taken on. Stopping does both at once, which is why a shutdown that only has
// Stop cannot stop consuming before it disconnects.
func TestDrainStopsReading(t *testing.T) {
	recv := &recorder{}
	_, cons, produce := drainFixture(t, true, recv)

	produce("before")
	recv.wait(t, 1)

	require.NoError(t, cons.Drain(context.Background()))

	produce("after")
	// Several block timeouts' worth: a loop that is still reading has had
	// every opportunity to pick this up.
	time.Sleep(300 * time.Millisecond)

	recv.mu.Lock()
	defer recv.mu.Unlock()
	require.Len(t, recv.events, 1, "the consumer kept reading after it was drained")
	assert.Equal(t, "before", recv.events[0].msg)
}

// A drained consumer is still connected, and the settler it handed out before
// the drain still acknowledges. This is the whole reason draining and stopping
// are separate: the acknowledgement for a message the pipeline is still
// carrying arrives after the poll loop has gone.
func TestDrainLeavesAnOutstandingSettlerAbleToAcknowledge(t *testing.T) {
	recv := &recorder{}
	c, cons, produce := drainFixture(t, false, recv)

	produce("hi")
	events := recv.wait(t, 1)
	settler := bus.SettlerFromContext(events[0].ctx)
	require.NotNil(t, settler)

	require.NoError(t, cons.Drain(context.Background()))
	require.EqualValues(t, 1, pendingCount(t, c), "nothing settled it yet")
	assert.Equal(t, 1, cons.Unsettled())

	settled, err := settler.Ack(context.Background())
	require.NoError(t, err)
	assert.True(t, settled)

	assert.EqualValues(t, 0, pendingCount(t, c), "the XACK did not reach Redis")
	assert.Equal(t, 0, cons.Unsettled())
}

// What the shutdown phase reads. Under manual settle nothing acknowledges the
// entry until the configuration does, so the count is what says the process
// still owes Redis an answer.
func TestUnsettledCountsWhatIsStillOwed(t *testing.T) {
	recv := &recorder{}
	_, cons, produce := drainFixture(t, false, recv)

	assert.Equal(t, 0, cons.Unsettled())

	produce("one")
	produce("two")
	events := recv.wait(t, 2)
	assert.Equal(t, 2, cons.Unsettled())

	_, err := bus.SettlerFromContext(events[0].ctx).Ack(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, cons.Unsettled())

	// A nack settles nothing at Redis — the entry stays pending for reclaim —
	// but this process is done with it, so it stops being something a shutdown
	// waits for.
	_, err = bus.SettlerFromContext(events[1].ctx).Nack(context.Background(), "no")
	require.NoError(t, err)
	assert.Equal(t, 0, cons.Unsettled())
}

// Under auto the framework settles when the work finishes, so the count comes
// back to zero on its own and a shutdown waits for nothing.
func TestUnsettledReturnsToZeroWhenTheFrameworkSettles(t *testing.T) {
	recv := &recorder{}
	_, cons, produce := drainFixture(t, true, recv)

	produce("hi")
	recv.wait(t, 1)

	assert.Eventually(t, func() bool { return cons.Unsettled() == 0 },
		2*time.Second, 10*time.Millisecond,
		"an automatically settled delivery stayed on the books")
}

// blocker holds a delivery until it is released, so a test can be sure the
// consumer is mid-batch when it drains, and records what the delivery's own
// context looked like on the far side of the wait.
type blocker struct {
	bus.BaseSubscriber
	entered chan struct{}
	release chan struct{}

	// Atomic, though the one read is ordered after the write by Drain's own
	// wait. This fixture is copied per receiver, and the next one to take it
	// may not have that ordering.
	errAfterRelease atomic.Pointer[error]
}

// entered is signalled rather than closed, so a test that restarts a consumer
// past this target does not panic on a second delivery.
func (b *blocker) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	err := ctx.Err()
	b.errAfterRelease.Store(&err)
	return nil
}

func newBlocker() *blocker {
	return &blocker{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

// Draining must not cancel the work it is waiting for. Reading and delivering
// share one context until this splits them, and cancelling that one context to
// stop the loop would abort the delivery in flight and, with it, the
// acknowledgement the delivery was about to produce — a shutdown breaking
// exactly the message it was trying to let finish.
func TestDrainDoesNotCancelTheDeliveryInFlight(t *testing.T) {
	b := newBlocker()
	_, cons, produce := drainFixture(t, true, b)

	produce("hi")
	<-b.entered

	drained := make(chan error, 1)
	go func() { drained <- cons.Drain(context.Background()) }()

	// Long enough that a drain which cancels the delivery has already done so.
	time.Sleep(100 * time.Millisecond)
	close(b.release)

	require.NoError(t, <-drained)
	got := b.errAfterRelease.Load()
	require.NotNil(t, got)
	assert.NoError(t, *got, "the drain cancelled the context the delivery was running on")
}

// Delivery runs user-supplied work, so a drain that waited for it
// unconditionally would hand one stuck expression the power to stop a process
// from exiting. The caller's context is the bound.
func TestDrainIsBoundedByItsContext(t *testing.T) {
	b := newBlocker()
	_, cons, produce := drainFixture(t, true, b)
	defer close(b.release)

	produce("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := cons.Drain(ctx)
	require.Error(t, err, "drain waited for an action that never returned")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)

	// And it stays reported. A second call must not answer "drained cleanly"
	// for a consumer whose delivery is still running.
	assert.Error(t, cons.Drain(context.Background()))
}

// The bound above is worth nothing if the same stuck action then meets an
// unbounded wait one phase later. Stop cancels and reports instead — a delivery
// that ignored a bounded chance to finish does not get an unbounded one, and
// the process exits.
func TestStopDoesNotWaitAgainForADeliveryTheDrainGaveUpOn(t *testing.T) {
	b := newBlocker()
	_, cons, produce := drainFixture(t, true, b)
	defer close(b.release)

	produce("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, cons.Drain(ctx))

	stopped := make(chan error, 1)
	go func() { stopped <- cons.Stop() }()

	select {
	case err := <-stopped:
		assert.Error(t, err, "stopping past a running delivery should say so")
		// And it keeps saying so. A second call must not report a clean stop
		// for the same delivery, which is the asymmetry with Drain that a
		// caller stopping twice would otherwise see.
		assert.Error(t, cons.Stop())
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on the delivery the drain had already given up on")
	}
}

// A whole phase runs between Drain and Stop — quiesce, which is bounded at ten
// seconds of its own — so a delivery that overran the drain's deadline by a
// moment has very likely finished by the time Stop asks. Remembering the drain's
// verdict instead of re-checking puts an error in the log of a shutdown where
// nothing went wrong, which is the same failure as reporting the wrong holder:
// an operator-facing line that misreports.
func TestStopReportsCleanlyWhenTheDeliveryFinishedAfterTheDrainGaveUp(t *testing.T) {
	b := newBlocker()
	_, cons, produce := drainFixture(t, true, b)

	produce("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, cons.Drain(ctx), "the drain should have given up")

	// The delivery finishes in the window the real shutdown spends quiescing.
	// Polling Drain is how the test sees that: after a drain has already run,
	// the call reads the answer and changes nothing.
	close(b.release)
	assert.Eventually(t, func() bool { return cons.Drain(context.Background()) == nil },
		2*time.Second, 10*time.Millisecond,
		"Drain kept reporting a timeout for a delivery that had finished")

	assert.NoError(t, cons.Stop(),
		"Stop reported a delivery still running that had already finished")
}

// `running` is one WaitGroup for the life of the consumer, and a Stop that gave
// up on a delivery leaves that delivery's goroutine still holding it. Starting
// again over the top would put the counter at two, and the next Stop would
// block forever on the abandoned half — the unbounded wait this whole
// arrangement removes, reappearing one cycle later where nothing is left to
// report it. Start refuses instead, and stops refusing once the goroutine goes.
func TestStartRefusesWhileAnAbandonedDeliveryStillOwnsTheLoop(t *testing.T) {
	b := newBlocker()
	_, cons, produce := drainFixture(t, true, b)

	produce("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, cons.Drain(ctx))
	require.Error(t, cons.Stop())

	require.Error(t, cons.Start(context.Background()),
		"starting again would leave the next Stop waiting on the abandoned goroutine")

	close(b.release)
	assert.Eventually(t, func() bool { return cons.Start(context.Background()) == nil },
		2*time.Second, 10*time.Millisecond,
		"the refusal outlived the delivery it was about")
	require.NoError(t, cons.Stop())
}

// observer sees deliveries go past and settles none of them, which is a
// disposition of its own rather than a target that forgot.
type observer struct {
	bus.BaseSubscriber
	seen chan struct{}
}

func (o *observer) OnEvent(context.Context, string, any, map[string]string) error {
	o.seen <- struct{}{}
	return nil
}

func (o *observer) DeliveryDisposition() bus.Disposition { return bus.Observed }

// The third path that reaches no settler. An observing target makes the
// framework settle point return without acting, so nothing is ever going to
// settle the delivery — and a count that never comes down makes every later
// shutdown wait out its whole budget and warn about a message nobody is
// carrying.
func TestUnsettledDoesNotLeakOnAnObservingTarget(t *testing.T) {
	o := &observer{seen: make(chan struct{}, 1)}
	_, cons, produce := drainFixture(t, true, o)

	produce("hi")
	<-o.seen

	assert.Eventually(t, func() bool { return cons.Unsettled() == 0 },
		2*time.Second, 10*time.Millisecond,
		"a delivery nothing will ever settle stayed on the books")
}

// A second Drain arriving while the first is still waiting must not report a
// clean drain, which is what it did while the waiter channel was published only
// on the timeout path. Teardown drains once, so this is a contract about the
// method rather than a shape the process produces.
func TestAConcurrentDrainDoesNotReportADrainThatHasNotHappened(t *testing.T) {
	b := newBlocker()
	_, cons, produce := drainFixture(t, true, b)
	defer close(b.release)

	produce("hi")
	<-b.entered

	first := make(chan error, 1)
	go func() { first <- cons.Drain(context.Background()) }()

	// Long enough that the first Drain is certainly waiting.
	time.Sleep(100 * time.Millisecond)
	assert.Error(t, cons.Drain(context.Background()),
		"a second drain reported success while the first was still waiting")
}

// The count is decremented once per entry however many times an op runs. The
// settler releases its claim when an op returns an error — so a failed XACK can
// be retried and reach Ack a second time — and a count that went negative would
// *subtract* from the shutdown phase's total, which sums every holder, so one
// entry could cancel another holder's real backlog and let the wait end early.
//
// Redis is the one receiver whose settler cannot go stale, so this retry is the
// only way to reach the release twice — which is exactly why it is worth
// asserting rather than assuming.
func TestAFailedAckDoesNotReleaseTheCountTwice(t *testing.T) {
	recv := &recorder{}
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	cons := stream.NewConsumer("in", c).
		WithStream("events").WithGroup("g").WithConsumerName("c").
		WithBlockTimeout(50 * time.Millisecond).
		WithAutoAck(false).
		WithTarget(recv).
		Build()
	require.NoError(t, cons.Start(context.Background()))
	t.Cleanup(func() { _ = cons.Stop() })

	p := stream.NewProducer("out", c).WithStreamFunc(func(string, any, map[string]string) (string, error) {
		return "events", nil
	}).Build()
	require.NoError(t, p.OnEvent(context.Background(), "x", "hi", nil))

	events := recv.wait(t, 1)
	settler := bus.SettlerFromContext(events[0].ctx)
	require.Equal(t, 1, cons.Unsettled())

	// The one way to make XACK fail: take the server away.
	mr.Close()

	_, err := settler.Ack(context.Background())
	require.Error(t, err, "the acknowledgement should have failed")
	assert.Equal(t, 0, cons.Unsettled())

	// The settler let go of its claim, so this reaches the op a second time.
	_, err = settler.Ack(context.Background())
	require.Error(t, err)
	assert.Equal(t, 0, cons.Unsettled(), "the count went negative on a retried settle")
}

// Teardown calls both, in that order, and a consumer that was never started is
// torn down along with everything else. None of that may panic or block.
func TestDrainAndStopComposeInAnyOrder(t *testing.T) {
	recv := &recorder{}
	_, cons, _ := drainFixture(t, true, recv)

	require.NoError(t, cons.Drain(context.Background()))
	require.NoError(t, cons.Drain(context.Background()))
	require.NoError(t, cons.Stop())
	require.NoError(t, cons.Stop())
	require.NoError(t, cons.Drain(context.Background()))

	fresh := stream.NewConsumer("never-started", goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})).
		WithStream("events").WithGroup("g").WithTarget(recv).Build()
	assert.NoError(t, fresh.Drain(context.Background()))
	assert.NoError(t, fresh.Stop())
}
