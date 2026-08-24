package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
)

// WorkerCallbacksCircuitBreakerSuite covers the outbound queue's circuit breaker as it applies to
// Worker-variant callback deliveries: which failures count against it, that it is keyed per task
// queue, that Describe reports a held-back callback as BLOCKED, and that BLOCKED is not terminal.
//
// Standalone Nexus operations stand in for every execution type here, since the delivery path a
// callback takes is the same whatever it hangs off; see [TestWorkerCallbacks].
type WorkerCallbacksCircuitBreakerSuite struct {
	parallelsuite.Suite[*WorkerCallbacksCircuitBreakerSuite]
}

func TestWorkerCallbacksCircuitBreakerSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkerCallbacksCircuitBreakerSuite{})
}

// gobreaker's default ReadyToTrip, which the outbound queue's pool leaves in place, opens the
// breaker once consecutive failures exceed five.
const circuitBreakerFailureThreshold = 5

// newCircuitBreakerEnv builds an env with the callback retry policy dialed down so that failures
// accumulate within a test's lifetime. The retry policy is a global setting, hence the dedicated
// cluster.
func (s *WorkerCallbacksCircuitBreakerSuite) newCircuitBreakerEnv(extra ...testcore.TestOption) *NexusTestEnv {
	opts := []testcore.TestOption{
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 10*time.Millisecond),
	}
	return newNexusTestEnv(s.T(), true, append(opts, extra...)...)
}

// fastUpstreamTimeoutOpts make a delivery to a task queue with no poller time out promptly instead
// of after the default ten seconds. Matching buffers the dispatch deadline by MinDispatchTaskTimeout,
// so that has to come down too, and the request timeout has to stay above it — a request timeout
// below the buffer leaves zero time and would fail a delivery to a live worker as well.
func fastUpstreamTimeoutOpts() []testcore.TestOption {
	return []testcore.TestOption{
		testcore.WithDynamicConfig(nexusoperations.MinDispatchTaskTimeout, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RequestTimeout, 500*time.Millisecond),
	}
}

func workerCallbackTo(taskQueue string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: taskQueue,
				Service:       "completion-service",
				Operation:     "on-complete",
			},
		},
	}
}

// startHandler starts a worker whose completion handler fails its first initialFailures deliveries
// with a retryable error and succeeds after that, and returns a Worker-variant callback addressed to
// it. A randomized task queue keeps each caller from tripping any other's breaker, since the task
// queue is the destination the breaker is keyed by.
func (s *WorkerCallbacksCircuitBreakerSuite) startHandler(
	env *NexusTestEnv,
	t *testing.T,
	taskQueue string,
	initialFailures int32,
) {
	t.Helper()

	var delivered atomic.Int32
	service := nexus.NewService("completion-service")
	operation := nexus.NewSyncOperation(
		"on-complete",
		func(_ context.Context, _ *notificationpb.OnCompleteRequest, _ nexus.StartOperationOptions) (*notificationpb.OnCompleteResponse, error) {
			if delivered.Add(1) <= initialFailures {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional failure")
			}
			return &notificationpb.OnCompleteResponse{}, nil
		},
	)
	require.NoError(t, service.Register(operation))

	worker := sdkworker.New(env.SdkClient(), taskQueue, sdkworker.Options{})
	worker.RegisterNexusService(service)
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)
}

// startSANOWithCallbacks starts an operation against endpointName, which completes it immediately,
// so its callbacks are scheduled for delivery right away.
func (s *WorkerCallbacksCircuitBreakerSuite) startSANOWithCallbacks(
	env *NexusTestEnv,
	t *testing.T,
	endpointName string,
	cbs ...*commonpb.Callback,
) string {
	t.Helper()

	operationID := testcore.RandomizeStr(t.Name())
	startResp, err := env.startNexusOperation(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:         operationID,
		Endpoint:            endpointName,
		CompletionCallbacks: cbs,
	})
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())
	return operationID
}

// awaitCallbackState waits for the callback at index to reach wantState, failing if it is reported
// as any of forbidden along the way.
func (s *WorkerCallbacksCircuitBreakerSuite) awaitCallbackState(
	env *NexusTestEnv,
	t *testing.T,
	operationID string,
	index, wantCount int,
	wantState enumspb.CallbackState,
	forbidden ...enumspb.CallbackState,
) {
	t.Helper()

	await.Require(s.Context(), t, func(c *await.T) {
		cbs := env.describeNexusOperation(c.Context(), c, operationID).GetCompletionCallbacks()
		require.Len(c, cbs, wantCount)
		got := cbs[index].GetInfo().GetState()
		// Asserted on the enclosing t: a forbidden state is a failure, not something to retry until
		// it goes away.
		require.NotContains(t, forbidden, got)
		require.Equal(c, wantState, got)
	}, 30*time.Second, 200*time.Millisecond)
}

// TestBlockedWhenCircuitBreakerOpens covers deliveries that keep failing against the task queue
// itself: the breaker for that task queue opens and Describe reports the callback as BLOCKED.
func (s *WorkerCallbacksCircuitBreakerSuite) TestBlockedWhenCircuitBreakerOpens() {
	env := s.newCircuitBreakerEnv(fastUpstreamTimeoutOpts()...)
	t := s.T()

	// Nothing polls this task queue, so every delivery ends in matching's upstream timeout. That is
	// a property of the destination rather than of any one callback, so it counts against the
	// breaker — unlike a handler error, which is the answer of a worker that did receive the
	// delivery. See TestHandlerErrorsDoNotOpenTheBreaker.
	unservedTaskQueue := testcore.RandomizeStr(t.Name() + "-unserved")
	endpointName := env.createSyncSuccessEndpoint(s.Context(), t, "operation-result")
	operationID := s.startSANOWithCallbacks(env, t, endpointName, workerCallbackTo(unservedTaskQueue))

	// Deliveries are retried until enough have failed to open the breaker, so the callback passes
	// through SCHEDULED and BACKING_OFF on the way to BLOCKED.
	s.awaitCallbackState(env, t, operationID, 0, 1, enumspb.CALLBACK_STATE_BLOCKED)

	cbs := env.describeNexusOperation(s.Context(), t, operationID).GetCompletionCallbacks()
	require.Equal(t, "The circuit breaker is open.", cbs[0].GetInfo().GetBlockedReason())
	require.Greater(t, cbs[0].GetInfo().GetAttempt(), int32(circuitBreakerFailureThreshold),
		"the breaker should not open before the failure threshold is exceeded")
}

// TestHandlerErrorsDoNotOpenTheBreaker covers a handler that is up and returning retryable errors.
// That is the registering caller's problem, not the task queue's, so it must not trip the breaker
// for a task queue every other callback may also be delivering to. The handler fails more times than
// the breaker's threshold, so counting those failures — as the delivery path used to — would open it.
func (s *WorkerCallbacksCircuitBreakerSuite) TestHandlerErrorsDoNotOpenTheBreaker() {
	env := s.newCircuitBreakerEnv()
	t := s.T()

	taskQueue := testcore.RandomizeStr(t.Name() + "-handler")
	s.startHandler(env, t, taskQueue, circuitBreakerFailureThreshold+3)
	endpointName := env.createSyncSuccessEndpoint(s.Context(), t, "operation-result")
	operationID := s.startSANOWithCallbacks(env, t, endpointName, workerCallbackTo(taskQueue))

	// The delivery that follows the failures succeeds, and the callback is never held back on the
	// way there.
	s.awaitCallbackState(env, t, operationID, 0, 1,
		enumspb.CALLBACK_STATE_SUCCEEDED, enumspb.CALLBACK_STATE_BLOCKED)
}

// TestBreakerIsPerTaskQueue covers the isolation the per-task-queue destination buys: one dead task
// queue must not hold back callbacks delivering to a healthy one.
//
// The two operations are deliberately sequential. Attaching both callbacks to one operation does not
// test anything: the healthy delivery succeeds in milliseconds, while the dead one needs several
// hundred milliseconds per attempt to accumulate enough failures, so the healthy callback is long
// finished before there is an open breaker for it to be affected by.
func (s *WorkerCallbacksCircuitBreakerSuite) TestBreakerIsPerTaskQueue() {
	env := s.newCircuitBreakerEnv(fastUpstreamTimeoutOpts()...)
	t := s.T()
	endpointName := env.createSyncSuccessEndpoint(s.Context(), t, "operation-result")

	unservedTaskQueue := testcore.RandomizeStr(t.Name() + "-unserved")
	blockedOp := s.startSANOWithCallbacks(env, t, endpointName, workerCallbackTo(unservedTaskQueue))
	s.awaitCallbackState(env, t, blockedOp, 0, 1, enumspb.CALLBACK_STATE_BLOCKED)

	// With the breaker for the dead task queue now open, a delivery to a healthy one still goes
	// through — it is keyed by its own task queue.
	servedTaskQueue := testcore.RandomizeStr(t.Name() + "-served")
	s.startHandler(env, t, servedTaskQueue, 0)
	servedOp := s.startSANOWithCallbacks(env, t, endpointName, workerCallbackTo(servedTaskQueue))
	s.awaitCallbackState(env, t, servedOp, 0, 1,
		enumspb.CALLBACK_STATE_SUCCEEDED, enumspb.CALLBACK_STATE_BLOCKED)
}

// TestRecoversFromBlocked covers BLOCKED not being terminal: once the breaker's open period elapses
// it half-opens, and a worker that has since shown up gets the delivery.
func (s *WorkerCallbacksCircuitBreakerSuite) TestRecoversFromBlocked() {
	env := s.newCircuitBreakerEnv(append(
		fastUpstreamTimeoutOpts(),
		// Shorten the open period so the breaker half-opens within the test rather than after the
		// default minute.
		testcore.WithDynamicConfig(dynamicconfig.OutboundQueueCircuitBreakerSettings,
			dynamicconfig.CircuitBreakerSettings{Timeout: time.Second}),
	)...)
	t := s.T()

	taskQueue := testcore.RandomizeStr(t.Name() + "-late")
	endpointName := env.createSyncSuccessEndpoint(s.Context(), t, "operation-result")
	operationID := s.startSANOWithCallbacks(env, t, endpointName, workerCallbackTo(taskQueue))

	// No poller yet, so deliveries fail until the breaker opens.
	s.awaitCallbackState(env, t, operationID, 0, 1, enumspb.CALLBACK_STATE_BLOCKED)

	// The worker arrives late. The breaker half-opens, lets a delivery through, and it succeeds.
	s.startHandler(env, t, taskQueue, 0)
	s.awaitCallbackState(env, t, operationID, 0, 1, enumspb.CALLBACK_STATE_SUCCEEDED)
}
