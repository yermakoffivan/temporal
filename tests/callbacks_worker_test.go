package tests

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	chasmactivity "go.temporal.io/server/chasm/lib/activity"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Worker-variant completion callbacks deliver an execution's outcome to a Nexus service on a worker
// polling within the same namespace, rather than round tripping through the frontend's Nexus HTTP
// endpoint. They are gated per execution type by an "enabledCallbackKinds" dynamic config setting,
// which never enables the Worker kind by default.
//
// These tests exercise both sides of that gate, and then the delivery itself, for every execution
// type that accepts completion callbacks:
//
//   - without "worker" in the setting, attaching a Worker callback is rejected up front;
//   - with "worker" added to the setting, the callback is accepted and registered on the execution,
//     and is delivered to the handler its task queue, service, and operation name.
//
// Every case attaches two callbacks routed to two different handlers, so that the outcomes of
// concurrent deliveries off the same execution stay independent: one handler takes the completion
// on the first try, the other rejects it once retryably and then for good.

const (
	workerCallbackNotEnabledErr = "worker callbacks are not enabled for this execution type"

	// The messages the retry-then-fail handler answers its first and second deliveries with.
	firstDeliveryFailure  = "delivery #1"
	secondDeliveryFailure = "delivery #2"

	// The message the retried callback comes to rest on: the Nexus SDK's rendering of the
	// non-retryable handler error the second delivery is answered with.
	terminalDeliveryFailureMessage = "handler error (BAD_REQUEST): " + secondDeliveryFailure
)

// observedCallback normalizes the callback info reported by DescribeWorkflowExecution,
// DescribeActivityExecution, and DescribeNexusOperationExecution, which use different (though
// near-identical) protos.
type observedCallback struct {
	callback *commonpb.Callback
	state    enumspb.CallbackState
	// trigger is only reported for workflow executions.
	trigger                 *workflowpb.CallbackInfo_Trigger
	attempt                 int32
	lastAttemptFailure      *failurepb.Failure
	lastAttemptCompleteTime *timestamppb.Timestamp
	nextAttemptScheduleTime *timestamppb.Timestamp
}

// describeCallbacksFn reads the callbacks currently attached to an execution.
type describeCallbacksFn func() ([]observedCallback, error)

// workerCallbackHandler is a Nexus service on a worker in the test's namespace that receives
// Worker-variant completion callbacks. Every delivery is recorded, and answered by respond, so a
// test can drive the delivery outcome the server observes.
type workerCallbackHandler struct {
	taskQueue string
	service   string
	operation string
	// sourceContext is the opaque payload the callback is registered with, which the server carries
	// to the handler untouched.
	sourceContext *commonpb.Payload

	// respond decides what the handler answers the nth (1-based) delivery with. A nil error reports
	// a successful delivery.
	respond func(delivery int) error

	mu       sync.Mutex
	received []*notificationpb.OnCompleteRequest
}

// newWorkerCallbackHandler starts a worker polling its own task queue, so a delivery has to be
// routed by the callback rather than by the task queue the source execution used. The worker stops
// when t cleans up.
func newWorkerCallbackHandler(
	t *testing.T,
	client sdkclient.Client,
	name string,
	respond func(delivery int) error,
) *workerCallbackHandler {
	t.Helper()

	h := &workerCallbackHandler{
		taskQueue:     testcore.RandomizeStr(t.Name() + "-" + name),
		service:       "completion-service",
		operation:     "on-complete",
		sourceContext: payload.EncodeString("source-context-" + name),
		respond:       respond,
	}

	service := nexus.NewService(h.service)
	require.NoError(t, service.Register(nexus.NewSyncOperation(h.operation, h.handle)))

	worker := sdkworker.New(client, h.taskQueue, sdkworker.Options{})
	worker.RegisterNexusService(service)
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)
	return h
}

// newSucceedingWorkerCallbackHandler returns a handler that accepts every delivery.
func newSucceedingWorkerCallbackHandler(t *testing.T, client sdkclient.Client, name string) *workerCallbackHandler {
	return newWorkerCallbackHandler(t, client, name, func(int) error { return nil })
}

// newRetryThenFailWorkerCallbackHandler returns a handler that answers its first delivery with a
// retryable handler error and every delivery after that with a non-retryable one, so the callback
// is retried exactly once and then fails for good.
//
// UNAVAILABLE rather than UPSTREAM_TIMEOUT for the retryable answer on purpose: only the latter
// counts against the outbound queue's circuit breaker for this task queue. See
// [WorkerCallbacksCircuitBreakerSuite].
func newRetryThenFailWorkerCallbackHandler(t *testing.T, client sdkclient.Client, name string) *workerCallbackHandler {
	return newWorkerCallbackHandler(t, client, name, func(delivery int) error {
		if delivery == 1 {
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, firstDeliveryFailure)
		}
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, secondDeliveryFailure)
	})
}

func (h *workerCallbackHandler) handle(
	_ context.Context,
	req *notificationpb.OnCompleteRequest,
	_ nexus.StartOperationOptions,
) (*notificationpb.OnCompleteResponse, error) {
	h.mu.Lock()
	h.received = append(h.received, req)
	delivery := len(h.received)
	h.mu.Unlock()

	if err := h.respond(delivery); err != nil {
		return nil, err
	}
	return &notificationpb.OnCompleteResponse{}, nil
}

// callback returns a Worker-variant callback addressed to this handler.
func (h *workerCallbackHandler) callback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: h.taskQueue,
				Service:       h.service,
				Operation:     h.operation,
				SourceContext: h.sourceContext,
			},
		},
	}
}

// deliveries returns the completions the handler has received so far.
func (h *workerCallbackHandler) deliveries() []*notificationpb.OnCompleteRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.received)
}

// workerCallbackHandlers is the pair of handlers every execution type attaches a callback to: one
// that takes the completion on the first try, and one that has to be retried before it rejects it
// for good.
type workerCallbackHandlers struct {
	succeeding *workerCallbackHandler
	failing    *workerCallbackHandler
}

func newWorkerCallbackHandlers(t *testing.T, client sdkclient.Client) workerCallbackHandlers {
	t.Helper()
	return workerCallbackHandlers{
		succeeding: newSucceedingWorkerCallbackHandler(t, client, "succeeding"),
		failing:    newRetryThenFailWorkerCallbackHandler(t, client, "failing"),
	}
}

// callbacks returns the two callbacks to attach to an execution, in the order they are asserted on.
func (hs workerCallbackHandlers) callbacks() []*commonpb.Callback {
	return []*commonpb.Callback{hs.succeeding.callback(), hs.failing.callback()}
}

// requireRegistered asserts that the execution carries exactly the two Worker callbacks that were
// attached to it, keyed by the task queue each is addressed to.
func (hs workerCallbackHandlers) requireRegistered(
	t require.TestingT,
	cbs []observedCallback,
) map[string]observedCallback {
	require.Len(t, cbs, 2)

	byTaskQueue := make(map[string]observedCallback, len(cbs))
	for _, cb := range cbs {
		worker := cb.callback.GetWorker()
		require.NotNil(t, worker, "callback should round-trip as the Worker variant")
		byTaskQueue[worker.GetTaskQueueName()] = cb
	}
	require.Contains(t, byTaskQueue, hs.succeeding.taskQueue)
	require.Contains(t, byTaskQueue, hs.failing.taskQueue)
	return byTaskQueue
}

// requireStandby asserts that both callbacks are registered on an execution that has not closed
// yet, so neither has been triggered.
func (hs workerCallbackHandlers) requireStandby(t *testing.T, describe describeCallbacksFn) {
	t.Helper()
	await.Require(testcontext.For(t), t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		for _, cb := range hs.requireRegistered(c, cbs) {
			require.Equal(c, enumspb.CALLBACK_STATE_STANDBY, cb.state)
		}
	}, 15*time.Second, 200*time.Millisecond)
}

// requireExecuted waits for both callbacks to be delivered and reach a terminal state, then asserts
// the outcome each handler drove: the succeeding one is done after a single attempt, and the
// retry-then-fail one is failed after exactly one retry, carrying the handler's own message.
//
// It returns the observed callbacks keyed by task queue, for assertions specific to an execution
// type.
func (hs workerCallbackHandlers) requireExecuted(
	t *testing.T,
	describe describeCallbacksFn,
) map[string]observedCallback {
	t.Helper()

	var byTaskQueue map[string]observedCallback
	await.Require(testcontext.For(t), t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		byTaskQueue = hs.requireRegistered(c, cbs)
		require.Equal(c, enumspb.CALLBACK_STATE_SUCCEEDED, byTaskQueue[hs.succeeding.taskQueue].state)
		require.Equal(c, enumspb.CALLBACK_STATE_FAILED, byTaskQueue[hs.failing.taskQueue].state)
	}, 30*time.Second, 200*time.Millisecond)

	// The first handler took the completion on the first try, so there was nothing to retry.
	succeeded := byTaskQueue[hs.succeeding.taskQueue]
	require.EqualValues(t, 1, succeeded.attempt)
	require.Nil(t, succeeded.lastAttemptFailure)
	require.NotNil(t, succeeded.lastAttemptCompleteTime)
	require.Len(t, hs.succeeding.deliveries(), 1)

	// The second handler rejected the first delivery retryably and the second permanently, so the
	// callback was retried exactly once and then came to rest on the second answer.
	failed := byTaskQueue[hs.failing.taskQueue]
	require.EqualValues(t, 2, failed.attempt, "the callback should be retried exactly once")
	require.Len(t, hs.failing.deliveries(), 2)
	require.Equal(t, terminalDeliveryFailureMessage, failed.lastAttemptFailure.GetMessage())
	require.True(t, failed.lastAttemptFailure.GetApplicationFailureInfo().GetNonRetryable())
	require.NotNil(t, failed.lastAttemptCompleteTime)
	require.Nil(t, failed.nextAttemptScheduleTime, "a failed callback is not scheduled for another attempt")

	// Every delivery carried the execution's outcome and the context its own callback was
	// registered with.
	for _, h := range []*workerCallbackHandler{hs.succeeding, hs.failing} {
		for i, delivered := range h.deliveries() {
			require.NotNil(t, delivered.GetSuccess(), "delivery %d to %s", i+1, h.taskQueue)
			require.Nil(t, delivered.GetFailure(), "delivery %d to %s", i+1, h.taskQueue)
			protorequire.ProtoEqual(t, h.sourceContext, delivered.GetSourceContext())
		}
	}

	return byTaskQueue
}

func TestWorkerCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("Workflow", testWorkerCallbackOnWorkflow)
	t.Run("WorkflowUpdate", testWorkerCallbackOnWorkflowUpdate)
	t.Run("StandaloneActivity", testWorkerCallbackOnStandaloneActivity)
	t.Run("StandaloneNexusOperation", testWorkerCallbackOnStandaloneNexusOperation)
}

// testWorkerCallbackOnWorkflow attaches Worker callbacks to a workflow execution via
// StartWorkflowExecution.
func testWorkerCallbackOnWorkflow(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
	)
	ctx := testcontext.For(t)

	workflowType := "worker-callback-workflow"
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: workflowType})

	handlers := newWorkerCallbackHandlers(t, env.SdkClient())
	newStartRequest := func() *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          testcore.RandomizeStr("worker-callback-workflow"),
			WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			Identity:            t.Name(),
			CompletionCallbacks: handlers.callbacks(),
		}
	}

	// With the setting at its Nexus-only default, the callbacks are rejected before the workflow is
	// created.
	_, err := env.FrontendClient().StartWorkflowExecution(ctx, newStartRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmcallback.WorkflowEnabledKinds, []string{"nexus", "worker"})

	req := newStartRequest()
	_, err = env.FrontendClient().StartWorkflowExecution(ctx, req)
	require.NoError(t, err)

	describe := describeWorkflowCallbacks(ctx, env, req.WorkflowId, "")

	// Both callbacks are registered on the running workflow, and neither has been triggered yet.
	handlers.requireStandby(t, describe)

	// Close the workflow, which triggers the callbacks.
	require.NoError(t, env.SdkClient().SignalWorkflow(ctx, req.WorkflowId, "", "continue", nil))
	require.NoError(t, env.SdkClient().GetWorkflow(ctx, req.WorkflowId, "").Get(ctx, nil))

	for _, cb := range handlers.requireExecuted(t, describe) {
		require.NotNil(t, cb.trigger.GetWorkflowClosed(),
			"callback should be triggered by the workflow closing")
	}
}

// testWorkerCallbackOnWorkflowUpdate attaches Worker callbacks to a workflow update via
// UpdateWorkflowExecution.
func testWorkerCallbackOnWorkflowUpdate(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowUpdateCallbacks, true),
	)
	ctx := testcontext.For(t)

	const updateName = "update"
	const workflowType = "worker-callback-update-workflow"
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		if err := workflow.SetUpdateHandler(ctx, updateName, func(ctx workflow.Context) (string, error) {
			return "updated", nil
		}); err != nil {
			return err
		}
		workflow.GetSignalChannel(ctx, "stop").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: workflowType})

	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowType)
	require.NoError(t, err)

	handlers := newWorkerCallbackHandlers(t, env.SdkClient())
	newUpdateRequest := func() *workflowservice.UpdateWorkflowExecutionRequest {
		return &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
			},
			Request: &updatepb.Request{
				Meta:                &updatepb.Meta{UpdateId: uuid.NewString()},
				Input:               &updatepb.Input{Name: updateName},
				RequestId:           uuid.NewString(),
				CompletionCallbacks: handlers.callbacks(),
			},
		}
	}

	// With the setting at its Nexus-only default, the callbacks are rejected before the update is
	// admitted.
	_, err = env.FrontendClient().UpdateWorkflowExecution(ctx, newUpdateRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmcallback.WorkflowUpdateEnabledKinds, []string{"nexus", "worker"})

	// The update runs to completion, which triggers the callbacks.
	updateResp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, newUpdateRequest())
	require.NoError(t, err)
	require.Equal(t,
		enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		updateResp.GetStage())

	describe := describeWorkflowCallbacks(ctx, env, run.GetID(), run.GetRunID())

	for _, cb := range handlers.requireExecuted(t, describe) {
		require.NotNil(t, cb.trigger.GetUpdateWorkflowExecutionCompleted(),
			"callback should be triggered by the update completing")
	}

	require.NoError(t, env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

// testWorkerCallbackOnStandaloneActivity attaches Worker callbacks to a standalone activity via
// StartActivityExecution.
func testWorkerCallbackOnStandaloneActivity(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(chasmactivity.Enabled, true),
		testcore.WithDynamicConfig(chasmactivity.EnableCallbacks, true),
	)
	ctx := testcontext.For(t)

	activityID := testcore.RandomizeStr("worker-callback-activity")
	taskQueue := testcore.RandomizeStr("worker-callback-activity-tq")

	handlers := newWorkerCallbackHandlers(t, env.SdkClient())
	newStartRequest := func() *workflowservice.StartActivityExecutionRequest {
		return &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           uuid.NewString(),
			CompletionCallbacks: handlers.callbacks(),
		}
	}

	// With the setting at its Nexus-only default, the callbacks are rejected before the activity is
	// created.
	_, err := env.FrontendClient().StartActivityExecution(ctx, newStartRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmactivity.EnabledCallbackKinds, []string{"nexus", "worker"})

	startResp, err := env.FrontendClient().StartActivityExecution(ctx, newStartRequest())
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())

	describe := describeActivityCallbacks(ctx, env, activityID, startResp.GetRunId())

	handlers.requireStandby(t, describe)

	// Close the activity, which triggers the callbacks.
	pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  defaultIdentity,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pollResp.GetTaskToken())

	_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.GetTaskToken(),
		Result:    defaultResult,
		Identity:  defaultIdentity,
	})
	require.NoError(t, err)

	handlers.requireExecuted(t, describe)
}

// testWorkerCallbackOnStandaloneNexusOperation attaches Worker callbacks to a standalone Nexus
// operation via StartNexusOperationExecution.
func testWorkerCallbackOnStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	// Unlike the other execution types, standalone Nexus operations accept no callback kinds by
	// default, so the Nexus-only baseline the rejection below exercises has to be set explicitly.
	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"nexus"}),
	)
	ctx := testcontext.For(t)

	// The endpoint the operation itself runs against. Its result is what the callbacks carry to the
	// handlers, and it completes the operation immediately, so the callbacks are triggered as soon
	// as the operation is started.
	endpointName := env.createSyncSuccessEndpoint(ctx, t, "operation-result")

	operationID := testcore.RandomizeStr("worker-callback-nexus-operation")
	handlers := newWorkerCallbackHandlers(t, env.SdkClient())
	newStartRequest := func() *workflowservice.StartNexusOperationExecutionRequest {
		return &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           uuid.NewString(),
			CompletionCallbacks: handlers.callbacks(),
		}
	}

	// With only the Nexus kind enabled, the callbacks are rejected before the operation is created.
	_, err := env.startNexusOperation(ctx, newStartRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"nexus", "worker"})

	startResp, err := env.startNexusOperation(ctx, newStartRequest())
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())

	describe := describeNexusOperationCallbacks(ctx, env, operationID, startResp.GetRunId())

	handlers.requireExecuted(t, describe)

	// Every callback on a standalone operation is triggered by the operation completing. The trigger
	// is reported by a Nexus-operation-specific proto, so it is read here rather than through
	// observedCallback.
	cbInfos := env.describeNexusOperation(ctx, t, operationID).GetCompletionCallbacks()
	require.Len(t, cbInfos, 2)
	for _, cbInfo := range cbInfos {
		require.NotNil(t, cbInfo.GetTrigger().GetOperationCompleted())
	}
}

// TestWorkerCallbackDeliversFailedOutcome covers a failed execution: the failure reaches the handler
// in place of a result, and the callback carrying it still succeeds, since reporting a failure is a
// successful delivery.
func TestWorkerCallbackDeliversFailedOutcome(t *testing.T) {
	t.Parallel()

	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
	)
	ctx := testcontext.For(t)

	const operationFailure = "deliberate failure"
	endpointName := env.createSyncFailureEndpoint(ctx, t, operationFailure)

	handler := newSucceedingWorkerCallbackHandler(t, env.SdkClient(), "succeeding")
	operationID := testcore.RandomizeStr(t.Name())
	_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:         operationID,
		Endpoint:            endpointName,
		CompletionCallbacks: []*commonpb.Callback{handler.callback()},
	})
	require.NoError(t, err)

	// A failed operation does not make for a failed callback.
	cbInfo := env.awaitCallbackInfo(ctx, t, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
	require.NotNil(t, cbInfo.GetSuccess())

	deliveries := handler.deliveries()
	require.Len(t, deliveries, 1)
	require.Nil(t, deliveries[0].GetSuccess())
	// The OperationError the server wraps the outcome in for transport is unwrapped before the
	// handler sees it, so the endpoint's own failure is what arrives.
	require.Equal(t, operationFailure, deliveries[0].GetFailure().GetCause().GetMessage())

	// The operation itself is failed, and its outcome is the same failure the callback carried.
	descResp := env.describeNexusOperation(ctx, t, operationID)
	require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())
	require.Equal(t, operationFailure, descResp.GetFailure().GetCause().GetMessage())
}

// TestOversizedWorkerCallbackFailsPermanently drives a completion past the 4 MiB gRPC servers accept
// by default, so matching rejects the dispatch with ResourceExhausted on receive. Those bytes are
// fixed — every retry sends the same ones — so the callback must fail rather than retry until it is
// abandoned, holding the task queue's circuit breaker open on the way.
func TestOversizedWorkerCallbackFailsPermanently(t *testing.T) {
	t.Parallel()

	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
		// Raise the source context caps so this test reaches the transport limit rather than the
		// validator, whose aggregate cap of 2 MiB would otherwise reject the request first.
		testcore.WithDynamicConfig(chasmcallback.WorkerSourceContextMaxSize, 8*1024*1024),
		testcore.WithDynamicConfig(chasmcallback.WorkerSourceContextAggregateMaxSize, 8*1024*1024),
	)
	ctx := testcontext.For(t)

	endpointName := env.createSyncSuccessEndpoint(ctx, t, "operation-result")

	handler := newSucceedingWorkerCallbackHandler(t, env.SdkClient(), "oversized")
	// The dispatch carries the source context as json/protobuf, which base64s the bytes and so
	// inflates them by about a third: 3.5 MiB here encodes to roughly 4.8 MiB, over the limit, while
	// the start request carrying it stays under.
	handler.sourceContext = &commonpb.Payload{Data: make([]byte, 3500*1024)}

	operationID := testcore.RandomizeStr(t.Name())
	_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:         operationID,
		Endpoint:            endpointName,
		CompletionCallbacks: []*commonpb.Callback{handler.callback()},
	})
	require.NoError(t, err)

	// The operation itself is unaffected by the callback it cannot deliver.
	await.Require(ctx, t, func(c *await.T) {
		status := env.describeNexusOperation(c.Context(), c, operationID).GetInfo().GetStatus()
		require.Equal(c, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, status)
	}, 20*time.Second, 100*time.Millisecond)

	// FAILED rather than BACKING_OFF is the whole point: the delivery is not retried.
	cbInfo := env.awaitCallbackInfo(ctx, t, operationID, enumspb.CALLBACK_STATE_FAILED)
	require.NotNil(t, cbInfo.GetFailure())
	// The size rejection describes the caller's own payload, so it is surfaced rather than blinded
	// behind a reference ID.
	require.Contains(t, cbInfo.GetFailure().GetMessage(), "larger than max")
	require.NotContains(t, cbInfo.GetFailure().GetMessage(), "reference-id")

	// Matching never got the request, so the handler never ran.
	require.Empty(t, handler.deliveries())
}

func describeWorkflowCallbacks(ctx context.Context, env *testcore.TestEnv, workflowID, runID string) describeCallbacksFn {
	return func() ([]observedCallback, error) {
		resp, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		if err != nil {
			return nil, err
		}
		cbs := make([]observedCallback, 0, len(resp.GetCallbacks()))
		for _, cb := range resp.GetCallbacks() {
			cbs = append(cbs, observedCallback{
				callback:                cb.GetCallback(),
				state:                   cb.GetState(),
				trigger:                 cb.GetTrigger(),
				attempt:                 cb.GetAttempt(),
				lastAttemptFailure:      cb.GetLastAttemptFailure(),
				lastAttemptCompleteTime: cb.GetLastAttemptCompleteTime(),
				nextAttemptScheduleTime: cb.GetNextAttemptScheduleTime(),
			})
		}
		return cbs, nil
	}
}

func describeActivityCallbacks(ctx context.Context, env *testcore.TestEnv, activityID, runID string) describeCallbacksFn {
	return func() ([]observedCallback, error) {
		resp, err := env.FrontendClient().DescribeActivityExecution(
			ctx,
			&workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
		if err != nil {
			return nil, err
		}
		cbs := make([]observedCallback, 0, len(resp.GetCallbacks()))
		for _, cb := range resp.GetCallbacks() {
			cbs = append(cbs, observedCallback{
				callback:                cb.GetInfo().GetCallback(),
				state:                   cb.GetInfo().GetState(),
				attempt:                 cb.GetInfo().GetAttempt(),
				lastAttemptFailure:      cb.GetInfo().GetLastAttemptFailure(),
				lastAttemptCompleteTime: cb.GetInfo().GetLastAttemptCompleteTime(),
				nextAttemptScheduleTime: cb.GetInfo().GetNextAttemptScheduleTime(),
			})
		}
		return cbs, nil
	}
}

func describeNexusOperationCallbacks(ctx context.Context, env *NexusTestEnv, operationID, runID string) describeCallbacksFn {
	return func() ([]observedCallback, error) {
		resp, err := env.FrontendClient().DescribeNexusOperationExecution(
			ctx,
			&workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: operationID,
				RunId:       runID,
			})
		if err != nil {
			return nil, err
		}
		cbs := make([]observedCallback, 0, len(resp.GetCompletionCallbacks()))
		for _, cb := range resp.GetCompletionCallbacks() {
			cbs = append(cbs, observedCallback{
				callback:                cb.GetInfo().GetCallback(),
				state:                   cb.GetInfo().GetState(),
				attempt:                 cb.GetInfo().GetAttempt(),
				lastAttemptFailure:      cb.GetInfo().GetLastAttemptFailure(),
				lastAttemptCompleteTime: cb.GetInfo().GetLastAttemptCompleteTime(),
				nextAttemptScheduleTime: cb.GetInfo().GetNextAttemptScheduleTime(),
			})
		}
		return cbs, nil
	}
}
