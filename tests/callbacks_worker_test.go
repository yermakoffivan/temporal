package tests

import (
	"context"
	"testing"
	"time"

<<<<<<< HEAD
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	chasmactivity "go.temporal.io/server/chasm/lib/activity"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Worker-variant completion callbacks are gated per execution type by an
// "enabledCallbackKinds" dynamic config setting, which never enables the Worker kind by default.
// These tests exercise both sides of that gate for every execution type that accepts completion
// callbacks:
//
//   - without "worker" in the setting, attaching a Worker callback is rejected up front;
//   - with "worker" added to the setting, the callback is accepted and registered on the
//     execution, is triggered when the execution closes, and then fails to be delivered.
//
// The delivery failure is expected: the server recognizes and persists the Worker variant, but
// the invocation path that hands a completion to a Nexus worker is not implemented yet. Today
// that path rejects the Worker variant before it can record an attempt, so the callback sits in
// SCHEDULED while its invocation task fails and is retried, until it is eventually DLQ'd.
// Once delivery is implemented, requireWorkerCallbackRetriedWithoutDelivery becomes the place to
// assert real delivery.

const (
	workerCallbackNotEnabledErr = "worker callbacks are not enabled for this execution type"

	workerCallbackService   = "HTTPAdapter"
	workerCallbackOperation = "DeliverAsWebhook"

	// workerCallbackInvocationTaskType is the history task type the CHASM callback invocation task
	// reports itself as, used to pick its failures out of the task_errors metric.
	workerCallbackInvocationTaskType = "OutboundActive.callback.invoke"

	// Number of failed invocation attempts that count as evidence the callback is being retried
	// rather than merely having been scheduled once.
	minWorkerCallbackInvocationFailures = 2
)

// observedCallback normalizes the callback info reported by DescribeWorkflowExecution and
// DescribeActivityExecution, which use different (though near-identical) protos.
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

func workerCallback(taskQueue string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: taskQueue,
				Service:       workerCallbackService,
				Operation:     workerCallbackOperation,
=======
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
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
)

// WorkerCallbacksSuite covers Worker-variant completion callbacks, which deliver an execution's
// outcome to a Nexus service on a worker polling within the same namespace rather than round
// tripping through the frontend's Nexus HTTP endpoint.
//
// Standalone Nexus operations are the only execution type that accepts them. Workflows, workflow
// updates, and standalone activities all pass callbacks.OnlyNexus() to the callback validator, so
// a Worker-variant callback on any of those is rejected at the frontend.
type WorkerCallbacksSuite struct {
	parallelsuite.Suite[*WorkerCallbacksSuite]
}

func TestWorkerCallbacksSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkerCallbacksSuite{})
}

func (s *WorkerCallbacksSuite) TestCompletionDeliveredToWorker() {
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
	)

	// Endpoints the standalone operations themselves invoke. Their outcome is what the callback
	// carries to the worker.
	const operationResult = "sano-op-result"
	const operationFailure = "deliberate failure"
	successEndpoint := env.createSyncSuccessEndpoint(s.Context(), s.T(), operationResult)
	failureEndpoint := env.createSyncFailureEndpoint(s.Context(), s.T(), operationFailure)

	s.Run("SucceededOperation", func(s *WorkerCallbacksSuite) {
		t := s.T()
		ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
		defer cancel()

		handler := newWorkerCallbackHandler(t, env)
		sourceContext := payload.EncodeString("source-context")
		cb := handler.callback(sourceContext)

		operationID := testcore.RandomizeStr(t.Name())
		startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            successEndpoint,
			CompletionCallbacks: []*commonpb.Callback{cb},
		})
		require.NoError(t, err)
		require.True(t, startResp.GetStarted())

		// The completion reaches the service the callback names, not the endpoint the operation ran
		// against.
		delivered := handler.awaitDelivery(ctx, t)
		require.Nil(t, delivered.GetFailure())
		require.NotNil(t, delivered.GetSuccess())
		// The endpoint's handler returned a Go string, so the result travels as its JSON encoding.
		require.JSONEq(t, `"`+operationResult+`"`, string(delivered.GetSuccess().GetData()))
		// The context the callback was registered with is carried to the handler untouched.
		protorequire.ProtoEqual(t, sourceContext, delivered.GetSourceContext())
		handler.respond(ctx, t, nil)

		// The handler accepted the delivery, so the callback is done rather than backing off for
		// another attempt.
		cbInfo := env.awaitCallbackInfo(ctx, t, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
		require.NotNil(t, cbInfo.GetSuccess())
		protorequire.ProtoEqual(t, cb, cbInfo.GetCallback())
	})

	// A failed operation delivers its failure to the same handler, in place of a result. The
	// callback itself still succeeds: reporting a failure is a successful delivery.
	s.Run("FailedOperation", func(s *WorkerCallbacksSuite) {
		t := s.T()
		ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
		defer cancel()

		handler := newWorkerCallbackHandler(t, env)
		cb := handler.callback(nil)

		operationID := testcore.RandomizeStr(t.Name())
		_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            failureEndpoint,
			CompletionCallbacks: []*commonpb.Callback{cb},
		})
		require.NoError(t, err)

		delivered := handler.awaitDelivery(ctx, t)
		require.Nil(t, delivered.GetSuccess())
		require.NotNil(t, delivered.GetFailure())
		// The OperationError the server wraps the outcome in for transport is unwrapped before the
		// handler sees it, so the endpoint's own failure is what arrives.
		require.Equal(t, operationFailure, delivered.GetFailure().GetCause().GetMessage())
		handler.respond(ctx, t, nil)

		// A failed operation does not make for a failed callback.
		cbInfo := env.awaitCallbackInfo(ctx, t, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
		require.NotNil(t, cbInfo.GetSuccess())

		// The operation itself is failed, and its outcome is the same failure the callback carried.
		descResp := env.describeNexusOperation(ctx, t, operationID)
		require.Equal(t, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())
		require.Equal(t, operationFailure, descResp.GetFailure().GetCause().GetMessage())
	})
}

// TestOversizedCompletionFailsPermanently drives a completion past the 4 MiB gRPC servers accept by
// default, so matching rejects the dispatch with ResourceExhausted on receive. Those bytes are fixed
// — every retry sends the same ones — so the callback must fail rather than retry until it is
// abandoned, holding the task queue's circuit breaker open on the way.
func (s *WorkerCallbacksSuite) TestOversizedCompletionFailsPermanently() {
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
		// Raise the source context caps so this test reaches the transport limit rather than the
		// validator. Finding #6's aggregate cap of 2 MiB would otherwise reject the request first.
		testcore.WithDynamicConfig(callback.WorkerSourceContextMaxSize, 8*1024*1024),
		testcore.WithDynamicConfig(callback.WorkerSourceContextAggregateMaxSize, 8*1024*1024),
	)

	t := s.T()
	ctx, cancel := context.WithTimeout(s.Context(), 60*time.Second)
	defer cancel()

	endpointName := env.createSyncSuccessEndpoint(ctx, t, "operation-result")

	// The dispatch carries the source context as json/protobuf, which base64s the bytes and so
	// inflates them by about a third: 3.5 MiB here encodes to roughly 4.8 MiB, over the limit, while
	// the start request carrying it stays under.
	sourceContext := &commonpb.Payload{Data: make([]byte, 3500*1024)}

	handler := newWorkerCallbackHandler(t, env)
	operationID := testcore.RandomizeStr(t.Name())
	_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:         operationID,
		Endpoint:            endpointName,
		CompletionCallbacks: []*commonpb.Callback{handler.callback(sourceContext)},
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
	require.Empty(t, handler.invocationCh)
}

// workerCallbackHandler is a Nexus service on a worker in the test namespace that receives
// Worker-variant completion callbacks. Each delivered completion is handed to the test on
// invocationCh, and the handler then blocks until the test answers on resultCh, so a test can
// drive the delivery outcome the server observes.
type workerCallbackHandler struct {
	taskQueue string
	service   string
	operation string

	invocationCh chan *notificationpb.OnCompleteRequest
	resultCh     chan error
	doneCh       chan struct{}
}

// newWorkerCallbackHandler starts a worker polling its own task queue, so a delivery has to be
// routed by the callback rather than by the task queue the source execution used. The worker stops
// when t cleans up.
func newWorkerCallbackHandler(t *testing.T, env *NexusTestEnv) *workerCallbackHandler {
	t.Helper()

	h := &workerCallbackHandler{
		taskQueue: testcore.RandomizeStr(t.Name() + "-callback-handler"),
		service:   "completion-service",
		operation: "on-complete",
		// Buffered so the server can deliver a redelivery before the test drains the first attempt.
		invocationCh: make(chan *notificationpb.OnCompleteRequest, 4),
		resultCh:     make(chan error, 4),
		doneCh:       make(chan struct{}),
	}

	service := nexus.NewService(h.service)
	require.NoError(t, service.Register(nexus.NewSyncOperation(h.operation, h.handle)))

	worker := sdkworker.New(env.SdkClient(), h.taskQueue, sdkworker.Options{})
	worker.RegisterNexusService(service)
	require.NoError(t, worker.Start())
	t.Cleanup(func() {
		// Unblock a handler still waiting on the test before stopping the worker.
		close(h.doneCh)
		worker.Stop()
	})
	return h
}

func (h *workerCallbackHandler) handle(
	ctx context.Context,
	req *notificationpb.OnCompleteRequest,
	_ nexus.StartOperationOptions,
) (*notificationpb.OnCompleteResponse, error) {
	shuttingDown := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "test is shutting down")

	select {
	case h.invocationCh <- req:
	case <-h.doneCh:
		return nil, shuttingDown
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case err := <-h.resultCh:
		if err != nil {
			return nil, err
		}
		return &notificationpb.OnCompleteResponse{}, nil
	case <-h.doneCh:
		return nil, shuttingDown
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// callback returns a Worker-variant callback addressed to this handler, carrying sourceContext.
func (h *workerCallbackHandler) callback(sourceContext *commonpb.Payload) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: h.taskQueue,
				Service:       h.service,
				Operation:     h.operation,
				SourceContext: sourceContext,
>>>>>>> 6166f7460 (Support Worker-variant callbacks)
			},
		},
	}
}

<<<<<<< HEAD
// requireWorkerCallbackRegistered asserts that the execution carries exactly the one Worker
// callback that was attached to it.
func requireWorkerCallbackRegistered(t require.TestingT, cbs []observedCallback, taskQueue string) {
	require.Len(t, cbs, 1)
	worker := cbs[0].callback.GetWorker()
	require.NotNil(t, worker, "callback should round-trip as the Worker variant")
	require.Equal(t, taskQueue, worker.GetTaskQueueName())
	require.Equal(t, workerCallbackService, worker.GetService())
	require.Equal(t, workerCallbackOperation, worker.GetOperation())
}

// requireWorkerCallbackTriggered waits for the callback to leave STANDBY, which happens once the
// execution it is attached to reaches its terminal state.
func requireWorkerCallbackTriggered(t *testing.T, describe describeCallbacksFn, taskQueue string) {
	t.Helper()
	await.Require(testcontext.For(t), t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireWorkerCallbackRegistered(c, cbs, taskQueue)
		require.NotEqual(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state,
			"callback should be triggered once the execution closes")
	}, 15*time.Second, 200*time.Millisecond)
}

// countCallbackInvocationFailures reports how many times the callback invocation task has failed
// in the test's namespace.
func countCallbackInvocationFailures(capture *testcore.NamespaceMetricCapture) int {
	return len(capture.CollectMetric("task_errors", func(rec *metricstest.CapturedRecording) bool {
		return rec.Tags[metrics.TaskTypeTagName] == workerCallbackInvocationTaskType
	}))
}

// requireWorkerCallbackRetriedWithoutDelivery waits for positive evidence that the callback's
// invocation task ran and was retried, then asserts the callback still has not been delivered.
//
// The retry evidence comes from the task_errors metric rather than from the callback itself:
// invocation rejects the Worker variant before an attempt is recorded, so the callback's attempt,
// last_attempt_failure, and next_attempt_schedule_time all stay empty no matter how many times
// the task is retried.
func requireWorkerCallbackRetriedWithoutDelivery(
	t *testing.T,
	capture *testcore.NamespaceMetricCapture,
	describe describeCallbacksFn,
) observedCallback {
	t.Helper()

	var last observedCallback
	await.Require(testcontext.For(t), t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		require.Len(c, cbs, 1)
		last = cbs[0]
		require.GreaterOrEqual(c, countCallbackInvocationFailures(capture), minWorkerCallbackInvocationFailures,
			"the callback's invocation task should run and be retried")
	}, 30*time.Second, 200*time.Millisecond)

	require.NotEqual(t, enumspb.CALLBACK_STATE_SUCCEEDED, last.state,
		"Worker callback delivery is not implemented; the callback must not report success")
	// Logged rather than asserted: exactly where a callback whose invocation never starts comes to
	// rest is an implementation detail of the unimplemented delivery path. The empty per-attempt
	// fields here are why the retry assertion above reads the task_errors metric instead.
	t.Logf("Worker callback retried without delivery. "+
		"state=%s attempt=%d last_attempt_complete_time=%v last_attempt_failure=%v next_attempt_schedule_time=%v",
		last.state, last.attempt, last.lastAttemptCompleteTime, last.lastAttemptFailure, last.nextAttemptScheduleTime)
	return last
}

func TestWorkerCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("Workflow", testWorkerCallbackOnWorkflow)
	t.Run("WorkflowUpdate", testWorkerCallbackOnWorkflowUpdate)
	t.Run("StandaloneActivity", testWorkerCallbackOnStandaloneActivity)
	t.Run("StandaloneNexusOperation", testWorkerCallbackOnStandaloneNexusOperation)
}

// testWorkerCallbackOnWorkflow attaches a Worker callback to a workflow execution via
// StartWorkflowExecution.
func testWorkerCallbackOnWorkflow(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

	workflowType := "worker-callback-workflow"
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: workflowType})

	cbTaskQueue := testcore.RandomizeStr("worker-callback-workflow-completions")
	newStartRequest := func() *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          testcore.RandomizeStr("worker-callback-workflow"),
			WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			Identity:            t.Name(),
			CompletionCallbacks: []*commonpb.Callback{workerCallback(cbTaskQueue)},
		}
	}

	// With the setting at its Nexus-only default, the callback is rejected before the workflow is
	// created.
	_, err := env.FrontendClient().StartWorkflowExecution(ctx, newStartRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmcallback.WorkflowEnabledKinds, []string{"nexus", "worker"})

	req := newStartRequest()
	_, err = env.FrontendClient().StartWorkflowExecution(ctx, req)
	require.NoError(t, err)

	describe := describeWorkflowCallbacks(ctx, env, req.WorkflowId, "")

	// The callback is registered on the running workflow, and has not been triggered yet.
	await.Require(ctx, t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireWorkerCallbackRegistered(c, cbs, cbTaskQueue)
		require.Equal(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state)
	}, 15*time.Second, 200*time.Millisecond)

	// Close the workflow, which triggers the callback.
	require.NoError(t, env.SdkClient().SignalWorkflow(ctx, req.WorkflowId, "", "continue", nil))
	require.NoError(t, env.SdkClient().GetWorkflow(ctx, req.WorkflowId, "").Get(ctx, nil))

	requireWorkerCallbackTriggered(t, describe, cbTaskQueue)

	cbInfo := requireWorkerCallbackRetriedWithoutDelivery(t, capture, describe)
	require.NotNil(t, cbInfo.trigger.GetWorkflowClosed(),
		"callback should be triggered by the workflow closing")
}

// testWorkerCallbackOnWorkflowUpdate attaches a Worker callback to a workflow update via
// UpdateWorkflowExecution.
func testWorkerCallbackOnWorkflowUpdate(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowUpdateCallbacks, true),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

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

	cbTaskQueue := testcore.RandomizeStr("worker-callback-update-completions")
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
				CompletionCallbacks: []*commonpb.Callback{workerCallback(cbTaskQueue)},
			},
		}
	}

	// With the setting at its Nexus-only default, the callback is rejected before the update is
	// admitted.
	_, err = env.FrontendClient().UpdateWorkflowExecution(ctx, newUpdateRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmcallback.WorkflowUpdateEnabledKinds, []string{"nexus", "worker"})

	// The update runs to completion, which triggers the callback.
	updateResp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, newUpdateRequest())
	require.NoError(t, err)
	require.Equal(t,
		enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		updateResp.GetStage())

	describe := describeWorkflowCallbacks(ctx, env, run.GetID(), run.GetRunID())

	requireWorkerCallbackTriggered(t, describe, cbTaskQueue)

	cbInfo := requireWorkerCallbackRetriedWithoutDelivery(t, capture, describe)
	require.NotNil(t, cbInfo.trigger.GetUpdateWorkflowExecutionCompleted(),
		"callback should be triggered by the update completing")

	require.NoError(t, env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

// testWorkerCallbackOnStandaloneActivity attaches a Worker callback to a standalone activity via
// StartActivityExecution.
func testWorkerCallbackOnStandaloneActivity(t *testing.T) {
	t.Parallel()

	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(chasmactivity.Enabled, true),
		testcore.WithDynamicConfig(chasmactivity.EnableCallbacks, true),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

	activityID := testcore.RandomizeStr("worker-callback-activity")
	taskQueue := testcore.RandomizeStr("worker-callback-activity-tq")
	cbTaskQueue := testcore.RandomizeStr("worker-callback-activity-completions")

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
			CompletionCallbacks: []*commonpb.Callback{workerCallback(cbTaskQueue)},
		}
	}

	// With the setting at its Nexus-only default, the callback is rejected before the activity is
	// created.
	_, err := env.FrontendClient().StartActivityExecution(ctx, newStartRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(chasmactivity.EnabledCallbackKinds, []string{"nexus", "worker"})

	startResp, err := env.FrontendClient().StartActivityExecution(ctx, newStartRequest())
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())

	describe := describeActivityCallbacks(ctx, env, activityID, startResp.GetRunId())

	await.Require(ctx, t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireWorkerCallbackRegistered(c, cbs, cbTaskQueue)
		require.Equal(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state)
	}, 15*time.Second, 200*time.Millisecond)

	// Close the activity, which triggers the callback.
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

	requireWorkerCallbackTriggered(t, describe, cbTaskQueue)
	requireWorkerCallbackRetriedWithoutDelivery(t, capture, describe)
}

// testWorkerCallbackOnStandaloneNexusOperation attaches a Worker callback to a standalone Nexus
// operation via StartNexusOperationExecution.
func testWorkerCallbackOnStandaloneNexusOperation(t *testing.T) {
	t.Parallel()

	// Unlike the other execution types, standalone Nexus operations accept no callback kinds by
	// default, so the Nexus-only baseline the rejection below exercises has to be set explicitly.
	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"nexus"}),
	)
	ctx := testcontext.For(t)
	capture := env.StartNamespaceMetricCapture()

	// A worker-target endpoint whose task queue nobody polls: the operation starts and then stays
	// open until the test terminates it.
	endpointName := env.createRandomNexusEndpoint(ctx, t).GetSpec().GetName()

	operationID := testcore.RandomizeStr("worker-callback-nexus-operation")
	cbTaskQueue := testcore.RandomizeStr("worker-callback-nexus-operation-completions")
	newStartRequest := func() *workflowservice.StartNexusOperationExecutionRequest {
		return &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           uuid.NewString(),
			CompletionCallbacks: []*commonpb.Callback{workerCallback(cbTaskQueue)},
		}
	}

	// With only the Nexus kind enabled, the callback is rejected before the operation is created.
	_, err := env.startNexusOperation(ctx, newStartRequest())
	require.ErrorContains(t, err, workerCallbackNotEnabledErr)

	env.OverrideDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"nexus", "worker"})

	startResp, err := env.startNexusOperation(ctx, newStartRequest())
	require.NoError(t, err)
	require.True(t, startResp.GetStarted())

	describe := describeNexusOperationCallbacks(ctx, env, operationID, startResp.GetRunId())

	// The callback is registered on the open operation, and has not been triggered yet.
	await.Require(ctx, t, func(c *await.T) {
		cbs, err := describe()
		require.NoError(c, err)
		requireWorkerCallbackRegistered(c, cbs, cbTaskQueue)
		require.Equal(c, enumspb.CALLBACK_STATE_STANDBY, cbs[0].state)
	}, 15*time.Second, 200*time.Millisecond)

	// Every callback on a standalone operation is triggered by the operation completing. The trigger
	// is reported by a Nexus-operation-specific proto, so it is read here rather than through
	// observedCallback.
	cbInfos := env.describeNexusOperation(ctx, t, operationID).GetCompletionCallbacks()
	require.Len(t, cbInfos, 1)
	require.NotNil(t, cbInfos[0].GetTrigger().GetOperationCompleted())

	// Close the operation, which triggers the callback.
	_, err = env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: operationID,
		RunId:       startResp.GetRunId(),
		RequestId:   uuid.NewString(),
		Identity:    t.Name(),
		Reason:      "close the operation to trigger its completion callback",
	})
	require.NoError(t, err)

	requireWorkerCallbackTriggered(t, describe, cbTaskQueue)
	requireWorkerCallbackRetriedWithoutDelivery(t, capture, describe)
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
=======
// awaitDelivery returns the next completion delivered to the handler. The handler stays blocked
// until [workerCallbackHandler.respond] answers it.
func (h *workerCallbackHandler) awaitDelivery(ctx context.Context, t require.TestingT) *notificationpb.OnCompleteRequest {
	select {
	case req := <-h.invocationCh:
		return req
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for the worker callback to be delivered")
		return nil
	}
}

// respond hands err back to the server as the handler's answer to a delivery. A nil err reports a
// successful delivery.
func (h *workerCallbackHandler) respond(ctx context.Context, t require.TestingT, err error) {
	select {
	case h.resultCh <- err:
	case <-ctx.Done():
		require.FailNow(t, "timed out handing the worker callback result back to the handler")
>>>>>>> 6166f7460 (Support Worker-variant callbacks)
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
