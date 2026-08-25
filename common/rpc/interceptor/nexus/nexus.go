package nexus

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

const (
	methodNameStartNexusOp    = "StartNexusOperation"
	methodNameCancelNexusOp   = "CancelNexusOperation"
	methodNameCompleteNexusOp = "CompleteNexusOperation"
)

type HandlerFunc func(ctx context.Context, in InterceptorInput) (any, error)

type Interceptor func(ctx context.Context, in InterceptorInput, next HandlerFunc) (any, error)

type InterceptorInput interface {
	ServiceName() string
	OperationName() string
	NamespaceName() string // TODO: this should just use NamespaceEntry() instead
	ForwardingInfo() ForwardingInfo
	APIName() string // analogous to the gRPC FullMethod
	NamespaceEntry() (*namespace.Namespace, error)
	EndpointName() string
	MetricTags() []metrics.Tag
	Header() headers.HeaderGetter
	MethodName() string
	sealNexusOp()
}

var (
	_ InterceptorInput = StartOpInput{}
	_ InterceptorInput = CancelOpInput{}
	_ InterceptorInput = CompleteOpInput{}
)

// ForwardingInfo contains the request data needed to forward a Nexus operation.
type ForwardingInfo struct {
	OriginalRequestHeaders http.Header
	TaskQueue              string
	EndpointID             string
	EndpointName           string
	BusinessID             string
}

type InterceptorError struct {
	// wrapped error
	Err error
	// Outcome tag for metrics reporting, (draft-review: should Outcomes be enum or at least constants instead)
	Outcome string
}

func (t *InterceptorError) Error() string {
	return t.Err.Error()
}

func (t *InterceptorError) Unwrap() error {
	return t.Err
}

const (
	OutcomeInternalError = "internal_error"

	outcomeSyncSuccess   = "sync_success"
	outcomeAsyncSuccess  = "async_success"
	outcomeSuccess       = "success"
	outcomeErrorInternal = "error_internal"
)

// Outcome derives the outcome metric tag value based on the request type and its result
func Outcome(in InterceptorInput, out any, err error) string {
	if _, ok := in.(CompleteOpInput); ok {
		return completionOutcome(err)
	}
	if err != nil {
		if ie, ok := errors.AsType[*InterceptorError](err); ok && ie.Outcome != "" {
			return ie.Outcome
		}
		return OutcomeInternalError
	}
	switch out.(type) {
	case *nexus.HandlerStartOperationResultSync[any]:
		return outcomeSyncSuccess
	case *nexus.HandlerStartOperationResultAsync:
		return outcomeAsyncSuccess
	}
	return outcomeSuccess
}

func completionOutcome(err error) string {
	if err == nil {
		return outcomeSuccess
	}
	if ie, ok := errors.AsType[*InterceptorError](err); ok {
		if ie.Outcome != "" {
			return ie.Outcome
		}
		err = ie.Err
	}
	// retaining behavior
	if handlerErr, ok := errors.AsType[*nexus.HandlerError](err); ok {
		return "error_" + strings.ToLower(string(handlerErr.Type))
	}
	return outcomeErrorInternal
}

// RequestMetadata carries request metadata that is only known once the handler
// has resolved it (e.g. after a namespace registry lookup), and so cannot be supplied
// at InterceptorInput construction time. Set via nexusOpBase.WithRequestMetadata.
type RequestMetadata struct {
	APIName        string
	NamespaceEntry *namespace.Namespace
	EndpointName   string
	MetricTags     []metrics.Tag // handler-resolved frontend dynamic config for the tags to record
}

// container for ServiceName(), OperationName(), NamespaceName(), ForwardingInfo(), and
// the fields in RequestMetadata.
type nexusOpBase struct {
	serviceName, operation, namespaceName, methodName string
	forwardingInfo                                    ForwardingInfo
	requestMetadata                                   RequestMetadata
	header                                            headers.HeaderGetter
}

func (b *nexusOpBase) WithForwardingInfo(info ForwardingInfo) {
	b.forwardingInfo = info
}

func (b *nexusOpBase) WithRequestMetadata(metadata RequestMetadata) {
	b.requestMetadata = metadata
}

func (b nexusOpBase) ServiceName() string {
	return b.serviceName
}

func (b nexusOpBase) OperationName() string {
	return b.operation
}

func (b nexusOpBase) NamespaceName() string {
	return b.namespaceName
}

func (b nexusOpBase) ForwardingInfo() ForwardingInfo {
	return b.forwardingInfo
}

func (b nexusOpBase) APIName() string {
	return b.requestMetadata.APIName
}

func (b nexusOpBase) NamespaceEntry() (*namespace.Namespace, error) {
	if b.requestMetadata.NamespaceEntry == nil {
		return nil, errors.New("namespace not found in request metadata")
	}
	return b.requestMetadata.NamespaceEntry, nil
}

func (b nexusOpBase) EndpointName() string {
	return b.requestMetadata.EndpointName
}

func (b nexusOpBase) MetricTags() []metrics.Tag {
	return b.requestMetadata.MetricTags
}

func (b nexusOpBase) Header() headers.HeaderGetter {
	return b.header
}

func (b nexusOpBase) MethodName() string {
	return b.methodName
}

func (nexusOpBase) sealNexusOp() {}

type StartOpInput struct {
	nexusOpBase
	StartOperationOptions nexus.StartOperationOptions
	StartOperationInput   *nexus.LazyValue
}

func NewStartOpInput(
	serviceName string,
	operation string,
	namespaceName string,
	options nexus.StartOperationOptions,
	input *nexus.LazyValue,
) StartOpInput {
	return StartOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:   serviceName,
			operation:     operation,
			namespaceName: namespaceName,
			header:        options.Header,
			methodName:    methodNameStartNexusOp,
		},
		StartOperationOptions: options,
		StartOperationInput:   input,
	}
}

type CancelOpInput struct {
	nexusOpBase
	CancelOperationOptions nexus.CancelOperationOptions
	CancellationToken      string
}

func NewCancelOpInput(
	serviceName string,
	operation string,
	namespaceName string,
	options nexus.CancelOperationOptions,
	cancellationToken string,
) CancelOpInput {
	return CancelOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:   serviceName,
			operation:     operation,
			namespaceName: namespaceName,
			header:        options.Header,
			methodName:    methodNameCancelNexusOp,
		},
		CancelOperationOptions: options,
		CancellationToken:      cancellationToken,
	}
}

type CompleteOpInput struct {
	nexusOpBase
	CompletionRequest *nexusrpc.CompletionRequest
	Completion        *tokenspb.NexusOperationCompletion
}

// draft-review: Complete doesnt need servicename/op - verify
//
//nolint:staticcheck
func NewCompleteOpInput(
	namespaceName string,
	request *nexusrpc.CompletionRequest,
	completion *tokenspb.NexusOperationCompletion,
) (CompleteOpInput, error) {
	if request == nil || request.HTTPRequest == nil {
		return CompleteOpInput{}, errors.New("nexus completion request not found")
	}
	return CompleteOpInput{
		nexusOpBase: nexusOpBase{
			namespaceName: namespaceName,
			header:        request.HTTPRequest.Header,
			methodName:    methodNameCompleteNexusOp,
		},
		CompletionRequest: request,
		Completion:        completion,
	}, nil
}

func ChainInterceptors(final HandlerFunc, chain []Interceptor) HandlerFunc {
	for _, curr := range slices.Backward(chain) {
		next := final
		final = func(ctx context.Context, opts InterceptorInput) (any, error) {
			return curr(ctx, opts, next)
		}
	}
	return final
}
