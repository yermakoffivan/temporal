package callbacks

import (
	"context"
	"fmt"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc/status"
)

type ValidatorOptions struct {
	// EnabledKinds are the callback kinds that may be attached to the execution being validated.
	// A client-supplied callback of any other kind is rejected with an InvalidArgument error.
	EnabledKinds []Kind
}

// Validator validates completion callbacks attached to executions (e.g. workflows and standalone activities).
type Validator interface {
	// Validate rejects callbacks that are not enabled for the execution, or are malformed.
	// Will mutate the supplied Callbacks to normalize. e.g. converting Nexus headers to lower-case.
	Validate(ctx context.Context, namespaceName string, cbs []*commonpb.Callback, opts ValidatorOptions) error

	// ValidateTotalSourceContextSize checks that adding addingBytes of Worker source context to an
	// execution already carrying existingBytes will not exceed the per-execution limit.
	ValidateTotalSourceContextSize(namespaceName string, existingBytes, addingBytes int) error
}

// SourceContextSize returns the total size in bytes of the Worker source context payloads carried
// by cbs. Callbacks of any other kind contribute nothing.
func SourceContextSize(cbs []*commonpb.Callback) int {
	total := 0
	for _, cb := range cbs {
		if sc := cb.GetWorker().GetSourceContext(); sc != nil {
			total += sc.Size()
		}
	}
	return total
}

// ValidatorConfig holds the limits a [Validator] enforces.
type ValidatorConfig struct {
	MaxCallbacksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit         dynamicconfig.IntPropertyFn // All ID types use the same global setting.

	// Nexus-variant limits.
	URLMaxLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
	HeaderMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
	EndpointRules dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]

	// Worker-variant limits.
	MaxServiceNameLength                dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength              dynamicconfig.IntPropertyFnWithNamespaceFilter
	WorkerSourceContextMaxSize          dynamicconfig.IntPropertyFnWithNamespaceFilter
	WorkerSourceContextAggregateMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func (vc *ValidatorConfig) Validate() error {
	var missingFields []string
	if vc.MaxCallbacksPerExecution == nil {
		missingFields = append(missingFields, "MaxCallbacksPerExecution")
	}
	if vc.MaxIDLengthLimit == nil {
		missingFields = append(missingFields, "MaxIDLengthLimit")
	}
	if vc.URLMaxLength == nil {
		missingFields = append(missingFields, "URLMaxLength")
	}
	if vc.HeaderMaxSize == nil {
		missingFields = append(missingFields, "HeaderMaxSize")
	}
	if vc.EndpointRules == nil {
		missingFields = append(missingFields, "EndpointRules")
	}
	if vc.MaxServiceNameLength == nil {
		missingFields = append(missingFields, "MaxServiceNameLength")
	}
	if vc.MaxOperationNameLength == nil {
		missingFields = append(missingFields, "MaxOperationNameLength")
	}
	if vc.WorkerSourceContextMaxSize == nil {
		missingFields = append(missingFields, "WorkerSourceContextMaxSize")
	}
	if vc.WorkerSourceContextAggregateMaxSize == nil {
		missingFields = append(missingFields, "WorkerSourceContextAggregateMaxSize")
	}

	if len(missingFields) != 0 {
		return fmt.Errorf("missing required fields: %v", missingFields)
	}
	return nil
}

type validator struct {
	config ValidatorConfig
}

// NewValidator returns a new Validator.
func NewValidator(config ValidatorConfig) (Validator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &validator{config: config}, nil
}

// Validate validates completion callbacks: their kind, their count, and the fields of each variant.
// Nexus header keys are normalized to lowercase in place.
func (v *validator) Validate(
	_ context.Context,
	namespaceName string,
	cbs []*commonpb.Callback,
	opts ValidatorOptions,
) error {
	if len(cbs) > v.config.MaxCallbacksPerExecution(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"cannot attach more than %d callbacks to an execution", v.config.MaxCallbacksPerExecution(namespaceName),
		)
	}

	for i, cb := range cbs {
		if err := v.validateCallback(cb, namespaceName, opts); err != nil {
			return fmt.Errorf("completion_callbacks[%d]: %w", i, err)
		}
	}
	return nil
}

func (v *validator) validateCallback(cb *commonpb.Callback, namespaceName string, opts ValidatorOptions) error {
	kind := KindOf(cb)

	// For unknown callbacks, prefer the "unknown callback variant" error below.
	if kind != KindUnknown && !slices.Contains(opts.EnabledKinds, kind) {
		return serviceerror.NewInvalidArgumentf("%s callbacks are not enabled for this execution type", kind)
	}

	switch kind {
	case KindNexus:
		return v.validateNexus(namespaceName, cb.GetNexus())
	case KindWorker:
		return v.validateWorker(namespaceName, cb.GetWorker())
	case KindUnknown:
		fallthrough
	default:
		return serviceerror.NewUnimplementedf("unknown callback variant: %T", cb.GetVariant())
	}
}

func (v *validator) validateNexus(namespaceName string, cb *commonpb.Callback_Nexus) error {
	rawURL := cb.GetUrl()
	if len(rawURL) > v.config.URLMaxLength(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"invalid url: url length longer than max length allowed of %d",
			v.config.URLMaxLength(namespaceName),
		)
	}
	if err := v.config.EndpointRules(namespaceName).Validate(rawURL); err != nil {
		msg := err.Error()
		if s, ok := status.FromError(err); ok {
			msg = s.Message()
		}
		return serviceerror.NewInvalidArgument(msg)
	}

	// Validate total size of all headers, as well as normalize to lowercase.
	headerSize := 0
	lowerCaseHeaders := make(map[string]string, len(cb.GetHeader()))
	for k, val := range cb.GetHeader() {
		headerSize += len(k) + len(val)
		lowerCaseHeaders[strings.ToLower(k)] = val
	}
	if headerSize > v.config.HeaderMaxSize(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"invalid header: header size longer than max allowed size of %d",
			v.config.HeaderMaxSize(namespaceName),
		)
	}
	cb.Header = lowerCaseHeaders
	return nil
}

func (v *validator) validateWorker(namespaceName string, cb *commonpb.Callback_Worker) error {
	// Task Queue
	if err := tqid.Validate(cb.GetTaskQueueName(), v.config.MaxIDLengthLimit()); err != nil {
		return err
	}

	// Nexus handler
	for _, field := range []struct {
		name      string
		value     string
		maxLength int
	}{
		{"service", cb.GetService(), v.config.MaxServiceNameLength(namespaceName)},
		{"operation", cb.GetOperation(), v.config.MaxOperationNameLength(namespaceName)},
	} {
		if field.value == "" {
			return serviceerror.NewInvalidArgumentf("%s is required", field.name)
		}
		if len(field.value) > field.maxLength {
			return serviceerror.NewInvalidArgumentf(
				"%s exceeds length limit. Length=%d Limit=%d",
				field.name, len(field.value), field.maxLength)
		}
	}

	// Source Context blob
	maxSize := v.config.WorkerSourceContextMaxSize(namespaceName)
	if size := cb.GetSourceContext().Size(); size > maxSize {
		return serviceerror.NewInvalidArgumentf(
			"source_context exceeds size limit. Length=%d Limit=%d",
			size, v.config.WorkerSourceContextMaxSize(namespaceName))
	}

	return nil
}

func (v *validator) ValidateTotalSourceContextSize(namespaceName string, existingBytes, addingBytes int) error {
	maxSize := v.config.WorkerSourceContextAggregateMaxSize(namespaceName)
	if existingBytes+addingBytes > maxSize {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d bytes of callback source_context to an execution "+
				"(%d bytes already attached, %d more requested)",
			maxSize, existingBytes, addingBytes)
	}
	return nil
}
