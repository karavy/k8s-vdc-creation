package apicall

import (
	"context"
	"errors"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kmetalv1alpha1 "github.com/karavy/k8s-vdc-creation/api/v1alpha1"
)

// Handler implements the ApiCall state machine used by the Kubebuilder controller.
// It never starts goroutines: HTTP execution belongs to the shared APIExecutor.
type Handler struct {
	client.Client
	APIReader      client.Reader
	Executor       *APIExecutor
	AllowedOrigins []string
}

const (
	apiPollInterval   = 2 * time.Second
	apiPhasePending   = "Pending"
	apiPhaseRunning   = "Running"
	apiPhaseSucceeded = "Succeeded"
	apiPhaseFailed    = "Failed"
	apiPhaseUnknown   = "Unknown"
)

// Reconcile prepares work and persists checkpoints/results without waiting for HTTP.
func (r *Handler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Executor == nil {
		return ctrl.Result{}, errors.New("API executor is not configured")
	}
	call := &kmetalv1alpha1.ApiCall{}
	if err := r.reader().Get(ctx, req.NamespacedName, call); err != nil {
		if apierrors.IsNotFound(err) {
			r.Executor.cancelObsolete(req.NamespacedName, nil)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	key := operationKey{Name: req.NamespacedName, UID: call.UID, Generation: call.Generation}
	if !call.DeletionTimestamp.IsZero() {
		r.Executor.cancelObsolete(req.NamespacedName, nil)
		return ctrl.Result{}, nil
	}
	if !ownedByWorkflow(call) {
		// Standalone ApiCalls are templates referenced by Workflow steps, never executed.
		r.Executor.cancelObsolete(req.NamespacedName, nil)
		return ctrl.Result{}, nil
	}
	r.Executor.cancelObsolete(req.NamespacedName, &key)
	if call.Status.ObservedGeneration != call.Generation {
		call.Status = kmetalv1alpha1.ApiCallStatus{
			ObservedGeneration: call.Generation, Phase: apiPhasePending,
			Conditions: call.Status.Conditions,
		}
		setAPIConditions(call, apiPhasePending, "Queued", "Waiting to execute the API request")
		if err := r.Status().Update(ctx, call); err != nil {
			return ctrl.Result{}, err
		}
	}
	switch call.Status.Phase {
	case apiPhaseSucceeded, apiPhaseFailed, apiPhaseUnknown:
		r.Executor.forget(key)
		return ctrl.Result{}, nil
	case apiPhaseRunning:
		return r.collectResult(ctx, call, key)
	default:
		return r.scheduleRequest(ctx, call, key)
	}
}

func (r *Handler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *Handler) scheduleRequest(ctx context.Context, call *kmetalv1alpha1.ApiCall, key operationKey) (ctrl.Result, error) {
	request, oauth, err := r.buildRequest(ctx, call)
	if err != nil {
		if errors.Is(err, errCredentialsUnavailable) {
			condition := meta.FindStatusCondition(call.Status.Conditions, "Ready")
			if condition != nil && condition.Reason == "CredentialsUnavailable" && condition.ObservedGeneration == call.Generation {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			setAPIConditions(call, apiPhasePending, "CredentialsUnavailable", "Waiting for the authentication Secret and key")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, r.Status().Update(ctx, call)
		}
		call.Status.Phase = apiPhaseFailed
		completed := metav1.Now()
		call.Status.CompletedAt = &completed
		setAPIConditions(call, apiPhaseFailed, "InvalidRequest", "Invalid method, endpoint, headers or authentication; check the operator origin allowlist")
		return ctrl.Result{}, r.Status().Update(ctx, call)
	}
	timeout := call.Spec.TimeoutSeconds
	if timeout == 0 {
		timeout = 120
	}
	store := call.Spec.StoreResponseBody == nil || *call.Spec.StoreResponseBody
	if !r.Executor.reserveWithOAuth(key, request, oauth, time.Duration(timeout)*time.Second, store) {
		return ctrl.Result{RequeueAfter: apiPollInterval}, nil
	}
	// Write before sending: after a crash an ambiguous operation is not replayed.
	call.Status.Phase = apiPhaseRunning
	started := metav1.Now()
	call.Status.StartedAt = &started
	setAPIConditions(call, apiPhaseRunning, "Executing", "API request queued or executing in a background worker")
	if err := r.Status().Update(ctx, call); err != nil {
		r.Executor.forget(key)
		return ctrl.Result{}, err
	}
	r.Executor.activate(key)
	return ctrl.Result{RequeueAfter: apiPollInterval}, nil
}

func (r *Handler) collectResult(ctx context.Context, call *kmetalv1alpha1.ApiCall, key operationKey) (ctrl.Result, error) {
	result, done, exists := r.Executor.lookup(key)
	if !exists {
		call.Status.Phase = apiPhaseUnknown
		completed := metav1.Now()
		call.Status.CompletedAt = &completed
		setAPIConditions(call, apiPhaseUnknown, "ExecutionLost", "Worker state was lost; the request may have taken effect and will not be automatically repeated")
		return ctrl.Result{}, r.Status().Update(ctx, call)
	}
	if !done {
		r.Executor.activate(key)
		return ctrl.Result{RequeueAfter: apiPollInterval}, nil
	}
	call.Status.Phase = apiPhaseFailed
	if result.Success {
		call.Status.Phase = apiPhaseSucceeded
	}
	call.Status.Response = result.Response
	call.Status.CompletedAt = &result.CompletedAt
	setAPIConditions(call, call.Status.Phase, result.Reason, result.Message)
	if err := r.Status().Update(ctx, call); err != nil {
		return ctrl.Result{}, err // Keep the result until a durable status update succeeds.
	}
	r.Executor.forget(key)
	return ctrl.Result{}, nil
}

func setAPIConditions(call *kmetalv1alpha1.ApiCall, phase, reason, message string) {
	ready, progressing := metav1.ConditionFalse, metav1.ConditionFalse
	switch phase {
	case apiPhaseSucceeded:
		ready = metav1.ConditionTrue
	case apiPhaseUnknown:
		ready = metav1.ConditionUnknown
	}
	if phase == apiPhasePending || phase == apiPhaseRunning {
		progressing = metav1.ConditionTrue
	}
	for conditionType, status := range map[string]metav1.ConditionStatus{"Ready": ready, "Progressing": progressing} {
		meta.SetStatusCondition(&call.Status.Conditions, metav1.Condition{
			Type: conditionType, Status: status, ObservedGeneration: call.Generation,
			Reason: reason, Message: message,
		})
	}
}

// ownedByWorkflow reports whether a Workflow created this ApiCall as a step copy.
func ownedByWorkflow(call *kmetalv1alpha1.ApiCall) bool {
	owner := metav1.GetControllerOf(call)
	if owner == nil || owner.Kind != "Workflow" {
		return false
	}
	group, _, _ := strings.Cut(owner.APIVersion, "/")
	return group == kmetalv1alpha1.GroupVersion.Group
}
