package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kmetalv1alpha1 "github.com/karavy/k8s-vdc-creation/api/v1alpha1"
	"github.com/karavy/k8s-vdc-creation/internal/controller/terraform"
)

const (
	workflowPhasePending   = "Pending"
	workflowPhaseRunning   = "Running"
	workflowPhaseSucceeded = "Succeeded"
	callPhaseSucceeded     = "Succeeded"
	workflowPhaseFailed    = "Failed"
	workflowPhaseUnknown   = "Unknown"
	stepPhasePending       = "Pending"
	stepPhaseRunning       = "Running"
	stepPhasePolling       = "Polling"
	stepPhaseSucceeded     = "Succeeded"
	stepPhaseFailed        = "Failed"
	stepPhaseUnknown       = "Unknown"
	workflowPollInterval   = 2 * time.Second
)

var errTemplateUnavailable = errors.New("referenced ApiCallTemplate was not found")

var dnsSanitizer = regexp.MustCompile(`[^a-z0-9-]+`)

// Handler sequences child ApiCall resources and Terraform Jobs without waiting in Reconcile.
type Handler struct {
	client.Client
	Scheme               *runtime.Scheme
	TerraformRunnerImage string
	// APIReader reads Secrets and ConfigMaps without a cluster-wide cache; Client is used when nil.
	APIReader client.Reader
	// TerraformState is the storage of every Terraform state; Terraform steps need it.
	TerraformState terraform.StateConfig
}

func (r *Handler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workflow := &kmetalv1alpha1.Workflow{}
	if err := r.Get(ctx, req.NamespacedName, workflow); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !workflow.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if workflow.Status.ObservedGeneration != workflow.Generation {
		workflow.Status = kmetalv1alpha1.WorkflowStatus{
			ObservedGeneration: workflow.Generation,
			Phase:              workflowPhasePending,
			Steps:              initialStepStatuses(workflow.Spec.Steps),
		}
		if err := validateReferences(workflow.Spec.Steps); err != nil {
			workflow.Status.Phase = workflowPhaseFailed
			setWorkflowCondition(workflow, metav1.ConditionFalse, "Ready", "InvalidReference", err.Error())
			return ctrl.Result{}, r.Status().Update(ctx, workflow)
		}
		setWorkflowCondition(workflow, metav1.ConditionTrue, "Progressing", "Started", "Workflow execution initialized")
		if err := r.Status().Update(ctx, workflow); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if workflow.Status.Phase == workflowPhaseSucceeded || workflow.Status.Phase == workflowPhaseFailed || workflow.Status.Phase == workflowPhaseUnknown {
		return ctrl.Result{}, nil
	}
	if int(workflow.Status.CurrentStep) >= len(workflow.Spec.Steps) {
		workflow.Status.Phase = workflowPhaseSucceeded
		setWorkflowCondition(workflow, metav1.ConditionTrue, "Ready", "Completed", "All workflow steps completed")
		return ctrl.Result{}, r.Status().Update(ctx, workflow)
	}
	workflow.Status.Phase = workflowPhaseRunning
	step := workflow.Spec.Steps[workflow.Status.CurrentStep]
	stepStatus := findStepStatus(&workflow.Status, step.Name)
	if stepStatus == nil {
		return ctrl.Result{}, fmt.Errorf("workflow status has no entry for step %q", step.Name)
	}
	if step.API != nil {
		return r.reconcileAPIStep(ctx, workflow, step, stepStatus)
	}
	if step.Terraform != nil {
		return r.reconcileTerraformStep(ctx, workflow, step, stepStatus)
	}
	return r.failWorkflow(ctx, workflow, stepStatus, "InvalidStep", "Workflow step has no operation")
}

func initialStepStatuses(steps []kmetalv1alpha1.WorkflowStep) []kmetalv1alpha1.WorkflowStepStatus {
	statuses := make([]kmetalv1alpha1.WorkflowStepStatus, 0, len(steps))
	for _, step := range steps {
		statuses = append(statuses, kmetalv1alpha1.WorkflowStepStatus{Name: step.Name, Phase: stepPhasePending})
	}
	return statuses
}

func findStepStatus(status *kmetalv1alpha1.WorkflowStatus, name string) *kmetalv1alpha1.WorkflowStepStatus {
	for i := range status.Steps {
		if status.Steps[i].Name == name {
			return &status.Steps[i]
		}
	}
	return nil
}

func (r *Handler) reconcileAPIStep(ctx context.Context, workflow *kmetalv1alpha1.Workflow, step kmetalv1alpha1.WorkflowStep, state *kmetalv1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	apiStep := step.API
	if state.Phase == stepPhasePending {
		call, err := r.buildAPICall(ctx, workflow, step.Name, "initial", apiStep.TemplateRef.Name, apiStep.Inputs)
		if errors.Is(err, errTemplateUnavailable) {
			return r.failWorkflow(ctx, workflow, state, "ApiCallTemplateUnavailable", fmt.Sprintf("Referenced ApiCallTemplate %q was not found", apiStep.TemplateRef.Name))
		}
		if err != nil {
			return r.failWorkflow(ctx, workflow, state, "RequestBuildFailed", "Could not build the API request from the published step outputs")
		}
		if err := r.createOwned(ctx, workflow, call); err != nil {
			return ctrl.Result{}, err
		}
		state.Phase = stepPhaseRunning
		state.APICallName = call.Name
		state.Attempts = 1
		state.Message = "Initial API request submitted"
		return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, workflow)
	}
	if state.APICallName == "" {
		return r.createPollCall(ctx, workflow, step, state)
	}
	call := &kmetalv1alpha1.ApiCall{}
	key := types.NamespacedName{Namespace: workflow.Namespace, Name: state.APICallName}
	if err := r.Get(ctx, key, call); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failWorkflow(ctx, workflow, state, "ChildCallLost", "Workflow child ApiCall was not found")
		}
		return ctrl.Result{}, err
	}
	switch call.Status.Phase {
	case "Pending", "Running", "":
		return ctrl.Result{RequeueAfter: workflowPollInterval}, nil
	case "Unknown":
		return r.failWorkflow(ctx, workflow, state, "ChildCallUnknown", "Child ApiCall execution is unknown; workflow will not replay it")
	case "Failed":
		if state.Phase == stepPhasePolling && pollFailureTolerated(call, apiStep.Poll) {
			return r.scheduleNextPoll(ctx, workflow, apiStep.Poll, state, "Poll request failed with a tolerated error; another poll will be scheduled")
		}
		return r.failWorkflow(ctx, workflow, state, "ChildCallFailed", fmt.Sprintf("API step %q failed", step.Name))
	case callPhaseSucceeded:
	default:
		return r.failWorkflow(ctx, workflow, state, "InvalidChildPhase", "Child ApiCall reported an invalid phase")
	}
	if call.Status.Response == nil || call.Status.Response.Truncated {
		return r.failWorkflow(ctx, workflow, state, "ResponseUnavailable", "API response is missing or truncated")
	}
	if state.Phase == stepPhaseRunning && apiStep.Poll != nil {
		if err := r.captureOutputs(ctx, workflow, state, apiStep.Outputs, call.Status.Response.Body); err != nil {
			return r.outputError(ctx, workflow, state, err)
		}
		state.Phase = stepPhasePolling
		state.APICallName = ""
		state.Attempts = 0
		state.Message = "Initial request completed; polling for readiness"
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, workflow)
	}
	if state.Phase == stepPhasePolling {
		ready, err := pollCondition(call.Status.Response.Body, apiStep.Poll)
		if err != nil {
			return r.failWorkflow(ctx, workflow, state, "PollResponseInvalid", "Polling response did not contain valid JSON at the configured pointer")
		}
		if ready {
			if err := r.captureOutputs(ctx, workflow, state, apiStep.Poll.Outputs, call.Status.Response.Body); err != nil {
				return r.outputError(ctx, workflow, state, err)
			}
			state.Phase = stepPhaseSucceeded
			state.Message = "Polling condition satisfied"
			workflow.Status.CurrentStep++
			return ctrl.Result{Requeue: true}, r.Status().Update(ctx, workflow)
		}
		return r.scheduleNextPoll(ctx, workflow, apiStep.Poll, state, "Polling condition not satisfied; another poll will be scheduled")
	}
	if err := r.captureOutputs(ctx, workflow, state, apiStep.Outputs, call.Status.Response.Body); err != nil {
		return r.outputError(ctx, workflow, state, err)
	}
	state.Phase = stepPhaseSucceeded
	state.Message = fmt.Sprintf("API step completed with HTTP %d", call.Status.Response.StatusCode)
	workflow.Status.CurrentStep++
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, workflow)
}

// scheduleNextPoll ends the current poll attempt as "not ready": it fails the workflow
// once maxAttempts is used up, otherwise it requeues after the poll interval.
func (r *Handler) scheduleNextPoll(ctx context.Context, workflow *kmetalv1alpha1.Workflow, poll *kmetalv1alpha1.WorkflowPoll, state *kmetalv1alpha1.WorkflowStepStatus, message string) (ctrl.Result, error) {
	maxAttempts := poll.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 20
	}
	if state.Attempts >= maxAttempts {
		return r.failWorkflow(ctx, workflow, state, "PollTimeout", "Polling condition was not satisfied within maxAttempts")
	}
	state.APICallName = ""
	state.Message = message
	interval := poll.IntervalSeconds
	if interval == 0 {
		interval = 5
	}
	return ctrl.Result{RequeueAfter: time.Duration(interval) * time.Second}, r.Status().Update(ctx, workflow)
}

// pollFailureTolerated reports whether a failed poll ApiCall is one the poll was
// configured to treat as "not ready yet". Invalid requests, oversized responses and
// lost executions are never tolerated.
func pollFailureTolerated(call *kmetalv1alpha1.ApiCall, poll *kmetalv1alpha1.WorkflowPoll) bool {
	ready := meta.FindStatusCondition(call.Status.Conditions, "Ready")
	if ready == nil {
		return false
	}
	switch ready.Reason {
	case "HTTPError":
		return call.Status.Response != nil && slices.Contains(poll.RetryOnStatusCodes, int32(call.Status.Response.StatusCode))
	case "Timeout", "RequestFailed":
		return poll.RetryOnTransportErrors
	}
	return false
}

func (r *Handler) createPollCall(ctx context.Context, workflow *kmetalv1alpha1.Workflow, step kmetalv1alpha1.WorkflowStep, state *kmetalv1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	poll := step.API.Poll
	if poll == nil {
		return r.failWorkflow(ctx, workflow, state, "PollMissing", "Polling state has no poll configuration")
	}
	state.Attempts++
	call, err := r.buildAPICall(ctx, workflow, step.Name, fmt.Sprintf("poll-%02d", state.Attempts), poll.TemplateRef.Name, poll.Inputs)
	if errors.Is(err, errTemplateUnavailable) {
		state.Attempts--
		return r.failWorkflow(ctx, workflow, state, "ApiCallTemplateUnavailable", fmt.Sprintf("Referenced ApiCallTemplate %q was not found", poll.TemplateRef.Name))
	}
	if err != nil {
		return r.failWorkflow(ctx, workflow, state, "PollBuildFailed", "Could not build the polling request from the published step outputs")
	}
	if err := r.createOwned(ctx, workflow, call); err != nil {
		state.Attempts--
		return ctrl.Result{}, err
	}
	state.APICallName = call.Name
	state.Message = fmt.Sprintf("Polling attempt %d submitted", state.Attempts)
	return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, workflow)
}

// buildAPICall returns a Workflow-owned ApiCall created from the named ApiCallTemplate.
// Templates are never executed; the ApiCall controller only runs Workflow-owned calls.
func (r *Handler) buildAPICall(ctx context.Context, workflow *kmetalv1alpha1.Workflow, stepName, suffix, templateName string, inputs []kmetalv1alpha1.WorkflowInput) (*kmetalv1alpha1.ApiCall, error) {
	template := &kmetalv1alpha1.ApiCallTemplate{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workflow.Namespace, Name: templateName}, template); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errTemplateUnavailable
		}
		return nil, err
	}
	spec := &kmetalv1alpha1.ApiCallSpec{
		Method: template.Spec.Method, Endpoint: template.Spec.Endpoint, Body: template.Spec.Body,
		Headers: maps.Clone(template.Spec.Headers), Authentication: template.Spec.Authentication.DeepCopy(),
		TimeoutSeconds: template.Spec.TimeoutSeconds,
	}
	if len(inputs) > 0 {
		body, err := applyInputs([]byte(spec.Body), inputs, r.outputResolver(ctx, workflow))
		if err != nil {
			return nil, err
		}
		spec.Body = string(body)
	}
	// The Workflow reads each response to map it into later steps.
	spec.StoreResponseBody = new(true)
	if spec.TimeoutSeconds == 0 {
		spec.TimeoutSeconds = 120
	}
	call := &kmetalv1alpha1.ApiCall{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workflow.Namespace,
			Name:      workflowChildName(workflow, stepName, suffix),
			Labels: map[string]string{
				"kmetal.dinova.one/workflow":      workflow.Name,
				"kmetal.dinova.one/workflow-step": stepName,
			},
			Annotations: map[string]string{"kmetal.dinova.one/api-call-template": templateName},
		},
		Spec: *spec,
	}
	return call, controllerutil.SetControllerReference(workflow, call, r.Scheme)
}

func (r *Handler) reconcileTerraformStep(ctx context.Context, workflow *kmetalv1alpha1.Workflow, step kmetalv1alpha1.WorkflowStep, state *kmetalv1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	tf := step.Terraform
	if state.Phase == stepPhasePending {
		workspace, err := r.ensureWorkspacePVC(ctx, workflow)
		if err != nil {
			return r.failWorkflow(ctx, workflow, state, "WorkspaceFailed", "Could not create Terraform workspace PVC")
		}
		// The Job is created even if the claim is still Pending: storage classes with
		// WaitForFirstConsumer (Kind's default) bind a volume only once a pod uses it, so
		// waiting for Bound here would wait forever. The Job's deadline bounds a claim that
		// never binds.
		if !r.TerraformState.Configured() {
			return r.failWorkflow(ctx, workflow, state, "StateBackendNotConfigured", "The operator has no Terraform state storage configured (--tfstate-s3-* flags)")
		}
		stateKey, err := renderStateKey(tf.State.Key, r.outputResolver(ctx, workflow))
		if err != nil {
			return r.failWorkflow(ctx, workflow, state, "StateKeyInvalid", err.Error())
		}
		backend := r.TerraformState.Backend(terraform.ObjectKey(workflow.Namespace, stateKey))
		if err := backend.Validate(); err != nil {
			return r.failWorkflow(ctx, workflow, state, "StateKeyInvalid", err.Error())
		}
		state.StateKey = backend.Key
		template := &kmetalv1alpha1.TerraformTemplate{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: workflow.Namespace, Name: tf.TemplateRef.Name}, template); err != nil {
			if apierrors.IsNotFound(err) {
				return r.failWorkflow(ctx, workflow, state, "TerraformTemplateUnavailable", fmt.Sprintf("Referenced TerraformTemplate %q was not found", tf.TemplateRef.Name))
			}
			return ctrl.Result{}, err
		}
		files, err := terraform.ConfigMapData(template)
		if err != nil {
			return r.failWorkflow(ctx, workflow, state, "TerraformTemplateInvalid", err.Error())
		}
		// The files are copied now, so later edits of the template never change a running step.
		templateFiles := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: workflow.Namespace, Name: workflowChildName(workflow, step.Name, "tf-template"),
				Labels:      map[string]string{"kmetal.dinova.one/workflow": workflow.Name},
				Annotations: map[string]string{"kmetal.dinova.one/terraform-template": tf.TemplateRef.Name},
			},
			Data: files,
		}
		if err := r.createOwned(ctx, workflow, templateFiles); err != nil {
			return ctrl.Result{}, err
		}
		variables, err := applyInputs([]byte(`{}`), tf.Inputs, r.outputResolver(ctx, workflow))
		if err != nil {
			return r.failWorkflow(ctx, workflow, state, "VariablesMappingFailed", "Could not build Terraform variables from the published step outputs")
		}
		inputSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: workflow.Namespace, Name: workflowChildName(workflow, step.Name, "tf-input"),
				Labels: map[string]string{"kmetal.dinova.one/workflow": workflow.Name},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{terraform.VariablesKey: variables},
		}
		if err := controllerutil.SetControllerReference(workflow, inputSecret, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.createOwned(ctx, workflow, inputSecret); err != nil {
			return ctrl.Result{}, err
		}
		job, err := terraform.NewJob(terraform.JobConfig{
			Namespace: workflow.Namespace, Name: workflowChildName(workflow, step.Name, "terraform"), StepName: step.Name,
			RunnerImage: r.TerraformRunnerImage, WorkspacePVC: workspace.Name,
			TemplateConfigMap: templateFiles.Name, InputSecret: inputSecret.Name,
			TimeoutSeconds: tf.TimeoutSeconds, EnvFromSecrets: tf.EnvFromSecrets, State: backend,
			Labels: map[string]string{"kmetal.dinova.one/workflow": workflow.Name},
		})
		if err != nil {
			return r.failWorkflow(ctx, workflow, state, "TerraformJobInvalid", "Could not prepare Terraform Job")
		}
		if err := r.createOwned(ctx, workflow, job); err != nil {
			return ctrl.Result{}, err
		}
		state.Phase = stepPhaseRunning
		state.JobName = job.Name
		state.Message = "Terraform Job submitted"
		return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, workflow)
	}
	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workflow.Namespace, Name: state.JobName}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failWorkflow(ctx, workflow, state, "TerraformJobLost", "Terraform Job was not found")
		}
		return ctrl.Result{}, err
	}
	if job.Status.Succeeded > 0 {
		if err := r.captureTerraformOutputs(ctx, workflow, state, tf); err != nil {
			return r.outputError(ctx, workflow, state, err)
		}
		state.Phase = stepPhaseSucceeded
		state.Message = "Terraform apply completed"
		workflow.Status.CurrentStep++
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, workflow)
	}
	if job.Status.Failed > 0 || job.Status.Active == 0 && job.Status.StartTime != nil && time.Since(job.Status.StartTime.Time) > 2*time.Minute {
		return r.failWorkflow(ctx, workflow, state, "TerraformJobFailed", "Terraform Job failed; inspect its pod logs")
	}
	return ctrl.Result{RequeueAfter: workflowPollInterval}, nil
}

// captureTerraformOutputs publishes the step's declared outputs from the Terraform state
// the Job has just written. Without declared outputs the state is not even read.
func (r *Handler) captureTerraformOutputs(ctx context.Context, workflow *kmetalv1alpha1.Workflow, state *kmetalv1alpha1.WorkflowStepStatus, tf *kmetalv1alpha1.WorkflowTerraformStep) error {
	if len(tf.Outputs) == 0 {
		return nil
	}
	raw, err := r.TerraformState.Reader.Read(ctx, state.StateKey)
	switch {
	case errors.Is(err, terraform.ErrStateNotFound):
		return &outputFailure{"StateNotFound", "The Terraform state was not found after the Job succeeded"}
	case err != nil:
		return err // transient: the reconciler retries
	}
	document, sensitive, err := terraform.StateOutputs(raw)
	if err != nil {
		return &outputFailure{"StateInvalid", "The Terraform state could not be read for its outputs"}
	}
	return r.captureOutputs(ctx, workflow, state, tf.Outputs, document, sensitiveDeclared(tf.Outputs, sensitive)...)
}

// sensitiveDeclared returns the names of the declared outputs that read from a Terraform
// output marked sensitive. An output reads from the Terraform output named by the first
// segment of its pointer, so "/db/password" is sensitive when the "db" output is.
func sensitiveDeclared(declared []kmetalv1alpha1.WorkflowOutput, sensitiveTerraformOutputs []string) []string {
	var names []string
	for _, output := range declared {
		segments, err := pointerSegments(output.From)
		if err == nil && len(segments) > 0 && slices.Contains(sensitiveTerraformOutputs, segments[0]) {
			names = append(names, output.Name)
		}
	}
	return names
}

func (r *Handler) ensureWorkspacePVC(ctx context.Context, workflow *kmetalv1alpha1.Workflow) (*corev1.PersistentVolumeClaim, error) {
	name := workflowChildName(workflow, "workspace", "pvc")
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: workflow.Namespace, Name: name}, pvc)
	if err == nil {
		workflow.Status.WorkspacePVCName = name
		return pvc, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	storage := workflow.Spec.WorkspaceStorage
	if storage == "" {
		storage = "2Gi"
	}
	quantity, err := resource.ParseQuantity(storage)
	if err != nil {
		return nil, err
	}
	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: workflow.Namespace, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: quantity}},
		},
	}
	if err := controllerutil.SetControllerReference(workflow, pvc, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	workflow.Status.WorkspacePVCName = name
	return pvc, nil
}

func pollCondition(response []byte, poll *kmetalv1alpha1.WorkflowPoll) (bool, error) {
	var value any
	if err := json.Unmarshal(response, &value); err != nil {
		return false, err
	}
	actual, err := getPointer(value, poll.ConditionJSONPointer)
	if err != nil {
		return false, err
	}
	if poll.EqualsJSON == "" {
		return true, nil
	}
	var expected any
	if err := json.Unmarshal([]byte(poll.EqualsJSON), &expected); err != nil {
		return false, err
	}
	return jsonEqual(actual, expected), nil
}

func jsonEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func (r *Handler) createOwned(ctx context.Context, owner, object client.Object) error {
	if err := controllerutil.SetControllerReference(owner, object, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, object); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// outputError fails the workflow for output problems that retrying cannot fix and
// returns any other error so that the reconciler retries.
func (r *Handler) outputError(ctx context.Context, workflow *kmetalv1alpha1.Workflow, state *kmetalv1alpha1.WorkflowStepStatus, err error) (ctrl.Result, error) {
	var failure *outputFailure
	if errors.As(err, &failure) {
		return r.failWorkflow(ctx, workflow, state, failure.reason, failure.message)
	}
	return ctrl.Result{}, err
}

func (r *Handler) failWorkflow(ctx context.Context, workflow *kmetalv1alpha1.Workflow, state *kmetalv1alpha1.WorkflowStepStatus, reason, message string) (ctrl.Result, error) {
	state.Phase, state.Message = stepPhaseFailed, message
	workflow.Status.Phase = workflowPhaseFailed
	setWorkflowCondition(workflow, metav1.ConditionFalse, "Ready", reason, message)
	return ctrl.Result{}, r.Status().Update(ctx, workflow)
}

func setWorkflowCondition(workflow *kmetalv1alpha1.Workflow, status metav1.ConditionStatus, conditionType, reason, message string) {
	meta.SetStatusCondition(&workflow.Status.Conditions, metav1.Condition{
		Type: conditionType, Status: status, ObservedGeneration: workflow.Generation,
		Reason: reason, Message: message,
	})
}

func workflowChildName(workflow *kmetalv1alpha1.Workflow, step, suffix string) string {
	uid := string(workflow.UID)
	if len(uid) > 6 {
		uid = uid[:6]
	}
	base := strings.Trim(dnsSanitizer.ReplaceAllString(strings.ToLower(workflow.Name+"-"+step), "-"), "-")
	tail := "-" + uid + "-" + suffix
	maxBase := 63 - len(tail)
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + tail
}
