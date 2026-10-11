package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kmetalv1alpha1 "github.com/karavy/k8s-vdc-creation/api/v1alpha1"
)

const (
	// maxOutputValueBytes bounds one JSON-encoded output value kept in the Workflow status.
	maxOutputValueBytes = 4096
	maxOutputsPerStep   = 32
)

// outputFailure is an error that must fail the workflow with a fixed reason; any
// other error returned by output handling is transient and retried by the reconciler.
type outputFailure struct{ reason, message string }

func (e *outputFailure) Error() string { return e.message }

// extractOutputs reads every declared output from a JSON response and returns the
// JSON-encoded values by output name. A missing pointer uses DefaultJSON, or fails.
func extractOutputs(response []byte, outputs []kmetalv1alpha1.WorkflowOutput) (map[string][]byte, error) {
	if len(outputs) == 0 {
		return nil, nil
	}
	var document any
	if err := json.Unmarshal(response, &document); err != nil {
		return nil, &outputFailure{"OutputExtractionFailed", "Response is not valid JSON, so step outputs cannot be read"}
	}
	values := make(map[string][]byte, len(outputs))
	for _, output := range outputs {
		value, err := getPointer(document, output.From)
		if err != nil {
			if output.DefaultJSON == "" {
				return nil, &outputFailure{"OutputMissing", fmt.Sprintf("Output %q is missing from the response at %s and has no defaultJSON", output.Name, output.From)}
			}
			if err := json.Unmarshal([]byte(output.DefaultJSON), &value); err != nil {
				return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Output %q has an invalid defaultJSON", output.Name)}
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Output %q could not be encoded", output.Name)}
		}
		if len(encoded) > maxOutputValueBytes {
			return nil, &outputFailure{"OutputTooLarge", fmt.Sprintf("Output %q exceeds %d bytes", output.Name, maxOutputValueBytes)}
		}
		values[output.Name] = encoded
	}
	return values, nil
}

// applyInputs sets each referenced output into destination, which must be empty or a
// JSON object, and returns the resulting JSON.
func applyInputs(destination []byte, inputs []kmetalv1alpha1.WorkflowInput, resolve func(step, output string) ([]byte, error)) ([]byte, error) {
	if len(inputs) == 0 {
		return append([]byte(nil), destination...), nil
	}
	var target any = map[string]any{}
	if len(destination) > 0 {
		if err := json.Unmarshal(destination, &target); err != nil {
			return nil, fmt.Errorf("target body must be valid JSON: %w", err)
		}
	}
	object, ok := target.(map[string]any)
	if !ok {
		return nil, errors.New("target body must be a JSON object")
	}
	seen := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if _, duplicate := seen[input.To]; duplicate {
			return nil, fmt.Errorf("duplicate input target %q", input.To)
		}
		seen[input.To] = struct{}{}
		raw, err := resolve(input.Step, input.Output)
		if err != nil {
			return nil, err
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("output %s.%s is not valid JSON", input.Step, input.Output)
		}
		if err := setObjectPointer(object, input.To, value); err != nil {
			return nil, fmt.Errorf("set input target %q: %w", input.To, err)
		}
	}
	return json.Marshal(object)
}

// validateReferences checks, before any step runs, that every input points to an
// output declared by an earlier step, and that names and step names are unique.
func validateReferences(steps []kmetalv1alpha1.WorkflowStep) error {
	declared := map[string]map[string]bool{} // step -> output -> declared by the step itself
	index := map[string]int{}
	for i, step := range steps {
		if _, exists := index[step.Name]; exists {
			return fmt.Errorf("step name %q is used more than once", step.Name)
		}
		index[step.Name] = i
		own := map[string]bool{}
		for _, output := range stepOutputs(step) {
			if _, exists := own[output.Name]; exists {
				return fmt.Errorf("step %q declares output %q more than once", step.Name, output.Name)
			}
			own[output.Name] = true
		}
		declared[step.Name] = own
	}
	check := func(current int, inputs []kmetalv1alpha1.WorkflowInput, allowSameStep bool, available map[string]bool) error {
		for _, input := range inputs {
			position, known := index[input.Step]
			switch {
			case !known:
				return fmt.Errorf("step %q uses output %q of unknown step %q", steps[current].Name, input.Output, input.Step)
			case position > current || position == current && !allowSameStep:
				return fmt.Errorf("step %q can only use outputs of earlier steps, not of %q", steps[current].Name, input.Step)
			case position == current && !available[input.Output]:
				return fmt.Errorf("step %q poll can only use outputs declared by the step's initial request, not %q", steps[current].Name, input.Output)
			case position < current && !declared[input.Step][input.Output]:
				return fmt.Errorf("step %q uses output %q that step %q does not declare", steps[current].Name, input.Output, input.Step)
			}
		}
		return nil
	}
	for i, step := range steps {
		switch {
		case step.API != nil:
			if err := check(i, step.API.Inputs, false, nil); err != nil {
				return err
			}
			if step.API.Poll != nil {
				initial := map[string]bool{}
				for _, output := range step.API.Outputs {
					initial[output.Name] = true
				}
				if err := check(i, step.API.Poll.Inputs, true, initial); err != nil {
					return err
				}
			}
		case step.Terraform != nil:
			if err := check(i, step.Terraform.Inputs, false, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// stepOutputs returns every output a step publishes: its own and its poll's.
func stepOutputs(step kmetalv1alpha1.WorkflowStep) []kmetalv1alpha1.WorkflowOutput {
	if step.API == nil {
		return nil
	}
	outputs := slices.Clone(step.API.Outputs)
	if step.API.Poll != nil {
		outputs = append(outputs, step.API.Poll.Outputs...)
	}
	return outputs
}

func (r *Handler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func outputsSecretName(workflow *kmetalv1alpha1.Workflow) string {
	return workflowChildName(workflow, "outputs", "outputs")
}

func outputSecretKey(step, output string) string { return step + "." + output }

// captureOutputs extracts the declared outputs from response and records them:
// non-sensitive values in the step status, sensitive ones in the workflow's Secret.
// It is idempotent, so a retry after a partial failure rewrites the same values.
func (r *Handler) captureOutputs(ctx context.Context, workflow *kmetalv1alpha1.Workflow, state *kmetalv1alpha1.WorkflowStepStatus, outputs []kmetalv1alpha1.WorkflowOutput, response []byte) error {
	values, err := extractOutputs(response, outputs)
	if err != nil || len(values) == 0 {
		return err
	}
	if len(state.Outputs)+len(state.SensitiveOutputs)+len(values) > maxOutputsPerStep {
		return &outputFailure{"OutputTooLarge", fmt.Sprintf("Step publishes more than %d outputs", maxOutputsPerStep)}
	}
	secretData := map[string][]byte{}
	for _, output := range outputs {
		if output.Sensitive {
			secretData[outputSecretKey(state.Name, output.Name)] = values[output.Name]
			if !slices.Contains(state.SensitiveOutputs, output.Name) {
				state.SensitiveOutputs = append(state.SensitiveOutputs, output.Name)
			}
			continue
		}
		if state.Outputs == nil {
			state.Outputs = map[string]string{}
		}
		state.Outputs[output.Name] = string(values[output.Name])
	}
	if len(secretData) == 0 {
		return nil
	}
	return r.writeOutputsSecret(ctx, workflow, secretData)
}

func (r *Handler) writeOutputsSecret(ctx context.Context, workflow *kmetalv1alpha1.Workflow, data map[string][]byte) error {
	key := types.NamespacedName{Namespace: workflow.Namespace, Name: outputsSecretName(workflow)}
	secret := &corev1.Secret{}
	err := r.reader().Get(ctx, key, secret)
	switch {
	case apierrors.IsNotFound(err):
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{"kmetal.dinova.one/workflow": workflow.Name}},
			Type:       corev1.SecretTypeOpaque, Data: data,
		}
		if err := controllerutil.SetControllerReference(workflow, secret, r.Scheme); err != nil {
			return err
		}
		// AlreadyExists is returned too: the retry then merges into the existing Secret.
		if err := r.Create(ctx, secret); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		maps.Copy(secret.Data, data)
		if err := r.Update(ctx, secret); err != nil {
			return err
		}
	}
	workflow.Status.OutputsSecretName = key.Name
	return nil
}

// outputResolver returns a lookup for published output values. The Secret is read at
// most once per resolver.
func (r *Handler) outputResolver(ctx context.Context, workflow *kmetalv1alpha1.Workflow) func(step, output string) ([]byte, error) {
	var secret *corev1.Secret
	return func(step, output string) ([]byte, error) {
		state := findStepStatus(&workflow.Status, step)
		if state == nil {
			return nil, fmt.Errorf("step %q has no recorded outputs", step)
		}
		if value, ok := state.Outputs[output]; ok {
			return []byte(value), nil
		}
		if !slices.Contains(state.SensitiveOutputs, output) {
			return nil, fmt.Errorf("output %q of step %q has not been published", output, step)
		}
		if secret == nil {
			secret = &corev1.Secret{}
			if err := r.reader().Get(ctx, types.NamespacedName{Namespace: workflow.Namespace, Name: outputsSecretName(workflow)}, secret); err != nil {
				return nil, fmt.Errorf("read outputs Secret: %w", err)
			}
		}
		value, ok := secret.Data[outputSecretKey(step, output)]
		if !ok {
			return nil, fmt.Errorf("sensitive output %q of step %q is missing from the outputs Secret", output, step)
		}
		return value, nil
	}
}
