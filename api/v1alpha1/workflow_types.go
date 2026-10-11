/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkflowSpec describes an immutable ordered sequence of API and Terraform steps.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Workflow spec is immutable; create a new workflow to change its steps"
type WorkflowSpec struct {
	// Steps run strictly in order. A step starts only after its predecessor succeeds.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	// +listType=atomic
	Steps []WorkflowStep `json:"steps"`
	// WorkspaceStorage requests a per-workflow PVC for the Terraform working files (rendered
	// templates, providers). Terraform state does not live here: see WorkflowTerraformState.
	// +kubebuilder:default="2Gi"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*(Mi|Gi|Ti)$`
	// +optional
	WorkspaceStorage string `json:"workspaceStorage,omitempty"`
}

// WorkflowStep is exactly one API or Terraform operation.
// +kubebuilder:validation:XValidation:rule="has(self.api) != has(self.terraform)",message="exactly one of api or terraform must be set"
type WorkflowStep struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Name string `json:"name"`
	// +optional
	API *WorkflowAPIStep `json:"api,omitempty"`
	// +optional
	Terraform *WorkflowTerraformStep `json:"terraform,omitempty"`
}

// WorkflowResourceReference names an object in the Workflow's namespace.
type WorkflowResourceReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// WorkflowAPIStep runs an ApiCall created from an existing ApiCallTemplate and
// may poll until ready.
type WorkflowAPIStep struct {
	// TemplateRef names the ApiCallTemplate that defines this step's request.
	// The Workflow runs a child ApiCall created from it, owned by the Workflow.
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Inputs set values published by earlier steps (see Outputs) into the copied body.
	// The body must be a JSON object.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Inputs []WorkflowInput `json:"inputs,omitempty"`
	// Outputs publish values from this request's response, by name, for later steps.
	// With a poll, they are read from the initial response; add poll.outputs to
	// publish values from the final poll response.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Outputs []WorkflowOutput `json:"outputs,omitempty"`
	// Poll repeats a read request until the JSON response satisfies the condition.
	// +optional
	Poll *WorkflowPoll `json:"poll,omitempty"`
}

// WorkflowPoll defines a bounded polling loop after the initial API call.
type WorkflowPoll struct {
	// TemplateRef names the ApiCallTemplate used for each polling request.
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Inputs set published values into each poll request body. They may use outputs
	// of earlier steps and of this step's initial request.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Inputs []WorkflowInput `json:"inputs,omitempty"`
	// Outputs publish values from the final poll response, the one that satisfies
	// the condition. Names must differ from the step's own outputs.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Outputs []WorkflowOutput `json:"outputs,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=5
	// +optional
	IntervalSeconds int32 `json:"intervalSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`
	// JSON Pointer into the polling response.
	// +kubebuilder:validation:Pattern=`^/.*$`
	ConditionJSONPointer string `json:"conditionJSONPointer"`
	// Expected value as JSON (for example true, "ready", or 3).
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	EqualsJSON string `json:"equalsJSON,omitempty"`
	// RetryOnStatusCodes lists HTTP statuses of a poll response (for example 404 or 503)
	// that mean "not ready yet" instead of a failure. Such a poll consumes one of
	// maxAttempts. Any other non-2xx status fails the workflow. It never applies to
	// the step's initial request.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Minimum=100
	// +kubebuilder:validation:items:Maximum=599
	// +listType=set
	// +optional
	RetryOnStatusCodes []int32 `json:"retryOnStatusCodes,omitempty"`
	// RetryOnTransportErrors treats a poll that timed out or failed at the network
	// level as "not ready yet". Invalid requests and oversized responses still fail.
	// +optional
	RetryOnTransportErrors bool `json:"retryOnTransportErrors,omitempty"`
}

// WorkflowTerraformState identifies the Terraform state of a step.
type WorkflowTerraformState struct {
	// Key names the resource this step manages, for example "vdc/acme/network". Workflows
	// that use the same key in the same namespace share one state, even at different times,
	// so a later run with changed inputs modifies the existing resources. Concurrent runs on
	// the same key do not overlap: the second one fails on the state lock.
	//
	// Segments are separated by "/" and use letters, digits, ".", "_" and "-". A segment can
	// contain placeholders {{steps.<step>.outputs.<name>}} that are replaced by the output of
	// an earlier step, which must be a string, number or boolean made of the same characters.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._{}/ -]+$`
	Key string `json:"key"`
}

// WorkflowOutput publishes one named value from a JSON response so that later steps
// can use it, instead of depending on the shape of the previous response.
type WorkflowOutput struct {
	// Name identifies the output within its step.
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// From is a JSON Pointer into the response, for example /id.
	// +kubebuilder:validation:Pattern=`^/.*$`
	// +kubebuilder:validation:MaxLength=512
	From string `json:"from"`
	// DefaultJSON is the JSON value used when From does not exist in the response
	// (for example "eu-1", 3 or null). Without it, a missing value fails the workflow.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	DefaultJSON string `json:"defaultJSON,omitempty"`
	// Sensitive stores the value in a per-workflow Secret instead of the Workflow status.
	// +optional
	Sensitive bool `json:"sensitive,omitempty"`
}

// WorkflowInput sets an output published by an earlier step into a JSON object.
type WorkflowInput struct {
	// Step is the name of the step that publishes the output. It must come before the
	// current step (a poll may also use outputs of its own step's initial request).
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Step string `json:"step"`
	// Output is the name of the output declared by that step.
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Output string `json:"output"`
	// To is a non-root JSON Pointer into the target object, for example /vdcId.
	// +kubebuilder:validation:Pattern=`^/.+$`
	// +kubebuilder:validation:MaxLength=512
	To string `json:"to"`
}

// WorkflowTerraformStep provisions infrastructure with terraform-engine. Its state is kept
// in S3-compatible storage under State.Key, independently of the workflow: any later
// workflow that uses the same key works on the same state and so on the same resources.
// Every Terraform step works in its own folder of the workflow's workspace, named after
// the step, so a workflow can have any number of them.
type WorkflowTerraformStep struct {
	// TemplateRef names the TerraformTemplate whose files this step runs. Its files are
	// copied when the step starts; later edits of the template do not affect it.
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Inputs set values published by earlier steps into terraform.auto.tfvars.json;
	// To is a JSON pointer such as /cluster_id naming the Terraform variable.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Inputs []WorkflowInput `json:"inputs,omitempty"`
	// State says where the Terraform state of this step lives.
	State WorkflowTerraformState `json:"state"`
	// Outputs publish values from the Terraform outputs (the "output" blocks of the
	// template) for later steps. They are read from the state once the Job succeeds,
	// as one JSON object {"<output name>": value}, so From is a JSON Pointer such as
	// /vpc_id or /ips/0. Terraform outputs marked sensitive are always kept in the Secret.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Outputs []WorkflowOutput `json:"outputs,omitempty"`
	// Maximum wall time for the Terraform Job.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7200
	// +kubebuilder:default=1800
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
	// Secret names in the workflow namespace whose keys are exposed as environment variables to the Terraform Job.
	// The storage credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) must come from one of them.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	EnvFromSecrets []string `json:"envFromSecrets,omitempty"`
}

// WorkflowStepStatus records execution progress. Besides the declared non-sensitive
// outputs, no API response data is copied into the Workflow status.
type WorkflowStepStatus struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=Pending;Running;Polling;Succeeded;Failed;Unknown
	Phase string `json:"phase"`
	// +optional
	APICallName string `json:"apiCallName,omitempty"`
	// +optional
	JobName string `json:"jobName,omitempty"`
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// Outputs holds the step's non-sensitive output values, JSON-encoded, by name.
	// +kubebuilder:validation:MaxProperties=32
	// +optional
	Outputs map[string]string `json:"outputs,omitempty"`
	// StateKey is the object key of the step's Terraform state in the state storage.
	// +optional
	StateKey string `json:"stateKey,omitempty"`
	// SensitiveOutputs lists the output names whose values live in the workflow's
	// outputs Secret, under the key "<step>.<output>". Values are never shown here.
	// +kubebuilder:validation:MaxItems=32
	// +listType=set
	// +optional
	SensitiveOutputs []string `json:"sensitiveOutputs,omitempty"`
}

// WorkflowStatus defines the observed state of Workflow.
type WorkflowStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Unknown
	// +optional
	Phase string `json:"phase,omitempty"`
	// Index of the next step; zero-based.
	// +optional
	CurrentStep int32 `json:"currentStep,omitempty"`
	// Name of the Secret holding the workflow's sensitive outputs, if any.
	// +optional
	OutputsSecretName string `json:"outputsSecretName,omitempty"`
	// Per-workflow PVC for Terraform files and state.
	// +optional
	WorkspacePVCName string `json:"workspacePVCName,omitempty"`
	// +listType=map
	// +listMapKey=name
	// +optional
	Steps []WorkflowStepStatus `json:"steps,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Step",type=integer,JSONPath=".status.currentStep"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// Workflow is the Schema for the workflows API
type Workflow struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Workflow
	// +required
	Spec WorkflowSpec `json:"spec"`

	// status defines the observed state of Workflow
	// +optional
	Status WorkflowStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkflowList contains a list of Workflow
type WorkflowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Workflow `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Workflow{}, &WorkflowList{})
}
