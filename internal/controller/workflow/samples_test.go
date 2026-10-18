package workflow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	kmetalv1alpha1 "github.com/karavy/k8s-vdc-creation/api/v1alpha1"
)

// The p2p sample is what people copy: its references must hold, and a Workflow that gives
// the values it shows must run it.
func TestP2PSampleIsConsistent(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "config", "samples", "p2p-workflow")
	read := func(name string, into any) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.UnmarshalStrict(raw, into); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	template := &kmetalv1alpha1.WorkflowTemplate{}
	workflow := &kmetalv1alpha1.Workflow{}
	read("workflowtemplate.yaml", template)
	read("workflow.yaml", workflow)

	if err := validateTemplate(template); err != nil {
		t.Fatalf("template references: %v", err)
	}
	if len(workflow.Spec.Templates) == 0 {
		t.Fatal("the sample workflow runs nothing")
	}
	stage := workflow.Spec.Templates[0]
	if stage.TemplateRef.Name != template.Name {
		t.Fatalf("the workflow runs %q, the sample template is %q", stage.TemplateRef.Name, template.Name)
	}
	if _, err := effectiveParameters(template, stage); err != nil {
		t.Fatalf("parameters of the sample workflow: %v", err)
	}
}

// The deliver step builds the body the portal expects, and puts env_id in the path.
func TestP2PDeliverRequest(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "config", "samples", "p2p-workflow")
	raw, err := os.ReadFile(filepath.Join(dir, "apicalltemplates.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var call *kmetalv1alpha1.ApiCallTemplate
	for document := range strings.SplitSeq(string(raw), "\n---\n") {
		candidate := &kmetalv1alpha1.ApiCallTemplate{}
		if err := yaml.UnmarshalStrict([]byte(document), candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.Name == "p2p-fortigate-deliver" {
			call = candidate
		}
	}
	if call == nil || call.Spec.Method != "POST" {
		t.Fatalf("p2p-fortigate-deliver must be a POST: %+v", call)
	}
	templateRaw, err := os.ReadFile(filepath.Join(dir, "workflowtemplate.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	template := &kmetalv1alpha1.WorkflowTemplate{}
	if err := yaml.UnmarshalStrict(templateRaw, template); err != nil {
		t.Fatal(err)
	}
	var inputs []kmetalv1alpha1.WorkflowInput
	for _, step := range template.Spec.Steps {
		if step.Name == "deliver" {
			inputs = step.API.Inputs
			if !step.API.Debug && !call.Spec.Debug {
				t.Error("the deliver step must be in debug mode")
			}
		}
	}
	values := map[string]string{
		"parameter.vmName": "fw-acme-01", "parameter.vcloudOrgName": "Dinova-Cloud", "parameter.clientName": "ACME",
		"parameter.rifContratto": "CNT.26.0001.01",
		"get-env.env_id":         "6", "get-orgs.org_id": `"urn:vcloud:org:1"`,
		"create-segment.nsx_segment_id": `"P2P-ACME-NET"`, "create-segment.nsx_segment_name": `"P2P-ACME-NET"`,
		"create-segment.nsx_segment_network": `"178.239.21.34/31"`,
		"get-flexvm-configs.config_id":       `3619`,
		"free-subnet.tenant_ip":              `"178.239.21.35"`, "free-subnet.nsx_ip": `"178.239.21.34"`,
	}
	resolve := func(input kmetalv1alpha1.WorkflowInput) ([]byte, error) {
		if input.Parameter != "" {
			return json.Marshal(values["parameter."+input.Parameter])
		}
		value, ok := values[input.Step+"."+input.Output]
		if !ok {
			t.Errorf("input %s.%s has no value in the test", input.Step, input.Output)
		}
		return []byte(value), nil
	}
	endpoint, err := applyPathInputs(call.Spec.Endpoint, inputs, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://dev-portal.admin.dinova.cloud:8453/api/v1/fortigate-vm/vcloud/6/deliver"; endpoint != want {
		t.Errorf("endpoint = %s, want %s", endpoint, want)
	}
	body, err := applyInputs([]byte(call.Spec.Body), inputs, resolve)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"vm_name":"fw-acme-01","template_id":"CHANGEME","org_name":"Dinova-Cloud","org_id":"urn:vcloud:org:1",
		"vdc_name":"CHANGEME","create_vdc":true,"nsx_segment_id":"P2P-ACME-NET","nsx_segment_name":"P2P-ACME-NET",
		"nsx_segment_network":"178.239.21.34/31","assigned_ip":"178.239.21.35","netmask":"255.255.255.254",
		"gateway":"178.239.21.34","flexvm_config_id":"3619","cliente":"ACME","rif_contratto":"CNT.26.0001.01",
		"vdc_cpu_mhz":4000,"vdc_ram_mb":8192,"vdc_storage_mb":61440}`
	var gotJSON, wantJSON map[string]any
	if err := json.Unmarshal(body, &gotJSON); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantJSON); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		t.Errorf("body = %s\nwant   %s", body, want)
	}
}

// While the p2p sample is being built, every step that changes something runs in debug mode:
// a POST that is not in debug would really create segments and virtual machines.
func TestP2PSampleWritesAreInDebug(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "config", "samples", "p2p-workflow")
	calls := map[string]*kmetalv1alpha1.ApiCallTemplate{}
	raw, err := os.ReadFile(filepath.Join(dir, "apicalltemplates.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for document := range strings.SplitSeq(string(raw), "\n---\n") {
		call := &kmetalv1alpha1.ApiCallTemplate{}
		if err := yaml.UnmarshalStrict([]byte(document), call); err != nil {
			t.Fatal(err)
		}
		calls[call.Name] = call
	}
	templateRaw, err := os.ReadFile(filepath.Join(dir, "workflowtemplate.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	template := &kmetalv1alpha1.WorkflowTemplate{}
	if err := yaml.UnmarshalStrict(templateRaw, template); err != nil {
		t.Fatal(err)
	}
	writes := 0
	for _, step := range append(template.Spec.Steps, template.Spec.DeleteSteps...) {
		if step.API == nil {
			continue
		}
		call, found := calls[step.API.TemplateRef.Name]
		if !found {
			t.Errorf("step %q uses the ApiCallTemplate %q, which the sample does not define", step.Name, step.API.TemplateRef.Name)
			continue
		}
		if call.Spec.Method == "GET" {
			continue
		}
		writes++
		if !step.API.Debug && !call.Spec.Debug {
			t.Errorf("step %q sends a %s and is not in debug mode", step.Name, call.Spec.Method)
		}
	}
	if writes == 0 {
		t.Error("the sample has no POST step: this test checks nothing")
	}
}
