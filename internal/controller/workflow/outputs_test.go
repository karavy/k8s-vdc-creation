package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	kmetalv1alpha1 "github.com/karavy/k8s-vdc-creation/api/v1alpha1"
)

type (
	output = kmetalv1alpha1.WorkflowOutput
	input  = kmetalv1alpha1.WorkflowInput
)

func TestExtractOutputs(t *testing.T) {
	response := []byte(`{"id":"vdc-1","nested":{"count":3,"ready":true},"list":["a","b"]}`)
	values, err := extractOutputs(response, []output{
		{Name: "id", From: "/id"},
		{Name: "count", From: "/nested/count"},
		{Name: "second", From: "/list/1"},
		{Name: "region", From: "/region", DefaultJSON: `"eu-1"`},
		{Name: "nothing", From: "/missing", DefaultJSON: `null`},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"id": `"vdc-1"`, "count": `3`, "second": `"b"`, "region": `"eu-1"`, "nothing": `null`}
	for name, expected := range want {
		if string(values[name]) != expected {
			t.Errorf("output %s = %s, want %s", name, values[name], expected)
		}
	}
}

func TestExtractOutputsFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		response string
		outputs  []output
		reason   string
	}{
		"missing without default": {`{}`, []output{{Name: "id", From: "/id"}}, "OutputMissing"},
		"response not JSON":       {`not json`, []output{{Name: "id", From: "/id"}}, "OutputExtractionFailed"},
		"invalid default":         {`{}`, []output{{Name: "id", From: "/id", DefaultJSON: `{bad`}}, "OutputExtractionFailed"},
		"value too large":         {fmt.Sprintf(`{"big":%q}`, strings.Repeat("x", maxOutputValueBytes)), []output{{Name: "big", From: "/big"}}, "OutputTooLarge"},
	} {
		_, err := extractOutputs([]byte(tc.response), tc.outputs)
		var failure *outputFailure
		if !errors.As(err, &failure) || failure.reason != tc.reason {
			t.Errorf("%s: error = %v, want reason %s", name, err, tc.reason)
		}
	}
	if values, err := extractOutputs([]byte(`not json`), nil); err != nil || values != nil {
		t.Errorf("no declared outputs must not parse the response: %v %v", values, err)
	}
}

func resolverFrom(values map[string]string) func(step, output string) ([]byte, error) {
	return func(step, output string) ([]byte, error) {
		if value, ok := values[step+"."+output]; ok {
			return []byte(value), nil
		}
		return nil, fmt.Errorf("no output %s.%s", step, output)
	}
}

func TestApplyInputsSetsValuesIntoBody(t *testing.T) {
	body, err := applyInputs([]byte(`{"request":{},"static":"kept"}`), []input{
		{Step: "create-vdc", Output: "id", To: "/request/vdcId"},
		{Step: "create-vdc", Output: "count", To: "/count"},
		{Step: "other", Output: "ready", To: "/ready"},
	}, resolverFrom(map[string]string{"create-vdc.id": `"vdc-1"`, "create-vdc.count": `3`, "other.ready": `true`}))
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]any
	if err := json.Unmarshal(body, &actual); err != nil {
		t.Fatal(err)
	}
	request := actual["request"].(map[string]any)
	if request["vdcId"] != "vdc-1" || actual["count"] != float64(3) || actual["ready"] != true || actual["static"] != "kept" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestApplyInputsWithoutInputsKeepsBodyUntouched(t *testing.T) {
	body, err := applyInputs([]byte(`not even json`), nil, nil)
	if err != nil || string(body) != `not even json` {
		t.Fatalf("body = %q, error = %v", body, err)
	}
}

func TestApplyInputsRejectsInvalidUse(t *testing.T) {
	resolve := resolverFrom(map[string]string{"a.x": `"v"`, "a.y": `"w"`, "a.bad": `{`})
	for name, tc := range map[string]struct {
		body   string
		inputs []input
	}{
		"unknown output":           {`{}`, []input{{Step: "a", Output: "zzz", To: "/x"}}},
		"duplicate target":         {`{}`, []input{{Step: "a", Output: "x", To: "/t"}, {Step: "a", Output: "y", To: "/t"}}},
		"body is not an object":    {`[]`, []input{{Step: "a", Output: "x", To: "/t"}}},
		"body is not JSON":         {`text`, []input{{Step: "a", Output: "x", To: "/t"}}},
		"parent is not an object":  {`{"n":"str"}`, []input{{Step: "a", Output: "x", To: "/n/t"}}},
		"stored value is not JSON": {`{}`, []input{{Step: "a", Output: "bad", To: "/t"}}},
	} {
		if _, err := applyInputs([]byte(tc.body), tc.inputs, resolve); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func apiStep(name string, outputs ...output) kmetalv1alpha1.WorkflowStep {
	return kmetalv1alpha1.WorkflowStep{Name: name, API: &kmetalv1alpha1.WorkflowAPIStep{Outputs: outputs}}
}

func TestValidateReferences(t *testing.T) {
	first := apiStep("first", output{Name: "id", From: "/id"})
	withInput := func(name string, in ...input) kmetalv1alpha1.WorkflowStep {
		step := apiStep(name)
		step.API.Inputs = in
		return step
	}
	polling := apiStep("polling", output{Name: "op", From: "/op"})
	polling.API.Poll = &kmetalv1alpha1.WorkflowPoll{
		Inputs:  []input{{Step: "polling", Output: "op", To: "/op"}},
		Outputs: []output{{Name: "result", From: "/result"}},
	}
	terraform := kmetalv1alpha1.WorkflowStep{Name: "tf", Terraform: &kmetalv1alpha1.WorkflowTerraformStep{
		Inputs: []input{{Step: "polling", Output: "result", To: "/result"}},
	}}

	valid := [][]kmetalv1alpha1.WorkflowStep{
		{first, withInput("second", input{Step: "first", Output: "id", To: "/id"})},
		{first, withInput("second"), withInput("third", input{Step: "first", Output: "id", To: "/id"})},
		{polling, terraform},
	}
	for i, steps := range valid {
		if err := validateReferences(steps); err != nil {
			t.Errorf("valid case %d rejected: %v", i, err)
		}
	}

	badPollSelf := apiStep("polling", output{Name: "op", From: "/op"})
	badPollSelf.API.Poll = &kmetalv1alpha1.WorkflowPoll{
		Inputs:  []input{{Step: "polling", Output: "result", To: "/x"}},
		Outputs: []output{{Name: "result", From: "/result"}},
	}
	badTerraform := kmetalv1alpha1.WorkflowStep{Name: "tf", Terraform: &kmetalv1alpha1.WorkflowTerraformStep{
		Inputs: []input{{Step: "first", Output: "nope", To: "/x"}},
	}}
	invalid := map[string][]kmetalv1alpha1.WorkflowStep{
		"unknown step":               {first, withInput("second", input{Step: "ghost", Output: "id", To: "/id"})},
		"undeclared output":          {first, withInput("second", input{Step: "first", Output: "nope", To: "/id"})},
		"later step":                 {withInput("second", input{Step: "first", Output: "id", To: "/id"}), first},
		"own output without a poll":  {withInput("self", input{Step: "self", Output: "id", To: "/id"})},
		"poll uses its own poll out": {badPollSelf},
		"duplicate step name":        {first, first},
		"duplicate output name":      {apiStep("dup", output{Name: "a", From: "/a"}, output{Name: "a", From: "/b"})},
		"terraform undeclared":       {first, badTerraform},
	}
	for name, steps := range invalid {
		if err := validateReferences(steps); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func terraformStep(name, key string, inputs []input, outputs ...output) kmetalv1alpha1.WorkflowStep {
	return kmetalv1alpha1.WorkflowStep{Name: name, Terraform: &kmetalv1alpha1.WorkflowTerraformStep{
		State: kmetalv1alpha1.WorkflowTerraformState{Key: key}, Inputs: inputs, Outputs: outputs,
	}}
}

func TestValidateReferencesCoversTerraformStateKeysAndOutputs(t *testing.T) {
	create := apiStep("create-vdc", output{Name: "name", From: "/name"})
	net := terraformStep("net", "vdc/{{steps.create-vdc.outputs.name}}/net", nil, output{Name: "network_id", From: "/network_id"})
	for i, steps := range [][]kmetalv1alpha1.WorkflowStep{
		{create, net},
		// a Terraform step may use the outputs of an earlier Terraform step, as variables and in its key
		{create, net, terraformStep("cluster", "vdc/{{steps.create-vdc.outputs.name}}/{{steps.net.outputs.network_id}}",
			[]input{{Step: "net", Output: "network_id", To: "/network_id"}})},
		{terraformStep("solo", "static/key", nil)},
	} {
		if err := validateReferences(steps); err != nil {
			t.Errorf("valid case %d rejected: %v", i, err)
		}
	}
	for name, steps := range map[string][]kmetalv1alpha1.WorkflowStep{
		"key uses an unknown step":                  {create, terraformStep("net", "vdc/{{steps.ghost.outputs.name}}", nil)},
		"key uses an undeclared value":              {create, terraformStep("net", "vdc/{{steps.create-vdc.outputs.nope}}", nil)},
		"key uses a later step":                     {terraformStep("net", "vdc/{{steps.create-vdc.outputs.name}}", nil), create},
		"key uses its own output":                   {terraformStep("net", "vdc/{{steps.net.outputs.id}}", nil, output{Name: "id", From: "/id"})},
		"input from an undeclared terraform output": {net, terraformStep("cluster", "k", []input{{Step: "net", Output: "nope", To: "/x"}})},
		"terraform declares a duplicate output":     {terraformStep("net", "k", nil, output{Name: "a", From: "/a"}, output{Name: "a", From: "/b"})},
	} {
		if err := validateReferences(steps); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestSensitiveDeclaredFollowsTheTerraformOutputThePointerReadsFrom(t *testing.T) {
	declared := []output{
		{Name: "adminPassword", From: "/admin_password"}, // the name differs from the Terraform output
		{Name: "dbPassword", From: "/db/password"},       // a field of a sensitive object output
		{Name: "networkId", From: "/network_id"},
		{Name: "firstIP", From: "/ips/0"},
	}
	got := sensitiveDeclared(declared, []string{"admin_password", "db"})
	if len(got) != 2 || got[0] != "adminPassword" || got[1] != "dbPassword" {
		t.Fatalf("sensitive declared outputs = %v, want [adminPassword dbPassword]", got)
	}
	if got := sensitiveDeclared(declared, nil); len(got) != 0 {
		t.Fatalf("nothing is sensitive: %v", got)
	}
}
