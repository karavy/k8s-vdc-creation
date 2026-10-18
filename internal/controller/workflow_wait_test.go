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

package controller

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kmetalv1alpha1 "github.com/karavy/k8s-vdc-creation/api/v1alpha1"
)

var _ = Describe("Waiting for a job to succeed", func() {
	// jobServer answers POST /deliver with a job id, and GET /jobs/7 with the next status of
	// the list (the last one repeats).
	jobServer := func(polls *atomic.Int32, statuses ...string) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodPost && req.URL.Path == "/deliver":
				_, _ = w.Write([]byte(`{"job_id":7,"status":"queued"}`))
			case req.Method == http.MethodGet && req.URL.Path == "/jobs/7":
				n := min(int(polls.Add(1))-1, len(statuses)-1)
				_, _ = w.Write([]byte(`{"id":7,"status":"` + statuses[n] + `"}`))
			default:
				http.NotFound(w, req)
			}
		}))
		DeferCleanup(server.Close)
		return server
	}
	run := func(name string, server *httptest.Server, maxAttempts int32) *kmetalv1alpha1.Workflow {
		GinkgoHelper()
		createTemplates(map[string]kmetalv1alpha1.ApiCallTemplateSpec{
			name + "-deliver": {Method: http.MethodPost, Endpoint: server.URL + "/deliver"},
			name + "-job":     {Method: http.MethodGet, Endpoint: server.URL + "/jobs/{job_id}"},
		})
		byID := []kmetalv1alpha1.WorkflowInput{{Step: "deliver", Output: "job_id", ToPath: "job_id"}}
		return runWorkflow(&kmetalv1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: kmetalv1alpha1.WorkflowTemplateSpec{Steps: []kmetalv1alpha1.WorkflowStep{
				{Name: "deliver", API: apiStepOf(name+"-deliver", func(step *kmetalv1alpha1.WorkflowAPIStep) {
					step.Outputs = []kmetalv1alpha1.WorkflowOutput{{Name: "job_id", From: "/job_id"}}
				})},
				{Name: "wait", API: apiStepOf(name+"-job", func(step *kmetalv1alpha1.WorkflowAPIStep) {
					step.Inputs = byID
					step.Poll = &kmetalv1alpha1.WorkflowPoll{
						TemplateRef: kmetalv1alpha1.WorkflowResourceReference{Name: name + "-job"}, Inputs: byID,
						IntervalSeconds: 1, MaxAttempts: maxAttempts, ConditionJSONPointer: "/status", EqualsJSON: `"success"`,
					}
				})},
			}},
		}, server.URL, startExecutor())
	}

	It("keeps asking until the status is success, and then lets the workflow go on", func() {
		var polls atomic.Int32
		workflow := run("job-succeeds", jobServer(&polls, "running", "running", "success"), 10)
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(polls.Load()).To(Equal(int32(3)), "the first request of the step and two polls: running, running, success")
	})

	It("fails with PollTimeout when the job never succeeds in the attempts it is given", func() {
		var polls atomic.Int32
		workflow := run("job-never", jobServer(&polls, "running"), 2)
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("PollTimeout"))
		Expect(polls.Load()).To(Equal(int32(3)), "the first request, then the two polls the step is allowed")
	})
})
