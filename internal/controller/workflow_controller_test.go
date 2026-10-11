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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kmetalv1alpha1 "github.com/karavy/k8s-vdc-creation/api/v1alpha1"
	"github.com/karavy/k8s-vdc-creation/internal/controller/apicall"
)

var _ = Describe("Workflow Controller", func() {
	It("sequences API calls, polls asynchronously, and maps the previous response into the next body", func() {
		var mu sync.Mutex
		var observed []string
		polls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch req.URL.Path {
			case "/start":
				observed = append(observed, "start")
				_, _ = w.Write([]byte(`{"operationId":"op-42"}`))
			case "/poll":
				var body map[string]string
				Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
				Expect(body["operation"]).To(Equal("op-42"))
				polls++
				observed = append(observed, "poll")
				_ = json.NewEncoder(w).Encode(map[string]any{"ready": polls >= 2, "resultId": "resource-99"})
			case "/finish":
				var body map[string]map[string]any
				Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
				Expect(body["resource"]["id"]).To(Equal("resource-99"))
				Expect(body["resource"]["ready"]).To(BeTrue())
				Expect(body["resource"]["origin"]).To(Equal("op-42"))
				observed = append(observed, "finish")
				w.WriteHeader(http.StatusCreated)
			default:
				http.NotFound(w, req)
			}
		}))
		DeferCleanup(server.Close)

		apiClient, err := apiengine.NewClient(apiengine.Config{})
		Expect(err).NotTo(HaveOccurred())
		executor, err := apicall.NewAPIExecutor(apiClient, 2, 8)
		Expect(err).NotTo(HaveOccurred())
		workerCtx, stopWorkers := context.WithCancel(ctx)
		workersDone := make(chan struct{})
		go func() {
			defer close(workersDone)
			_ = executor.Start(workerCtx)
		}()
		DeferCleanup(func() {
			stopWorkers()
			Eventually(workersDone, 5*time.Second).Should(BeClosed())
		})
		Eventually(executor.Ready).Should(BeTrue())

		templates := map[string]kmetalv1alpha1.ApiCallTemplateSpec{
			"seq-start":  {Method: http.MethodPost, Endpoint: server.URL + "/start"},
			"seq-poll":   {Method: http.MethodPost, Endpoint: server.URL + "/poll", Body: `{"operation":""}`},
			"seq-finish": {Method: http.MethodPost, Endpoint: server.URL + "/finish", Body: `{"resource":{}}`},
		}
		for templateName, spec := range templates {
			template := &kmetalv1alpha1.ApiCallTemplate{ObjectMeta: metav1.ObjectMeta{Name: templateName, Namespace: "default"}, Spec: spec}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		}
		ref := func(name string) kmetalv1alpha1.WorkflowResourceReference {
			return kmetalv1alpha1.WorkflowResourceReference{Name: name}
		}

		name := types.NamespacedName{Name: "api-sequence", Namespace: "default"}
		workflow := &kmetalv1alpha1.Workflow{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: kmetalv1alpha1.WorkflowSpec{Steps: []kmetalv1alpha1.WorkflowStep{
				{Name: "start-job", API: &kmetalv1alpha1.WorkflowAPIStep{
					TemplateRef: ref("seq-start"),
					Outputs:     []kmetalv1alpha1.WorkflowOutput{{Name: "origin", From: "/operationId"}},
				}},
				{
					Name: "wait-job",
					API: &kmetalv1alpha1.WorkflowAPIStep{
						TemplateRef: ref("seq-start"),
						Outputs:     []kmetalv1alpha1.WorkflowOutput{{Name: "operation", From: "/operationId"}},
						Poll: &kmetalv1alpha1.WorkflowPoll{
							TemplateRef:     ref("seq-poll"),
							Inputs:          []kmetalv1alpha1.WorkflowInput{{Step: "wait-job", Output: "operation", To: "/operation"}},
							Outputs:         []kmetalv1alpha1.WorkflowOutput{{Name: "resultId", From: "/resultId"}, {Name: "ready", From: "/ready"}},
							IntervalSeconds: 1, MaxAttempts: 3, ConditionJSONPointer: "/ready", EqualsJSON: "true",
						},
					},
				},
				{
					Name: "finish-job",
					API: &kmetalv1alpha1.WorkflowAPIStep{
						TemplateRef: ref("seq-finish"),
						// "origin" comes from step 1, the others from step 2: not only the previous step is usable.
						Inputs: []kmetalv1alpha1.WorkflowInput{
							{Step: "wait-job", Output: "resultId", To: "/resource/id"},
							{Step: "wait-job", Output: "ready", To: "/resource/ready"},
							{Step: "start-job", Output: "origin", To: "/resource/origin"},
						},
					},
				},
			}},
		}
		Expect(k8sClient.Create(ctx, workflow)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, workflow)).To(Succeed()) })

		workflowReconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test"}
		apiReconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{server.URL}}
		request := reconcile.Request{NamespacedName: name}
		Eventually(func() string {
			_, reconcileErr := workflowReconciler.Reconcile(ctx, request)
			Expect(reconcileErr).NotTo(HaveOccurred())
			children := &kmetalv1alpha1.ApiCallList{}
			Expect(k8sClient.List(ctx, children, client.InNamespace(name.Namespace), client.MatchingLabels{"kmetal.dinova.one/workflow": name.Name})).To(Succeed())
			for i := range children.Items {
				child := &children.Items[i]
				_, childErr := apiReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: child.Namespace, Name: child.Name}})
				Expect(childErr).NotTo(HaveOccurred())
			}
			current := &kmetalv1alpha1.Workflow{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			return current.Status.Phase
		}, 30*time.Second, 20*time.Millisecond).Should(Equal("Succeeded"))

		mu.Lock()
		Expect(observed).To(Equal([]string{"start", "start", "poll", "poll", "finish"}))
		mu.Unlock()
		result := &kmetalv1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, name, result)).To(Succeed())
		Expect(result.Status.CurrentStep).To(Equal(int32(3)))
		Expect(result.Status.Steps).To(HaveLen(3))
		calls := &kmetalv1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, calls, client.InNamespace(name.Namespace))).To(Succeed())
		for i := range calls.Items {
			Expect(calls.Items[i].OwnerReferences).NotTo(BeEmpty(), "every ApiCall in the namespace must be a Workflow-owned copy")
		}
	})

	It("fails the workflow when a referenced ApiCall does not exist", func() {
		name := types.NamespacedName{Name: "missing-template", Namespace: "default"}
		workflow := &kmetalv1alpha1.Workflow{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: kmetalv1alpha1.WorkflowSpec{Steps: []kmetalv1alpha1.WorkflowStep{{
				Name: "call", API: &kmetalv1alpha1.WorkflowAPIStep{TemplateRef: kmetalv1alpha1.WorkflowResourceReference{Name: "does-not-exist"}},
			}}},
		}
		Expect(k8sClient.Create(ctx, workflow)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, workflow)).To(Succeed()) })
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test"}
		Eventually(func() string {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: name})
			Expect(err).NotTo(HaveOccurred())
			current := &kmetalv1alpha1.Workflow{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			return current.Status.Phase
		}).Should(Equal("Failed"))
		children := &kmetalv1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, children, client.InNamespace(name.Namespace), client.MatchingLabels{"kmetal.dinova.one/workflow": name.Name})).To(Succeed())
		Expect(children.Items).To(BeEmpty())
	})

	It("creates a PVC-backed Terraform job and keeps mapped variables in a Secret", func() {
		name := types.NamespacedName{Name: "terraform-workflow", Namespace: "default"}
		template := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "terraform-templates", Namespace: name.Namespace}, Data: map[string]string{"main.tf.tmpl": `output "cluster_id" { value = "<FGT_CLUSTER_ID>" }`}}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		workflow := &kmetalv1alpha1.Workflow{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: kmetalv1alpha1.WorkflowSpec{WorkspaceStorage: "1Gi", Steps: []kmetalv1alpha1.WorkflowStep{{
				Name: "apply", Terraform: &kmetalv1alpha1.WorkflowTerraformStep{TemplateConfigMap: template.Name, TimeoutSeconds: 60},
			}}},
		}
		Expect(k8sClient.Create(ctx, workflow)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, workflow)).To(Succeed()) })
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test"}
		request := reconcile.Request{NamespacedName: name}
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		current := &kmetalv1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		workspace := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: name.Namespace, Name: current.Status.WorkspacePVCName}, workspace)).To(Succeed())
		workspace.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, workspace)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		secrets := &corev1.SecretList{}
		Expect(k8sClient.List(ctx, secrets, client.InNamespace(name.Namespace), client.MatchingLabels{"kmetal.dinova.one/workflow": name.Name})).To(Succeed())
		Expect(secrets.Items).To(HaveLen(1))
		Expect(json.Valid(secrets.Items[0].Data["terraform.auto.tfvars.json"])).To(BeTrue())
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(name.Namespace))).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1))
		Expect(jobs.Items[0].Spec.Template.Spec.Containers[0].Image).To(Equal("runner:test"))
		Expect(jobs.Items[0].Spec.Template.Spec.Volumes).To(ContainElement(HaveField("Name", "workspace")))
	})
})
