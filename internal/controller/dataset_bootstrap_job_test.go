package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

func TestReconcileDatasetBootstrapJobPreservesExistingTemplate(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "standalone",
			Namespace: "default",
			UID:       types.UID("server-uid"),
		},
	}
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default"},
		Spec:       fusekiv1alpha1.DatasetSpec{Name: "primary"},
	}
	target := datasetBootstrapTarget{
		Kind:     "server",
		Name:     server.Name,
		Image:    "ghcr.io/example/fuseki:6.0.0",
		WriteURL: "http://standalone:3030",
	}
	jobName := datasetBootstrapJobName(target, dataset.Name)
	originalTemplateLabels := map[string]string{
		"fuseki.apache.org/component":        "dataset-bootstrap",
		"fuseki.apache.org/dataset":          dataset.Name,
		"fuseki.apache.org/dataset-name":     dataset.Spec.Name,
		"fuseki.apache.org/server":           server.Name,
		"job-name":                           jobName,
		"batch.kubernetes.io/controller-uid": "controller-uid",
		"batch.kubernetes.io/job-name":       jobName,
		"controller-uid":                     "controller-uid",
	}
	existingJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              jobName,
			Namespace:         server.Namespace,
			CreationTimestamp: metav1.NewTime(time.Unix(1, 0)),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptrTo(int32(3)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: originalTemplateLabels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers:    []corev1.Container{{Name: "bootstrap", Image: target.Image}},
					Volumes: []corev1.Volume{{
						Name: datasetConfigVolumeName,
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: dataset.ConfigMapName()},
						}},
					}},
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(server, existingJob).
		Build()

	if err := reconcileDatasetBootstrapJob(context.Background(), k8sClient, scheme, server, dataset, target, fusekiServerLabels(server), nil, nil); err != nil {
		t.Fatalf("reconcile dataset bootstrap job: %v", err)
	}

	updatedJob := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: jobName, Namespace: server.Namespace}, updatedJob); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	if got := updatedJob.Spec.Template.Labels["batch.kubernetes.io/controller-uid"]; got != "controller-uid" {
		t.Fatalf("expected controller-added template labels to be preserved, got %q", got)
	}
	if got := updatedJob.Spec.Template.Labels["job-name"]; got != jobName {
		t.Fatalf("unexpected job-name label: %q", got)
	}
	if got := updatedJob.Labels["fuseki.apache.org/dataset-name"]; got != dataset.Spec.Name {
		t.Fatalf("unexpected top-level dataset label: %q", got)
	}
	if len(updatedJob.Spec.Template.Labels) != len(originalTemplateLabels) {
		t.Fatalf("expected template labels to remain unchanged, got %#v", updatedJob.Spec.Template.Labels)
	}
}
