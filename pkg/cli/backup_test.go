package cli

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestManualBackupJobFromCronJobCopiesTemplate(t *testing.T) {
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-backup",
			Namespace: "default",
			Labels:    map[string]string{"app": "rdf-delta"},
		},
		Spec: batchv1.CronJobSpec{
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"fuseki.apache.org/component": "backup-job"},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers: []corev1.Container{{
								Name:  "backup",
								Image: "minio/mc:latest",
							}},
						},
					},
				},
			},
		},
	}

	job := manualBackupJobFromCronJob(cronJob)
	if job.GenerateName != "example-backup-manual-" {
		t.Fatalf("unexpected generateName: %q", job.GenerateName)
	}
	if job.Namespace != "default" {
		t.Fatalf("unexpected namespace: %q", job.Namespace)
	}
	if got := job.Labels["fuseki.apache.org/backup-trigger"]; got != "manual" {
		t.Fatalf("expected manual backup label, got %q", got)
	}
	if got := job.Spec.Template.Spec.Containers[0].Image; got != "minio/mc:latest" {
		t.Fatalf("unexpected container image: %q", got)
	}
	if got := job.Annotations["cronjob.kubernetes.io/instantiate"]; got != "manual" {
		t.Fatalf("expected instantiate annotation, got %q", got)
	}
}
