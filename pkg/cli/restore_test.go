package cli

import (
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func TestBuildRestoreRequestUsesGenerateNameWhenNameIsEmpty(t *testing.T) {
	request := buildRestoreRequest(restoreRequestParams{
		Namespace:    "default",
		TargetName:   "example-delta",
		BackupObject: "20260308T120000Z-example-delta",
		BackupPolicy: "nightly",
	})

	if request.GenerateName != "example-delta-restore-" {
		t.Fatalf("unexpected generate name: %q", request.GenerateName)
	}
	if request.Spec.TargetRef.Name != "example-delta" {
		t.Fatalf("unexpected target name: %q", request.Spec.TargetRef.Name)
	}
	if request.Spec.BackupPolicyRef == nil || request.Spec.BackupPolicyRef.Name != "nightly" {
		t.Fatalf("expected backup policy reference to be set")
	}
	if request.Spec.BackupObject != "20260308T120000Z-example-delta" {
		t.Fatalf("unexpected backup object: %q", request.Spec.BackupObject)
	}
}

func TestRestoreObservationPrefersConfiguredReasonWhilePending(t *testing.T) {
	request := &fusekiv1alpha1.RestoreRequest{}
	request.Status.Phase = "Pending"
	request.Status.TargetName = "example-delta"
	request.Status.ResolvedBackupRef = "latest"
	request.Status.Conditions = []metav1.Condition{
		{Type: configuredConditionType, Reason: "BackupPolicyPending", Message: "Waiting for BackupPolicy \"nightly\" to become ready."},
		{Type: restoreCompletedConditionType, Reason: "WaitingForTarget", Message: "Waiting for restore prerequisites."},
	}

	observation := observeRestoreRequest(request)
	reason, message := observation.primaryReasonAndMessage()
	if reason != "BackupPolicyPending" {
		t.Fatalf("unexpected reason: %q", reason)
	}
	if message != "Waiting for BackupPolicy \"nightly\" to become ready." {
		t.Fatalf("unexpected message: %q", message)
	}
}

func TestFormatRestoreObservationIncludesReasonAndJob(t *testing.T) {
	formatted := formatRestoreObservation("example-restore", restoreObservation{
		Phase:             "Failed",
		TargetName:        "example-delta",
		ResolvedBackupRef: "20260308T120000Z-example-delta",
		JobName:           "example-restore-restore",
		RestoreReason:     "JobFailed",
		RestoreMessage:    "mc mirror returned exit code 1",
	})

	checks := []string{
		"phase=Failed",
		"target=example-delta",
		"backup=20260308T120000Z-example-delta",
		"job=example-restore-restore",
		"reason=JobFailed",
		"mc mirror returned exit code 1",
	}
	for _, check := range checks {
		if !strings.Contains(formatted, check) {
			t.Fatalf("expected formatted output to contain %q, got %q", check, formatted)
		}
	}
}

func TestSummarizeRestoreJobReportsFailedJob(t *testing.T) {
	job := &batchv1.Job{}
	job.Name = "example-restore-restore"
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{{
		Type:    batchv1.JobFailed,
		Status:  "True",
		Message: "mc mirror returned exit code 1",
	}}

	summary := summarizeRestoreJob(job)
	if summary.State != "Failed" {
		t.Fatalf("unexpected state: %q", summary.State)
	}
	if summary.Reason != "RestoreFailed" {
		t.Fatalf("unexpected reason: %q", summary.Reason)
	}
	if summary.Message != "mc mirror returned exit code 1" {
		t.Fatalf("unexpected message: %q", summary.Message)
	}
}

func TestFormatRestoreDescribeSummaryIncludesJobDetails(t *testing.T) {
	formatted := formatRestoreDescribeSummary(restoreDescribeSummary{
		Name:       "example-restore",
		Namespace:  "default",
		Phase:      "Running",
		Target:     "example-delta",
		Backup:     "latest",
		JobName:    "example-restore-restore",
		Configured: restoreConditionSummary{Status: "True", Reason: "TargetResolved", Message: "RDFDeltaServer \"example-delta\" is resolved."},
		Restore:    restoreConditionSummary{Status: "False", Reason: "RestoreRunning", Message: "Restore job \"example-restore-restore\" is running."},
		Job: &restoreJobSummary{
			Name:      "example-restore-restore",
			State:     "Running",
			Active:    1,
			Reason:    "RestoreRunning",
			Message:   "Restore job \"example-restore-restore\" is running.",
			StartTime: "2026-03-08T12:00:00Z",
		},
	})

	checks := []string{
		"RestoreRequest example-restore",
		"Namespace: default",
		"Phase: Running",
		"Job Status: Running active=1 succeeded=0 failed=0",
		"Job Detail: reason=RestoreRunning",
		"Job Start: 2026-03-08T12:00:00Z",
		"Configured: True",
		"Restore: False",
	}
	for _, check := range checks {
		if !strings.Contains(formatted, check) {
			t.Fatalf("expected formatted output to contain %q, got %q", check, formatted)
		}
	}
}

func TestSelectRestorePodNamePrefersRunningPods(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "restore-pod-succeeded", CreationTimestamp: metav1.NewTime(mustTime(t, "2026-03-08T11:00:00Z"))},
			Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "restore-pod-running", CreationTimestamp: metav1.NewTime(mustTime(t, "2026-03-08T10:00:00Z"))},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "restore-pod-pending", CreationTimestamp: metav1.NewTime(mustTime(t, "2026-03-08T12:00:00Z"))},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		},
	}

	selected := selectRestorePodName(pods)
	if selected != "restore-pod-running" {
		t.Fatalf("unexpected selected pod: %q", selected)
	}
}

func TestSelectRestorePodNamePrefersNewestWhenPriorityMatches(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "restore-pod-old", CreationTimestamp: metav1.NewTime(mustTime(t, "2026-03-08T10:00:00Z"))},
			Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "restore-pod-new", CreationTimestamp: metav1.NewTime(mustTime(t, "2026-03-08T12:00:00Z"))},
			Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
		},
	}

	selected := selectRestorePodName(pods)
	if selected != "restore-pod-new" {
		t.Fatalf("unexpected selected pod: %q", selected)
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}
