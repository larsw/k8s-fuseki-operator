package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

type restoreTriggerOptions struct {
	root         *RootOptions
	Namespace    string
	Name         string
	BackupObject string
	BackupPolicy string
	Wait         bool
	Timeout      time.Duration
}

func newRestoreCommand(root *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Create and inspect restore workflows",
	}
	cmd.AddCommand(newRestoreTriggerCommand(root), newRestoreDescribeCommand(root), newRestoreLogsCommand(root))
	return cmd
}

type restoreDescribeOptions struct {
	root      *RootOptions
	Namespace string
	Output    string
}

type restoreLogsOptions struct {
	root      *RootOptions
	Namespace string
	Pod       string
	Container string
	Follow    bool
	Previous  bool
	Tail      int64
}

func newRestoreTriggerCommand(root *RootOptions) *cobra.Command {
	options := &restoreTriggerOptions{
		root:    root,
		Wait:    true,
		Timeout: 10 * time.Minute,
	}

	cmd := &cobra.Command{
		Use:   "trigger RDFDELTASERVER",
		Short: "Create a RestoreRequest for an RDFDeltaServer",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}

			namespace, err := namespaceOrCurrent(options.root, options.Namespace)
			if err != nil {
				return err
			}

			server := &fusekiv1alpha1.RDFDeltaServer{}
			if err := kubeClient.Get(cmd.Context(), client.ObjectKey{Namespace: namespace, Name: args[0]}, server); err != nil {
				return err
			}

			request := buildRestoreRequest(restoreRequestParams{
				Namespace:    namespace,
				Name:         options.Name,
				TargetName:   server.Name,
				BackupObject: options.BackupObject,
				BackupPolicy: options.BackupPolicy,
			})
			if err := kubeClient.Create(cmd.Context(), request); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "created restorerequest %s in namespace %s\n", request.Name, request.Namespace)
			if !options.Wait {
				return nil
			}

			if err := waitForRestoreCompletion(cmd.Context(), cmd.OutOrStdout(), kubeClient, types.NamespacedName{Namespace: request.Namespace, Name: request.Name}, options.Timeout); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restorerequest %s completed successfully\n", request.Name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace containing the RDFDeltaServer. Defaults to the current kubeconfig namespace.")
	cmd.Flags().StringVar(&options.Name, "name", "", "Name for the RestoreRequest. Defaults to a generated name.")
	cmd.Flags().StringVar(&options.BackupObject, "backup-object", "", "Backup object to restore. Defaults to the controller's latest selection.")
	cmd.Flags().StringVar(&options.BackupPolicy, "backup-policy", "", "BackupPolicy to use for the restore. Defaults to the RDFDeltaServer reference.")
	cmd.Flags().BoolVar(&options.Wait, "wait", options.Wait, "Wait for the restore request to finish.")
	cmd.Flags().DurationVar(&options.Timeout, "timeout", options.Timeout, "Maximum time to wait for restore completion.")

	return cmd
}

type restoreRequestParams struct {
	Namespace    string
	Name         string
	TargetName   string
	BackupObject string
	BackupPolicy string
}

func buildRestoreRequest(params restoreRequestParams) *fusekiv1alpha1.RestoreRequest {
	request := &fusekiv1alpha1.RestoreRequest{}
	request.Namespace = params.Namespace
	if params.Name != "" {
		request.Name = params.Name
	} else {
		request.GenerateName = params.TargetName + "-restore-"
	}
	request.Spec.TargetRef.Kind = fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer
	request.Spec.TargetRef.Name = params.TargetName
	request.Spec.BackupObject = params.BackupObject
	if params.BackupPolicy != "" {
		request.Spec.BackupPolicyRef = localObjectReference(params.BackupPolicy)
	}
	return request
}

const (
	configuredConditionType       = "Configured"
	restoreCompletedConditionType = "RestoreCompleted"
)

type restoreObservation struct {
	Phase             string
	TargetName        string
	ResolvedBackupRef string
	JobName           string
	ConfiguredReason  string
	ConfiguredMessage string
	RestoreReason     string
	RestoreMessage    string
}

type restoreConditionSummary struct {
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type restoreJobSummary struct {
	Name           string `json:"name,omitempty"`
	State          string `json:"state,omitempty"`
	Active         int32  `json:"active,omitempty"`
	Succeeded      int32  `json:"succeeded,omitempty"`
	Failed         int32  `json:"failed,omitempty"`
	StartTime      string `json:"startTime,omitempty"`
	CompletionTime string `json:"completionTime,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Message        string `json:"message,omitempty"`
}

type restoreDescribeSummary struct {
	Name       string                  `json:"name"`
	Namespace  string                  `json:"namespace"`
	Phase      string                  `json:"phase"`
	Target     string                  `json:"target,omitempty"`
	Backup     string                  `json:"backup,omitempty"`
	JobName    string                  `json:"jobName,omitempty"`
	Configured restoreConditionSummary `json:"configured,omitempty"`
	Restore    restoreConditionSummary `json:"restore,omitempty"`
	Job        *restoreJobSummary      `json:"job,omitempty"`
}

func newRestoreDescribeCommand(root *RootOptions) *cobra.Command {
	options := &restoreDescribeOptions{root: root, Output: "text"}
	cmd := &cobra.Command{
		Use:   "describe NAME",
		Short: "Describe an existing RestoreRequest",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}
			namespace, err := namespaceOrCurrent(options.root, options.Namespace)
			if err != nil {
				return err
			}
			request := &fusekiv1alpha1.RestoreRequest{}
			if err := kubeClient.Get(cmd.Context(), client.ObjectKey{Namespace: namespace, Name: args[0]}, request); err != nil {
				return err
			}
			summary, err := summarizeRestoreRequest(cmd.Context(), kubeClient, request)
			if err != nil {
				return err
			}
			switch options.Output {
			case "json":
				encoded, err := json.MarshalIndent(summary, "", "  ")
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(cmd.OutOrStdout(), string(encoded))
				return err
			case "text":
				_, err := fmt.Fprint(cmd.OutOrStdout(), formatRestoreDescribeSummary(summary))
				return err
			default:
				return fmt.Errorf("unsupported output format %q", options.Output)
			}
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace containing the RestoreRequest.")
	cmd.Flags().StringVarP(&options.Output, "output", "o", options.Output, "Output format: text or json.")
	return cmd
}

func newRestoreLogsCommand(root *RootOptions) *cobra.Command {
	options := &restoreLogsOptions{root: root, Container: "restore", Tail: -1}
	cmd := &cobra.Command{
		Use:   "logs NAME",
		Short: "Stream logs for the restore Job or one of its Pods",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}
			clientset, err := options.root.kubernetesClientset()
			if err != nil {
				return err
			}
			namespace, err := namespaceOrCurrent(options.root, options.Namespace)
			if err != nil {
				return err
			}
			request := &fusekiv1alpha1.RestoreRequest{}
			if err := kubeClient.Get(cmd.Context(), client.ObjectKey{Namespace: namespace, Name: args[0]}, request); err != nil {
				return err
			}

			podName, err := resolveRestoreLogPod(cmd.Context(), kubeClient, request, options.Pod)
			if err != nil {
				return err
			}

			podLogOptions := &corev1.PodLogOptions{
				Container: options.Container,
				Follow:    options.Follow,
				Previous:  options.Previous,
			}
			if options.Tail >= 0 {
				podLogOptions.TailLines = &options.Tail
			}

			fmt.Fprintf(cmd.OutOrStdout(), "streaming restore logs from pod %s in namespace %s\n", podName, request.Namespace)
			stream, err := clientset.CoreV1().Pods(request.Namespace).GetLogs(podName, podLogOptions).Stream(cmd.Context())
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = io.Copy(cmd.OutOrStdout(), stream)
			return err
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace containing the RestoreRequest.")
	cmd.Flags().StringVar(&options.Pod, "pod", "", "Specific restore Pod to stream logs from.")
	cmd.Flags().StringVar(&options.Container, "container", options.Container, "Container name to stream logs from.")
	cmd.Flags().BoolVarP(&options.Follow, "follow", "f", false, "Follow the log stream.")
	cmd.Flags().BoolVar(&options.Previous, "previous", false, "Return logs from the previous container instance.")
	cmd.Flags().Int64Var(&options.Tail, "tail", options.Tail, "Number of log lines to show. Use -1 for all lines.")
	return cmd
}

func waitForRestoreCompletion(ctx context.Context, out io.Writer, kubeClient client.Client, key types.NamespacedName, timeout time.Duration) error {
	lastObservation := restoreObservation{}
	haveObservation := false

	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		request := &fusekiv1alpha1.RestoreRequest{}
		if err := kubeClient.Get(ctx, key, request); err != nil {
			if errors.IsNotFound(err) {
				return false, err
			}
			return false, err
		}

		observation := observeRestoreRequest(request)
		if !haveObservation || observation != lastObservation {
			fmt.Fprintln(out, formatRestoreObservation(request.Name, observation))
			lastObservation = observation
			haveObservation = true
		}

		switch observation.Phase {
		case "Succeeded":
			return true, nil
		case "Failed":
			reason, message := observation.primaryReasonAndMessage()
			return false, fmt.Errorf("restorerequest %s failed: reason=%s message=%s", request.Name, emptyDash(reason), emptyDash(message))
		default:
			return false, nil
		}
	})
}

func observeRestoreRequest(request *fusekiv1alpha1.RestoreRequest) restoreObservation {
	configured := apimeta.FindStatusCondition(request.Status.Conditions, configuredConditionType)
	restore := apimeta.FindStatusCondition(request.Status.Conditions, restoreCompletedConditionType)
	phase := request.Status.Phase
	if phase == "" {
		phase = "Pending"
	}

	observation := restoreObservation{
		Phase:             phase,
		TargetName:        request.Status.TargetName,
		ResolvedBackupRef: request.Status.ResolvedBackupRef,
		JobName:           request.Status.JobName,
	}
	if configured != nil {
		observation.ConfiguredReason = configured.Reason
		observation.ConfiguredMessage = configured.Message
	}
	if restore != nil {
		observation.RestoreReason = restore.Reason
		observation.RestoreMessage = restore.Message
	}
	return observation
}

func formatRestoreObservation(name string, observation restoreObservation) string {
	parts := []string{fmt.Sprintf("restorerequest %s", name), fmt.Sprintf("phase=%s", observation.Phase)}
	if observation.TargetName != "" {
		parts = append(parts, fmt.Sprintf("target=%s", observation.TargetName))
	}
	if observation.ResolvedBackupRef != "" {
		parts = append(parts, fmt.Sprintf("backup=%s", observation.ResolvedBackupRef))
	}
	if observation.JobName != "" {
		parts = append(parts, fmt.Sprintf("job=%s", observation.JobName))
	}
	reason, message := observation.primaryReasonAndMessage()
	if reason != "" {
		parts = append(parts, fmt.Sprintf("reason=%s", reason))
	}
	if message != "" {
		parts = append(parts, fmt.Sprintf("message=%q", message))
	}
	return strings.Join(parts, " ")
}

func summarizeRestoreRequest(ctx context.Context, kubeClient client.Client, request *fusekiv1alpha1.RestoreRequest) (restoreDescribeSummary, error) {
	configured := apimeta.FindStatusCondition(request.Status.Conditions, configuredConditionType)
	restore := apimeta.FindStatusCondition(request.Status.Conditions, restoreCompletedConditionType)
	summary := restoreDescribeSummary{
		Name:       request.Name,
		Namespace:  request.Namespace,
		Phase:      defaultString(request.Status.Phase, "Pending"),
		Target:     defaultString(request.Status.TargetName, request.Spec.TargetRef.Name),
		Backup:     defaultString(request.Status.ResolvedBackupRef, request.DesiredResolvedBackupRef()),
		JobName:    request.Status.JobName,
		Configured: summarizeCondition(configured),
		Restore:    summarizeCondition(restore),
	}
	if request.Status.JobName != "" {
		job := &batchv1.Job{}
		err := kubeClient.Get(ctx, client.ObjectKey{Namespace: request.Namespace, Name: request.Status.JobName}, job)
		if err != nil {
			if errors.IsNotFound(err) {
				summary.Job = &restoreJobSummary{Name: request.Status.JobName, State: "Missing", Reason: "JobNotFound", Message: fmt.Sprintf("Restore job %q has not been created yet.", request.Status.JobName)}
				return summary, nil
			}
			return restoreDescribeSummary{}, err
		}
		summary.Job = summarizeRestoreJob(job)
	}
	return summary, nil
}

func summarizeCondition(condition *metav1.Condition) restoreConditionSummary {
	if condition == nil {
		return restoreConditionSummary{}
	}
	return restoreConditionSummary{
		Status:  string(condition.Status),
		Reason:  condition.Reason,
		Message: condition.Message,
	}
}

func summarizeRestoreJob(job *batchv1.Job) *restoreJobSummary {
	summary := &restoreJobSummary{
		Name:      job.Name,
		Active:    job.Status.Active,
		Succeeded: job.Status.Succeeded,
		Failed:    job.Status.Failed,
		State:     "Pending",
	}
	if job.Status.StartTime != nil {
		summary.StartTime = job.Status.StartTime.Time.Format(time.RFC3339)
	}
	if job.Status.CompletionTime != nil {
		summary.CompletionTime = job.Status.CompletionTime.Time.Format(time.RFC3339)
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == "True" {
			summary.State = "Succeeded"
			summary.Reason = "RestoreCompleted"
			summary.Message = defaultString(condition.Message, fmt.Sprintf("Restore job %q completed successfully.", job.Name))
			return summary
		}
		if condition.Type == batchv1.JobFailed && condition.Status == "True" {
			summary.State = "Failed"
			summary.Reason = "RestoreFailed"
			summary.Message = defaultString(condition.Message, fmt.Sprintf("Restore job %q failed.", job.Name))
			return summary
		}
	}
	if job.Status.Active > 0 {
		summary.State = "Running"
		summary.Reason = "RestoreRunning"
		summary.Message = fmt.Sprintf("Restore job %q is running.", job.Name)
		return summary
	}
	summary.Reason = "RestorePending"
	summary.Message = fmt.Sprintf("Restore job %q is pending.", job.Name)
	return summary
}

func formatRestoreDescribeSummary(summary restoreDescribeSummary) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("RestoreRequest %s\n", summary.Name))
	builder.WriteString(fmt.Sprintf("Namespace: %s\n", summary.Namespace))
	builder.WriteString(fmt.Sprintf("Phase: %s\n", summary.Phase))
	if summary.Target != "" {
		builder.WriteString(fmt.Sprintf("Target: %s\n", summary.Target))
	}
	if summary.Backup != "" {
		builder.WriteString(fmt.Sprintf("Backup: %s\n", summary.Backup))
	}
	if summary.JobName != "" {
		builder.WriteString(fmt.Sprintf("Job: %s\n", summary.JobName))
	}
	if summary.Configured.Reason != "" || summary.Configured.Message != "" || summary.Configured.Status != "" {
		builder.WriteString(fmt.Sprintf("Configured: %s reason=%s message=%q\n", emptyDash(summary.Configured.Status), emptyDash(summary.Configured.Reason), summary.Configured.Message))
	}
	if summary.Restore.Reason != "" || summary.Restore.Message != "" || summary.Restore.Status != "" {
		builder.WriteString(fmt.Sprintf("Restore: %s reason=%s message=%q\n", emptyDash(summary.Restore.Status), emptyDash(summary.Restore.Reason), summary.Restore.Message))
	}
	if summary.Job != nil {
		builder.WriteString(fmt.Sprintf("Job Status: %s active=%d succeeded=%d failed=%d\n", summary.Job.State, summary.Job.Active, summary.Job.Succeeded, summary.Job.Failed))
		if summary.Job.Reason != "" || summary.Job.Message != "" {
			builder.WriteString(fmt.Sprintf("Job Detail: reason=%s message=%q\n", emptyDash(summary.Job.Reason), summary.Job.Message))
		}
		if summary.Job.StartTime != "" {
			builder.WriteString(fmt.Sprintf("Job Start: %s\n", summary.Job.StartTime))
		}
		if summary.Job.CompletionTime != "" {
			builder.WriteString(fmt.Sprintf("Job Completion: %s\n", summary.Job.CompletionTime))
		}
	}
	return builder.String()
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func resolveRestoreLogPod(ctx context.Context, kubeClient client.Client, request *fusekiv1alpha1.RestoreRequest, explicitPod string) (string, error) {
	if explicitPod != "" {
		return explicitPod, nil
	}

	jobName := defaultString(request.Status.JobName, request.JobName())
	pods, err := listRestorePodsForJob(ctx, kubeClient, request.Namespace, jobName)
	if err != nil {
		return "", err
	}
	if len(pods) == 0 {
		observation := observeRestoreRequest(request)
		reason, message := observation.primaryReasonAndMessage()
		return "", fmt.Errorf("restore job %q has no pods yet: phase=%s reason=%s message=%s", jobName, observation.Phase, emptyDash(reason), emptyDash(message))
	}
	return selectRestorePodName(pods), nil
}

func listRestorePodsForJob(ctx context.Context, kubeClient client.Client, namespace, jobName string) ([]corev1.Pod, error) {
	queries := []map[string]string{
		{"batch.kubernetes.io/job-name": jobName},
		{"job-name": jobName},
	}
	for _, labels := range queries {
		var pods corev1.PodList
		if err := kubeClient.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels(labels)); err != nil {
			return nil, err
		}
		if len(pods.Items) > 0 {
			return pods.Items, nil
		}
	}
	return nil, nil
}

func selectRestorePodName(pods []corev1.Pod) string {
	ordered := append([]corev1.Pod(nil), pods...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left := podPriority(ordered[i])
		right := podPriority(ordered[j])
		if left != right {
			return left > right
		}
		if !ordered[i].CreationTimestamp.Equal(&ordered[j].CreationTimestamp) {
			return ordered[i].CreationTimestamp.After(ordered[j].CreationTimestamp.Time)
		}
		return ordered[i].Name < ordered[j].Name
	})
	return ordered[0].Name
}

func podPriority(pod corev1.Pod) int {
	switch pod.Status.Phase {
	case corev1.PodRunning:
		return 3
	case corev1.PodPending:
		return 2
	case corev1.PodSucceeded:
		return 1
	default:
		return 0
	}
}

func (o restoreObservation) primaryReasonAndMessage() (string, string) {
	if o.Phase == "Pending" && (o.ConfiguredReason != "" || o.ConfiguredMessage != "") {
		return o.ConfiguredReason, o.ConfiguredMessage
	}
	if o.RestoreReason != "" || o.RestoreMessage != "" {
		return o.RestoreReason, o.RestoreMessage
	}
	if o.ConfiguredReason != "" || o.ConfiguredMessage != "" {
		return o.ConfiguredReason, o.ConfiguredMessage
	}
	return string(metav1.ConditionUnknown), ""
}
