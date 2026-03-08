package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

type backupTriggerOptions struct {
	root      *RootOptions
	Namespace string
	Wait      bool
	Timeout   time.Duration
}

func newBackupCommand(root *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Trigger and inspect backup workflows",
	}
	cmd.AddCommand(newBackupTriggerCommand(root))
	return cmd
}

func newBackupTriggerCommand(root *RootOptions) *cobra.Command {
	options := &backupTriggerOptions{
		root:    root,
		Wait:    true,
		Timeout: 10 * time.Minute,
	}

	cmd := &cobra.Command{
		Use:   "trigger RDFDELTASERVER",
		Short: "Create an on-demand backup job from the managed RDF Delta backup CronJob",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}

			namespace, err := options.effectiveNamespace()
			if err != nil {
				return err
			}

			server := &fusekiv1alpha1.RDFDeltaServer{}
			if err := kubeClient.Get(cmd.Context(), client.ObjectKey{Namespace: namespace, Name: args[0]}, server); err != nil {
				return err
			}

			cronJobName := server.Status.BackupCronJobName
			if cronJobName == "" {
				if server.Spec.BackupPolicyRef == nil {
					return fmt.Errorf("rdfdeltaserver/%s has no backup policy configured", server.Name)
				}
				cronJobName = server.BackupCronJobName()
			}

			cronJob := &batchv1.CronJob{}
			if err := kubeClient.Get(cmd.Context(), client.ObjectKey{Namespace: namespace, Name: cronJobName}, cronJob); err != nil {
				if apierrors.IsNotFound(err) {
					return fmt.Errorf("backup cronjob %q for rdfdeltaserver/%s was not found", cronJobName, server.Name)
				}
				return err
			}

			job := manualBackupJobFromCronJob(cronJob)
			if err := kubeClient.Create(cmd.Context(), job); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "created backup job %s in namespace %s\n", job.Name, job.Namespace)
			if !options.Wait {
				return nil
			}

			if err := waitForJobCompletion(cmd.Context(), kubeClient, client.ObjectKeyFromObject(job), options.Timeout); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "backup job %s completed successfully\n", job.Name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace containing the RDFDeltaServer. Defaults to the current kubeconfig namespace.")
	cmd.Flags().BoolVar(&options.Wait, "wait", options.Wait, "Wait for the backup job to finish.")
	cmd.Flags().DurationVar(&options.Timeout, "timeout", options.Timeout, "Maximum time to wait for backup completion.")

	return cmd
}

func (o *backupTriggerOptions) effectiveNamespace() (string, error) {
	if o.Namespace != "" {
		return o.Namespace, nil
	}
	return o.root.currentNamespace()
}

func manualBackupJobFromCronJob(cronJob *batchv1.CronJob) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    cronJob.Namespace,
			GenerateName: cronJob.Name + "-manual-",
			Labels:       mergeStringMaps(copyStringMap(cronJob.Labels), cronJob.Spec.JobTemplate.Labels),
			Annotations:  mergeStringMaps(copyStringMap(cronJob.Spec.JobTemplate.Annotations), map[string]string{"cronjob.kubernetes.io/instantiate": "manual", "fuseki.apache.org/source-cronjob": cronJob.Name}),
		},
		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	}

	job.Labels = mergeStringMaps(job.Labels, map[string]string{"fuseki.apache.org/backup-trigger": "manual", "fuseki.apache.org/source-cronjob": cronJob.Name})
	return job
}

func waitForJobCompletion(ctx context.Context, kubeClient client.Client, key client.ObjectKey, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		job := &batchv1.Job{}
		if err := kubeClient.Get(ctx, key, job); err != nil {
			return false, err
		}

		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				return false, fmt.Errorf("backup job %s failed: %s", job.Name, condition.Message)
			}
		}

		return false, nil
	})
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func mergeStringMaps(base map[string]string, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	merged := make(map[string]string, len(base)+len(overlay))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}
