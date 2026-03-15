package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

const (
	ingestCompletedConditionType     = "IngestCompleted"
	ingestGenerationAnnotation       = "fuseki.apache.org/observed-generation"
	ingestReportDirectoryAnnotation  = "fuseki.apache.org/ingest-report-directory"
	ingestSuccessfulJobsHistoryLimit = int32(1)
	ingestFailedJobsHistoryLimit     = int32(3)
	defaultInlineSHACLSourceFileName = "inline-shapes.ttl"
)

const ingestReportDirectory = transferWorkspaceMountPath + "/reports"

func ingestPipelineJobName(pipeline *fusekiv1alpha1.IngestPipeline) string {
	return pipeline.Name + "-ingest"
}

func ingestPipelineCronJobName(pipeline *fusekiv1alpha1.IngestPipeline) string {
	return pipeline.Name + "-ingest"
}

func ingestPipelineContainer(pipeline *fusekiv1alpha1.IngestPipeline, target resolvedTransferTarget, policy *fusekiv1alpha1.SHACLPolicy) (corev1.Container, error) {
	env, err := transferBaseEnvVars(target, transferDirectionImport)
	if err != nil {
		return corev1.Container{}, err
	}

	env = append(env,
		corev1.EnvVar{Name: "DATASET_NAME", Value: target.Dataset.Spec.Name},
		corev1.EnvVar{Name: "DATASET_DB_TYPE", Value: strings.ToLower(string(target.Dataset.DesiredType()))},
		corev1.EnvVar{Name: "FUSEKI_IMPORT_URL", Value: transferDataURL(target.Workload.ServiceURL, target.Dataset.Spec.Name, pipeline.Spec.Target.NamedGraph)},
		corev1.EnvVar{Name: "TRANSFER_SOURCE_TYPE", Value: string(pipeline.Spec.Source.Type)},
		corev1.EnvVar{Name: "TRANSFER_SOURCE_BASENAME", Value: transferSourceBaseName(pipeline.Spec.Source)},
		corev1.EnvVar{Name: "SHACL_SOURCE_COUNT", Value: strconv.Itoa(len(policy.Spec.Sources))},
		corev1.EnvVar{Name: "SHACL_FAILURE_ACTION", Value: string(policy.DesiredFailureAction())},
	)
	if pipeline.Spec.Source.URI != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SOURCE_URI", Value: pipeline.Spec.Source.URI})
	}
	if pipeline.Spec.Source.Path != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SOURCE_PATH", Value: pipeline.Spec.Source.Path})
	}
	if pipeline.Spec.Source.Format != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SOURCE_MEDIA_TYPE", Value: pipeline.Spec.Source.Format})
	}
	if policy.Spec.ReportFormat != "" {
		env = append(env, corev1.EnvVar{Name: "SHACL_REPORT_FORMAT", Value: policy.Spec.ReportFormat})
	}

	env, err = appendTransferSourceEnvVars(env, pipeline.Spec.Source)
	if err != nil {
		return corev1.Container{}, err
	}
	env = append(env, shaclSourceEnvVars(policy)...)

	return corev1.Container{
		Name:            "ingest",
		Image:           target.Workload.Image,
		ImagePullPolicy: target.Workload.ImagePullPolicy,
		Command:         []string{"/bin/bash", "-ceu", ingestPipelineScript()},
		Env:             env,
		VolumeMounts:    transferVolumeMounts(target.SecurityProfile),
	}, nil
}

func shaclSourceEnvVars(policy *fusekiv1alpha1.SHACLPolicy) []corev1.EnvVar {
	env := make([]corev1.EnvVar, 0, len(policy.Spec.Sources)*2)
	for index, source := range policy.Spec.Sources {
		nameVar := fmt.Sprintf("SHACL_SOURCE_NAME_%d", index)
		contentVar := fmt.Sprintf("SHACL_SOURCE_%d", index)
		env = append(env, corev1.EnvVar{Name: nameVar, Value: shaclSourceFileName(source, index)})
		switch source.Type {
		case fusekiv1alpha1.SHACLSourceTypeConfigMap:
			env = append(env, corev1.EnvVar{
				Name: contentVar,
				ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: *source.ConfigMapRef,
					Key:                  source.Key,
				}},
			})
		default:
			env = append(env, corev1.EnvVar{Name: contentVar, Value: source.Inline})
		}
	}
	return env
}

func shaclSourceFileName(source fusekiv1alpha1.SHACLSource, index int) string {
	if source.Type == fusekiv1alpha1.SHACLSourceTypeConfigMap {
		baseName := filepath.Base(source.Key)
		if baseName != "." && baseName != string(filepath.Separator) && baseName != "" {
			return baseName
		}
	}
	if index == 0 {
		return defaultInlineSHACLSourceFileName
	}
	return fmt.Sprintf("inline-shapes-%d.ttl", index)
}

func reconcileIngestJob(ctx context.Context, c client.Client, scheme *runtime.Scheme, pipeline *fusekiv1alpha1.IngestPipeline, target resolvedTransferTarget, policy *fusekiv1alpha1.SHACLPolicy) (*batchv1.Job, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: ingestPipelineJobName(pipeline), Namespace: pipeline.Namespace}}
	err := c.Get(ctx, client.ObjectKeyFromObject(job), job)
	if err == nil {
		if job.Annotations[ingestGenerationAnnotation] == strconv.FormatInt(pipeline.Generation, 10) {
			return job, nil
		}
		if err := c.Delete(ctx, job); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	container, err := ingestPipelineContainer(pipeline, target, policy)
	if err != nil {
		return nil, err
	}
	job.Labels = transferRequestLabels("ingest", pipeline.Name, target.Dataset.Name, target.Workload)
	job.Annotations = map[string]string{
		ingestGenerationAnnotation:      strconv.FormatInt(pipeline.Generation, 10),
		ingestReportDirectoryAnnotation: ingestReportDirectory,
	}
	job.Spec.BackoffLimit = ptrTo(int32(0))
	job.Spec.Template.ObjectMeta.Labels = mergeStringMaps(job.Labels, map[string]string{"job-name": job.Name, "batch.kubernetes.io/job-name": job.Name})
	job.Spec.Template.ObjectMeta.Annotations = map[string]string{
		ingestGenerationAnnotation:      strconv.FormatInt(pipeline.Generation, 10),
		ingestReportDirectoryAnnotation: ingestReportDirectory,
	}
	job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	job.Spec.Template.Spec.Containers = []corev1.Container{container}
	job.Spec.Template.Spec.Volumes = transferJobVolumes(target.SecurityProfile)
	if err := controllerutil.SetControllerReference(pipeline, job, scheme); err != nil {
		return nil, err
	}
	if err := c.Create(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func reconcileIngestCronJob(ctx context.Context, c client.Client, scheme *runtime.Scheme, pipeline *fusekiv1alpha1.IngestPipeline, target resolvedTransferTarget, policy *fusekiv1alpha1.SHACLPolicy) (*batchv1.CronJob, error) {
	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: ingestPipelineCronJobName(pipeline), Namespace: pipeline.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, cronJob, func() error {
		container, containerErr := ingestPipelineContainer(pipeline, target, policy)
		if containerErr != nil {
			return containerErr
		}
		cronJob.Labels = transferRequestLabels("ingest", pipeline.Name, target.Dataset.Name, target.Workload)
		cronJob.Annotations = mergeStringMaps(cronJob.Annotations, map[string]string{
			ingestGenerationAnnotation:      strconv.FormatInt(pipeline.Generation, 10),
			ingestReportDirectoryAnnotation: ingestReportDirectory,
		})
		cronJob.Spec.Schedule = pipeline.Spec.Schedule
		cronJob.Spec.Suspend = ptrTo(pipeline.Spec.Suspend)
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		cronJob.Spec.SuccessfulJobsHistoryLimit = ptrTo(ingestSuccessfulJobsHistoryLimit)
		cronJob.Spec.FailedJobsHistoryLimit = ptrTo(ingestFailedJobsHistoryLimit)
		cronJob.Spec.JobTemplate.ObjectMeta.Labels = mergeStringMaps(cronJob.Labels, map[string]string{"fuseki.apache.org/component": "ingest-job"})
		cronJob.Spec.JobTemplate.ObjectMeta.Annotations = map[string]string{
			ingestGenerationAnnotation:      strconv.FormatInt(pipeline.Generation, 10),
			ingestReportDirectoryAnnotation: ingestReportDirectory,
		}
		cronJob.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels = mergeStringMaps(cronJob.Labels, map[string]string{"fuseki.apache.org/component": "ingest-job"})
		cronJob.Spec.JobTemplate.Spec.Template.ObjectMeta.Annotations = map[string]string{
			ingestGenerationAnnotation:      strconv.FormatInt(pipeline.Generation, 10),
			ingestReportDirectoryAnnotation: ingestReportDirectory,
		}
		cronJob.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers = []corev1.Container{container}
		cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes = transferJobVolumes(target.SecurityProfile)
		return controllerutil.SetControllerReference(pipeline, cronJob, scheme)
	})
	if err != nil {
		return nil, err
	}
	return cronJob, nil
}

func deleteIngestJob(ctx context.Context, c client.Client, namespace, name string) error {
	if name == "" {
		return nil
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return client.IgnoreNotFound(c.Delete(ctx, job, client.GracePeriodSeconds(0)))
}

func deleteIngestCronJob(ctx context.Context, c client.Client, namespace, name string) error {
	if name == "" {
		return nil
	}
	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return client.IgnoreNotFound(c.Delete(ctx, cronJob))
}

func ingestJobProgress(job *batchv1.Job, jobName string) (string, *metav1.Time, metav1.ConditionStatus, string, string) {
	if job == nil {
		return "Pending", nil, metav1.ConditionFalse, "IngestPending", "Waiting for ingest job creation."
	}
	reportDirectory := job.Annotations[ingestReportDirectoryAnnotation]
	if reportDirectory == "" {
		reportDirectory = ingestReportDirectory
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return "Succeeded", job.Status.CompletionTime, metav1.ConditionTrue, "IngestCompleted", fmt.Sprintf("Ingest job %q completed successfully. SHACL reports were written under %s.", jobName, reportDirectory)
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			message := condition.Message
			if message == "" {
				message = fmt.Sprintf("Ingest job %q failed. Inspect SHACL reports under %s.", jobName, reportDirectory)
			}
			lastRunTime := job.Status.CompletionTime
			if lastRunTime == nil && job.Status.StartTime != nil {
				lastRunTime = job.Status.StartTime
			}
			return "Failed", lastRunTime, metav1.ConditionFalse, "IngestFailed", message
		}
	}

	if job.Status.StartTime != nil {
		return "Running", job.Status.StartTime, metav1.ConditionFalse, "IngestRunning", fmt.Sprintf("Ingest job %q is running. SHACL reports will be written under %s.", jobName, reportDirectory)
	}
	return "Running", nil, metav1.ConditionFalse, "IngestPending", fmt.Sprintf("Ingest job %q is pending. SHACL reports will be written under %s.", jobName, reportDirectory)
}

func ingestCronJobProgress(cronJob *batchv1.CronJob, pipelineName string, suspended bool) (string, *metav1.Time, metav1.ConditionStatus, string, string) {
	if suspended {
		return "Suspended", cronJob.Status.LastScheduleTime, metav1.ConditionFalse, "IngestSuspended", fmt.Sprintf("IngestPipeline %q is suspended.", pipelineName)
	}
	if cronJob.Status.LastScheduleTime != nil {
		return "Running", cronJob.Status.LastScheduleTime, metav1.ConditionFalse, "IngestScheduled", fmt.Sprintf("IngestPipeline %q is scheduled and last ran at %s.", pipelineName, cronJob.Status.LastScheduleTime.Time.UTC().Format(time.RFC3339))
	}
	return "Running", nil, metav1.ConditionFalse, "IngestScheduled", fmt.Sprintf("IngestPipeline %q is scheduled and waiting for its first run.", pipelineName)
}

func ingestPipelineScript() string {
	script := transferScriptPrelude() + `
stage_dir='__TRANSFER_WORKSPACE__/source'
shapes_dir='__TRANSFER_WORKSPACE__/shapes'
report_dir='__TRANSFER_WORKSPACE__/reports'
mkdir -p "${stage_dir}" "${shapes_dir}" "${report_dir}"
case "${TRANSFER_SOURCE_TYPE}" in
  URL)
    curl_args=(--silent --show-error --fail -L)
    if [[ -n "${TRANSFER_SOURCE_USERNAME:-}" ]] || [[ -n "${TRANSFER_SOURCE_PASSWORD:-}" ]]; then curl_args+=(-u "${TRANSFER_SOURCE_USERNAME:-}:${TRANSFER_SOURCE_PASSWORD:-}"); fi
    if [[ -n "${TRANSFER_SOURCE_AUTH_HEADER:-}" ]]; then curl_args+=(-H "${TRANSFER_SOURCE_AUTH_HEADER}"); fi
    curl "${curl_args[@]}" "${TRANSFER_SOURCE_URI}" -o "${stage_dir}/${TRANSFER_SOURCE_BASENAME}"
    ;;
  S3)
    setup_s3_client
    source_path="transfer/${TRANSFER_S3_BUCKET}"
    if [[ -n "${TRANSFER_S3_OBJECT:-}" ]]; then source_path="${source_path}/${TRANSFER_S3_OBJECT}"; fi
    if [[ -z "${TRANSFER_S3_OBJECT:-}" ]] || [[ "${TRANSFER_S3_OBJECT%/}" != "${TRANSFER_S3_OBJECT}" ]]; then
      "${TRANSFER_MC_BIN}" "${TRANSFER_MC_FLAGS[@]}" cp --recursive "${source_path}" "${stage_dir}/"
    else
      "${TRANSFER_MC_BIN}" "${TRANSFER_MC_FLAGS[@]}" cp "${source_path}" "${stage_dir}/${TRANSFER_SOURCE_BASENAME}"
    fi
    ;;
  Filesystem)
    if [[ -d "${TRANSFER_SOURCE_PATH}" ]]; then cp -R "${TRANSFER_SOURCE_PATH}/." "${stage_dir}/"; else cp "${TRANSFER_SOURCE_PATH}" "${stage_dir}/${TRANSFER_SOURCE_BASENAME}"; fi
    ;;
  *)
    echo "unsupported source type: ${TRANSFER_SOURCE_TYPE}" >&2
    exit 1
    ;;
esac
first_file=$(find "${stage_dir}" -type f | head -n 1 || true)
if [[ -z "${first_file}" ]]; then echo 'no ingest files were staged' >&2; exit 1; fi
shape_count="${SHACL_SOURCE_COUNT:-0}"
if [[ "${shape_count}" -eq 0 ]]; then echo 'no SHACL sources were configured' >&2; exit 1; fi
for index in $(seq 0 $((shape_count - 1))); do
  name_var="SHACL_SOURCE_NAME_${index}"
  content_var="SHACL_SOURCE_${index}"
  source_name="${!name_var}"
  source_content="${!content_var}"
  if [[ -z "${source_name}" ]]; then source_name="inline-shapes-${index}.ttl"; fi
  printf '%s\n' "${source_content}" > "${shapes_dir}/${source_name}"
done
shapes_file="${shapes_dir}/combined-shapes.ttl"
rm -f "${shapes_file}"
while IFS= read -r source_file; do
  cat "${source_file}" >> "${shapes_file}"
  printf '\n' >> "${shapes_file}"
done < <(find "${shapes_dir}" -type f ! -name 'combined-shapes.ttl' | sort)
if [[ ! -s "${shapes_file}" ]]; then echo 'no SHACL shapes were materialized' >&2; exit 1; fi
shacl_bin="${FUSEKI_HOME:-/opt/fuseki}/bin/shacl"
if [[ ! -x "${shacl_bin}" ]]; then echo "SHACL CLI not found at ${shacl_bin}" >&2; exit 1; fi
wait_for_fuseki
if [[ -n "${FUSEKI_ADMIN_USER:-}" ]] && [[ -n "${FUSEKI_ADMIN_PASSWORD:-}" ]]; then
  status=$(curl_fuseki --silent --output /tmp/fuseki-create.out --write-out '%{http_code}' -u "${FUSEKI_ADMIN_USER}:${FUSEKI_ADMIN_PASSWORD}" -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' --data "dbName=${DATASET_NAME}&dbType=${DATASET_DB_TYPE}" "${FUSEKI_BASE_URL}/$/datasets")
  if [[ "${status}" != '200' ]] && [[ "${status}" != '201' ]] && [[ "${status}" != '409' ]]; then cat /tmp/fuseki-create.out; exit 1; fi
fi
auth_args=()
if [[ -n "${FUSEKI_ADMIN_USER:-}" ]] && [[ -n "${FUSEKI_ADMIN_PASSWORD:-}" ]]; then auth_args=(-u "${FUSEKI_ADMIN_USER}:${FUSEKI_ADMIN_PASSWORD}"); fi
while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  content_type="${TRANSFER_SOURCE_MEDIA_TYPE:-}"
  if [[ -z "${content_type}" ]]; then content_type=$(content_type_for_file "${file}"); fi
  validation_input="${file}"
  temporary_input=''
  if [[ "${file}" == *.gz ]]; then
    temporary_input=$(mktemp "${report_dir}/validated-data-XXXXXX")
    gzip -cd "${file}" > "${temporary_input}"
    validation_input="${temporary_input}"
  fi
  report_file="${report_dir}/$(basename "${file}").report.txt"
  if ! "${shacl_bin}" validate --shapes "${shapes_file}" --data "${validation_input}" > "${report_file}" 2>&1; then
    cat "${report_file}" >&2
    if [[ "${SHACL_FAILURE_ACTION:-Reject}" != 'ReportOnly' ]]; then
      [[ -n "${temporary_input}" ]] && rm -f "${temporary_input}"
      exit 1
    fi
  fi
  if [[ "${file}" == *.gz ]]; then
    gzip -cd "${file}" | curl_fuseki --silent --show-error --fail "${auth_args[@]}" -X POST -H "Content-Type: ${content_type}" --data-binary @- "${FUSEKI_IMPORT_URL}"
  else
    curl_fuseki --silent --show-error --fail "${auth_args[@]}" -X POST -H "Content-Type: ${content_type}" --data-binary "@${file}" "${FUSEKI_IMPORT_URL}"
  fi
  [[ -n "${temporary_input}" ]] && rm -f "${temporary_input}"
done < <(find "${stage_dir}" -type f | sort)
`
	return strings.ReplaceAll(script, "__TRANSFER_WORKSPACE__", transferWorkspaceMountPath)
}
