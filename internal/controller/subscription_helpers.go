package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
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
	subscriptionDeliveredConditionType    = "SubscriptionDelivered"
	subscriptionGenerationAnnotation      = "fuseki.apache.org/subscription-generation"
	subscriptionStartCheckpointAnnotation = "fuseki.apache.org/subscription-start-checkpoint"
	subscriptionEndCheckpointAnnotation   = "fuseki.apache.org/subscription-end-checkpoint"
	subscriptionArtifactRefAnnotation     = "fuseki.apache.org/subscription-artifact-ref"
)

var rdfDeltaLogVersionFetcher = fetchRDFDeltaLogVersion

func changeSubscriptionJobName(subscription *fusekiv1alpha1.ChangeSubscription) string {
	return subscription.Name + "-delivery"
}

func changeSubscriptionLogName(subscription *fusekiv1alpha1.ChangeSubscription) string {
	if subscription.Spec.Target != nil && subscription.Spec.Target.DatasetRef.Name != "" {
		return subscription.Spec.Target.DatasetRef.Name
	}
	return subscription.Name
}

func changeSubscriptionServerURL(server *fusekiv1alpha1.RDFDeltaServer) string {
	return fmt.Sprintf("http://%s:%d", server.ServiceName(), server.DesiredServicePort())
}

func parseSubscriptionCheckpoint(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("invalid checkpoint %q: %w", raw, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("invalid checkpoint %q: must be non-negative", raw)
	}
	return value, nil
}

func changeSubscriptionArtifactRef(subscription *fusekiv1alpha1.ChangeSubscription, startVersion, endVersion int) string {
	fileName := fmt.Sprintf("%s-%012d-%012d.rdfpatch", subscription.Name, startVersion, endVersion)
	if strings.EqualFold(subscription.Spec.Sink.Compression, "gzip") && !strings.HasSuffix(fileName, ".gz") {
		fileName += ".gz"
	}

	switch subscription.Spec.Sink.Type {
	case fusekiv1alpha1.DataSinkTypeFilesystem:
		if strings.HasSuffix(subscription.Spec.Sink.Path, "/") {
			return path.Join(strings.TrimSuffix(subscription.Spec.Sink.Path, "/"), fileName)
		}
		return subscription.Spec.Sink.Path
	case fusekiv1alpha1.DataSinkTypeS3:
		if strings.HasSuffix(subscription.Spec.Sink.URI, "/") {
			return strings.TrimSuffix(subscription.Spec.Sink.URI, "/") + "/" + fileName
		}
		return subscription.Spec.Sink.URI
	default:
		return fileName
	}
}

func changeSubscriptionContainer(subscription *fusekiv1alpha1.ChangeSubscription, server *fusekiv1alpha1.RDFDeltaServer, startVersion, endVersion int) (corev1.Container, string, error) {
	artifactRef := changeSubscriptionArtifactRef(subscription, startVersion, endVersion)
	env := []corev1.EnvVar{
		{Name: "RDF_DELTA_BASE_URL", Value: changeSubscriptionServerURL(server)},
		{Name: "RDF_DELTA_LOG_NAME", Value: changeSubscriptionLogName(subscription)},
		{Name: "SUBSCRIPTION_START_VERSION", Value: strconv.Itoa(startVersion)},
		{Name: "SUBSCRIPTION_END_VERSION", Value: strconv.Itoa(endVersion)},
		{Name: "TRANSFER_SINK_TYPE", Value: string(subscription.Spec.Sink.Type)},
		{Name: "TRANSFER_ARTIFACT_REF", Value: artifactRef},
		{Name: "TRANSFER_OUTPUT_PATH", Value: path.Join(transferWorkspaceMountPath, path.Base(artifactRef))},
	}
	if subscription.Spec.Sink.URI != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SINK_URI", Value: subscription.Spec.Sink.URI})
	}
	if subscription.Spec.Sink.Path != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SINK_PATH", Value: subscription.Spec.Sink.Path})
	}
	if subscription.Spec.Sink.Compression != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SINK_COMPRESSION", Value: subscription.Spec.Sink.Compression})
	}

	var err error
	env, err = appendTransferSinkEnvVars(env, subscription.Spec.Sink, artifactRef)
	if err != nil {
		return corev1.Container{}, "", err
	}

	return corev1.Container{
		Name:            "deliver",
		Image:           server.Spec.Image,
		ImagePullPolicy: server.DesiredImagePullPolicy(),
		Command:         []string{"/bin/sh", "-ceu", changeSubscriptionScript()},
		Env:             env,
		VolumeMounts:    transferVolumeMounts(nil),
	}, artifactRef, nil
}

func reconcileChangeSubscriptionJob(ctx context.Context, c client.Client, scheme *runtime.Scheme, subscription *fusekiv1alpha1.ChangeSubscription, server *fusekiv1alpha1.RDFDeltaServer, startVersion, endVersion int) (*batchv1.Job, string, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: changeSubscriptionJobName(subscription), Namespace: subscription.Namespace}}
	existingErr := c.Get(ctx, client.ObjectKeyFromObject(job), job)
	if existingErr == nil {
		if job.Annotations[subscriptionGenerationAnnotation] == strconv.FormatInt(subscription.Generation, 10) &&
			job.Annotations[subscriptionStartCheckpointAnnotation] == strconv.Itoa(startVersion) &&
			job.Annotations[subscriptionEndCheckpointAnnotation] == strconv.Itoa(endVersion) {
			return job, job.Annotations[subscriptionArtifactRefAnnotation], nil
		}
		if err := c.Delete(ctx, job); err != nil {
			return nil, "", err
		}
		return nil, "", nil
	}
	if !apierrors.IsNotFound(existingErr) {
		return nil, "", existingErr
	}

	container, artifactRef, err := changeSubscriptionContainer(subscription, server, startVersion, endVersion)
	if err != nil {
		return nil, "", err
	}
	job.Labels = map[string]string{
		"app.kubernetes.io/name":       "rdf-delta-subscription",
		"app.kubernetes.io/instance":   subscription.Name,
		"app.kubernetes.io/managed-by": "fuseki-operator",
		"fuseki.apache.org/component":  "change-subscription",
		"fuseki.apache.org/server":     server.Name,
	}
	job.Annotations = map[string]string{
		subscriptionGenerationAnnotation:      strconv.FormatInt(subscription.Generation, 10),
		subscriptionStartCheckpointAnnotation: strconv.Itoa(startVersion),
		subscriptionEndCheckpointAnnotation:   strconv.Itoa(endVersion),
		subscriptionArtifactRefAnnotation:     artifactRef,
	}
	job.Spec.BackoffLimit = ptrTo(int32(0))
	job.Spec.Template.ObjectMeta.Labels = mergeStringMaps(job.Labels, map[string]string{"job-name": job.Name, "batch.kubernetes.io/job-name": job.Name})
	job.Spec.Template.ObjectMeta.Annotations = map[string]string{
		subscriptionGenerationAnnotation:      strconv.FormatInt(subscription.Generation, 10),
		subscriptionStartCheckpointAnnotation: strconv.Itoa(startVersion),
		subscriptionEndCheckpointAnnotation:   strconv.Itoa(endVersion),
		subscriptionArtifactRefAnnotation:     artifactRef,
	}
	job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	job.Spec.Template.Spec.Containers = []corev1.Container{container}
	job.Spec.Template.Spec.Volumes = transferJobVolumes(nil)
	if err := controllerutil.SetControllerReference(subscription, job, scheme); err != nil {
		return nil, "", err
	}
	if err := c.Create(ctx, job); err != nil {
		return nil, "", err
	}
	return job, artifactRef, nil
}

func deleteChangeSubscriptionJob(ctx context.Context, c client.Client, namespace, name string) error {
	if name == "" {
		return nil
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return client.IgnoreNotFound(c.Delete(ctx, job, client.GracePeriodSeconds(0)))
}

func subscriptionJobProgress(job *batchv1.Job, subscriptionName string) (string, string, metav1.ConditionStatus, string, string) {
	startCheckpoint := job.Annotations[subscriptionStartCheckpointAnnotation]
	endCheckpoint := job.Annotations[subscriptionEndCheckpointAnnotation]
	artifactRef := job.Annotations[subscriptionArtifactRefAnnotation]
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			message := fmt.Sprintf("ChangeSubscription %q delivered checkpoint %s to %s.", subscriptionName, endCheckpoint, artifactRef)
			return "Ready", endCheckpoint, metav1.ConditionTrue, "SubscriptionDelivered", message
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			message := condition.Message
			if message == "" {
				message = fmt.Sprintf("ChangeSubscription %q failed delivering checkpoints %s-%s to %s.", subscriptionName, startCheckpoint, endCheckpoint, artifactRef)
			}
			return "Failed", "", metav1.ConditionFalse, "SubscriptionFailed", message
		}
	}

	if job.Status.StartTime != nil || job.Status.Active > 0 {
		return "Running", "", metav1.ConditionFalse, "SubscriptionRunning", fmt.Sprintf("ChangeSubscription %q is delivering checkpoints %s-%s to %s.", subscriptionName, startCheckpoint, endCheckpoint, artifactRef)
	}
	return "Running", "", metav1.ConditionFalse, "SubscriptionPending", fmt.Sprintf("ChangeSubscription %q is waiting to deliver checkpoints %s-%s to %s.", subscriptionName, startCheckpoint, endCheckpoint, artifactRef)
}

func fetchRDFDeltaLogVersion(ctx context.Context, baseURL, logName string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/"+logName+"/version", nil)
	if err != nil {
		return 0, err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, fmt.Errorf("rdf-delta version request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return 0, err
	}
	version, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil {
		return 0, fmt.Errorf("parse rdf-delta version: %w", err)
	}
	return version, nil
}

func changeSubscriptionScript() string {
	return strings.ReplaceAll(`set -euo pipefail
workspace="__TRANSFER_WORKSPACE__"
mkdir -p "${workspace}"
temp_path="${workspace}/patches.rdfpatch"
rm -f "${temp_path}" "${temp_path}.gz"

download_mc() {
	export TRANSFER_MC_BIN=/tmp/mc
	if [ -x "${TRANSFER_MC_BIN}" ]; then return 0; fi
	case "$(uname -m)" in
		x86_64|amd64) mc_arch='linux-amd64' ;;
		aarch64|arm64) mc_arch='linux-arm64' ;;
		*) echo 'unsupported architecture for mc download' >&2; exit 1 ;;
	esac
	wget -qO "${TRANSFER_MC_BIN}" "https://dl.min.io/client/mc/release/${mc_arch}/mc"
	chmod 700 "${TRANSFER_MC_BIN}"
}

setup_s3_client() {
	download_mc
	TRANSFER_MC_INSECURE=''
	if [ "${TRANSFER_S3_INSECURE:-false}" = 'true' ]; then TRANSFER_MC_INSECURE='--insecure'; fi
	endpoint="${TRANSFER_S3_ENDPOINT:-https://s3.amazonaws.com}"
	if [ -n "${TRANSFER_MC_INSECURE}" ]; then
		"${TRANSFER_MC_BIN}" "${TRANSFER_MC_INSECURE}" alias set transfer "${endpoint}" "${TRANSFER_S3_ACCESS_KEY}" "${TRANSFER_S3_SECRET_KEY}" >/dev/null
	else
		"${TRANSFER_MC_BIN}" alias set transfer "${endpoint}" "${TRANSFER_S3_ACCESS_KEY}" "${TRANSFER_S3_SECRET_KEY}" >/dev/null
	fi
}

version="${SUBSCRIPTION_START_VERSION}"
while [ "${version}" -le "${SUBSCRIPTION_END_VERSION}" ]; do
	wget -qO- "${RDF_DELTA_BASE_URL}/${RDF_DELTA_LOG_NAME}/patch/${version}" >> "${temp_path}"
	printf '\n' >> "${temp_path}"
	version=$((version + 1))
done

if [ "${TRANSFER_SINK_COMPRESSION:-}" = 'gzip' ]; then
	gzip -f "${temp_path}"
	temp_path="${temp_path}.gz"
fi

case "${TRANSFER_SINK_TYPE}" in
	Filesystem)
		mkdir -p "$(dirname "${TRANSFER_ARTIFACT_REF}")"
		cp "${temp_path}" "${TRANSFER_ARTIFACT_REF}"
		;;
	S3)
		setup_s3_client
		destination="transfer/${TRANSFER_S3_BUCKET}"
		if [ -n "${TRANSFER_S3_OBJECT:-}" ]; then destination="${destination}/${TRANSFER_S3_OBJECT}"; fi
		if [ -n "${TRANSFER_MC_INSECURE:-}" ]; then
			"${TRANSFER_MC_BIN}" "${TRANSFER_MC_INSECURE}" cp "${temp_path}" "${destination}"
		else
			"${TRANSFER_MC_BIN}" cp "${temp_path}" "${destination}"
		fi
		;;
	*)
		echo "unsupported sink type: ${TRANSFER_SINK_TYPE}" >&2
		exit 1
		;;
	esac
`, "__TRANSFER_WORKSPACE__", transferWorkspaceMountPath)
}
