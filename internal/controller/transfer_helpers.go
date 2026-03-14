package controller

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

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
	importCompletedConditionType   = "ImportCompleted"
	exportCompletedConditionType   = "ExportCompleted"
	transferRequestRequeueInterval = securityProfileRequeueInterval
	transferWorkspaceVolumeName    = "transfer-workspace"
	transferWorkspaceMountPath     = "/transfer-workspace"
	transferS3AccessKeyKey         = backupPolicyAccessKeyKey
	transferS3SecretKeyKey         = backupPolicySecretKeyKey
	transferS3EndpointKey          = "endpoint"
	transferS3InsecureKey          = "insecure"
	transferURLUsernameKey         = "username"
	transferURLPasswordKey         = "password"
	transferURLAuthHeaderKey       = "authorizationHeader"
)

type transferDirection string

const (
	transferDirectionImport transferDirection = "import"
	transferDirectionExport transferDirection = "export"
)

type resolvedTransferTarget struct {
	Dataset         *fusekiv1alpha1.Dataset
	Workload        resolvedTransferWorkload
	SecurityProfile *fusekiv1alpha1.SecurityProfile
	AdminSecretRef  *corev1.LocalObjectReference
	Status          metav1.ConditionStatus
	Reason          string
	Message         string
}

type resolvedTransferWorkload struct {
	Kind               string
	Name               string
	DatasetRefs        []corev1.LocalObjectReference
	Image              string
	ImagePullPolicy    corev1.PullPolicy
	ServiceURL         string
	SecurityProfileRef *corev1.LocalObjectReference
}

type transferSecretStatus struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

func resolveTransferTarget(ctx context.Context, c client.Client, namespace string, target fusekiv1alpha1.DatasetAccessTarget, direction transferDirection) (resolvedTransferTarget, error) {
	if target.DatasetRef.Name == "" {
		return resolvedTransferTarget{
			Status:  metav1.ConditionFalse,
			Reason:  "DatasetNotConfigured",
			Message: "spec.target.datasetRef.name is required.",
		}, nil
	}

	var dataset fusekiv1alpha1.Dataset
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: target.DatasetRef.Name}, &dataset); err != nil {
		if apierrors.IsNotFound(err) {
			return resolvedTransferTarget{
				Status:  metav1.ConditionFalse,
				Reason:  "DatasetNotFound",
				Message: fmt.Sprintf("Waiting for Dataset %q.", target.DatasetRef.Name),
			}, nil
		}
		return resolvedTransferTarget{}, err
	}

	candidates, err := listResolvedTransferWorkloads(ctx, c, namespace, dataset.Name, direction)
	if err != nil {
		return resolvedTransferTarget{}, err
	}
	if len(candidates) == 0 {
		return resolvedTransferTarget{
			Dataset: &dataset,
			Status:  metav1.ConditionFalse,
			Reason:  "TransferTargetUnavailable",
			Message: fmt.Sprintf("Waiting for a FusekiServer or FusekiCluster to reference Dataset %q.", dataset.Name),
		}, nil
	}
	if len(candidates) > 1 {
		return resolvedTransferTarget{
			Dataset: &dataset,
			Status:  metav1.ConditionFalse,
			Reason:  "DatasetTargetAmbiguous",
			Message: fmt.Sprintf("Dataset %q is referenced by multiple Fuseki workloads: %s.", dataset.Name, joinWithCommaSpace(transferWorkloadNames(candidates))),
		}, nil
	}

	candidate := candidates[0]
	securityStatus, err := resolveFusekiWorkloadSecurityDependency(ctx, c, namespace, candidate.SecurityProfileRef, candidate.DatasetRefs)
	if err != nil {
		return resolvedTransferTarget{}, err
	}
	if !workloadSecurityReady(securityStatus) {
		return resolvedTransferTarget{
			Dataset:         &dataset,
			Workload:        candidate,
			SecurityProfile: securityStatus.Profile,
			AdminSecretRef:  securityStatus.AdminSecretRef,
			Status:          securityStatus.Status,
			Reason:          securityStatus.Reason,
			Message:         securityStatus.Message,
		}, nil
	}

	return resolvedTransferTarget{
		Dataset:         &dataset,
		Workload:        candidate,
		SecurityProfile: securityStatus.Profile,
		AdminSecretRef:  securityStatus.AdminSecretRef,
		Status:          metav1.ConditionTrue,
		Reason:          "TransferTargetResolved",
		Message:         fmt.Sprintf("%s %q is resolved for Dataset %q.", candidate.Kind, candidate.Name, dataset.Name),
	}, nil
}

func listResolvedTransferWorkloads(ctx context.Context, c client.Client, namespace, datasetName string, direction transferDirection) ([]resolvedTransferWorkload, error) {
	workloads := make([]resolvedTransferWorkload, 0)

	var servers fusekiv1alpha1.FusekiServerList
	if err := c.List(ctx, &servers, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for i := range servers.Items {
		server := &servers.Items[i]
		if !containsLocalObjectReference(server.Spec.DatasetRefs, datasetName) {
			continue
		}
		workloads = append(workloads, resolvedTransferWorkload{
			Kind:               "FusekiServer",
			Name:               server.Name,
			DatasetRefs:        append([]corev1.LocalObjectReference(nil), server.Spec.DatasetRefs...),
			Image:              server.Spec.Image,
			ImagePullPolicy:    server.DesiredImagePullPolicy(),
			ServiceURL:         fmt.Sprintf("http://%s:%d", server.ServiceName(), server.DesiredHTTPPort()),
			SecurityProfileRef: server.Spec.SecurityProfileRef,
		})
	}

	var clusters fusekiv1alpha1.FusekiClusterList
	if err := c.List(ctx, &clusters, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if !containsLocalObjectReference(cluster.Spec.DatasetRefs, datasetName) {
			continue
		}
		serviceName := cluster.WriteServiceName()
		if direction == transferDirectionExport {
			serviceName = cluster.ReadServiceName()
		}
		workloads = append(workloads, resolvedTransferWorkload{
			Kind:               "FusekiCluster",
			Name:               cluster.Name,
			DatasetRefs:        append([]corev1.LocalObjectReference(nil), cluster.Spec.DatasetRefs...),
			Image:              cluster.Spec.Image,
			ImagePullPolicy:    cluster.DesiredImagePullPolicy(),
			ServiceURL:         fmt.Sprintf("http://%s:%d", serviceName, cluster.DesiredHTTPPort()),
			SecurityProfileRef: cluster.Spec.SecurityProfileRef,
		})
	}

	sort.Slice(workloads, func(i, j int) bool {
		if workloads[i].Kind == workloads[j].Kind {
			return workloads[i].Name < workloads[j].Name
		}
		return workloads[i].Kind < workloads[j].Kind
	})

	return workloads, nil
}

func transferWorkloadNames(workloads []resolvedTransferWorkload) []string {
	names := make([]string, 0, len(workloads))
	for _, workload := range workloads {
		names = append(names, workload.Kind+"/"+workload.Name)
	}
	return names
}

func resolveTransferSecretDependency(ctx context.Context, c client.Client, namespace string, ref *corev1.LocalObjectReference, fieldPath string, requiredKeys []string) (transferSecretStatus, error) {
	if ref == nil || ref.Name == "" {
		if len(requiredKeys) > 0 {
			return transferSecretStatus{
				Status:  metav1.ConditionFalse,
				Reason:  "TransferSecretNotConfigured",
				Message: fmt.Sprintf("%s is required.", fieldPath),
			}, nil
		}
		return transferSecretStatus{Status: metav1.ConditionTrue, Reason: "TransferSecretNotConfigured", Message: "No transfer Secret is referenced."}, nil
	}

	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return transferSecretStatus{
				Status:  metav1.ConditionFalse,
				Reason:  "TransferSecretNotFound",
				Message: fmt.Sprintf("Waiting for Secret %q referenced by %s.", ref.Name, fieldPath),
			}, nil
		}
		return transferSecretStatus{}, err
	}

	missingKeys := make([]string, 0)
	for _, key := range requiredKeys {
		if _, ok := secret.Data[key]; !ok {
			missingKeys = append(missingKeys, key)
		}
	}
	if len(missingKeys) > 0 {
		return transferSecretStatus{
			Status:  metav1.ConditionFalse,
			Reason:  "TransferSecretInvalid",
			Message: fmt.Sprintf("Secret %q referenced by %s is missing required keys: %s.", ref.Name, fieldPath, strings.Join(missingKeys, ", ")),
		}, nil
	}

	return transferSecretStatus{Status: metav1.ConditionTrue, Reason: "TransferSecretReady", Message: fmt.Sprintf("Secret %q referenced by %s is ready.", ref.Name, fieldPath)}, nil
}

func requiredSourceSecretKeys(source fusekiv1alpha1.DataSourceSpec) []string {
	if source.Type == fusekiv1alpha1.DataSourceTypeS3 {
		return []string{transferS3AccessKeyKey, transferS3SecretKeyKey}
	}
	return nil
}

func requiredSinkSecretKeys(sink fusekiv1alpha1.DataSinkSpec) []string {
	if sink.Type == fusekiv1alpha1.DataSinkTypeS3 {
		return []string{transferS3AccessKeyKey, transferS3SecretKeyKey}
	}
	return nil
}

func containsLocalObjectReference(refs []corev1.LocalObjectReference, name string) bool {
	for _, ref := range refs {
		if ref.Name == name {
			return true
		}
	}
	return false
}

func transferDataURL(baseURL, datasetName, namedGraph string) string {
	transferURL := fmt.Sprintf("%s/%s/data", baseURL, datasetName)
	if namedGraph == "" {
		return transferURL
	}
	return transferURL + "?graph=" + url.QueryEscape(namedGraph)
}

func importRequestContainer(request *fusekiv1alpha1.ImportRequest, target resolvedTransferTarget) (corev1.Container, error) {
	env, err := transferBaseEnvVars(target, transferDirectionImport)
	if err != nil {
		return corev1.Container{}, err
	}
	env = append(env,
		corev1.EnvVar{Name: "DATASET_NAME", Value: target.Dataset.Spec.Name},
		corev1.EnvVar{Name: "DATASET_DB_TYPE", Value: strings.ToLower(string(target.Dataset.DesiredType()))},
		corev1.EnvVar{Name: "FUSEKI_IMPORT_URL", Value: transferDataURL(target.Workload.ServiceURL, target.Dataset.Spec.Name, request.Spec.Target.NamedGraph)},
		corev1.EnvVar{Name: "TRANSFER_SOURCE_TYPE", Value: string(request.Spec.Source.Type)},
	)
	if request.Spec.Source.URI != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SOURCE_URI", Value: request.Spec.Source.URI})
	}
	if request.Spec.Source.Path != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SOURCE_PATH", Value: request.Spec.Source.Path})
	}
	if request.Spec.Source.Format != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SOURCE_MEDIA_TYPE", Value: request.Spec.Source.Format})
	}
	env = append(env, corev1.EnvVar{Name: "TRANSFER_SOURCE_BASENAME", Value: transferSourceBaseName(request.Spec.Source)})
	env, err = appendTransferSourceEnvVars(env, request.Spec.Source)
	if err != nil {
		return corev1.Container{}, err
	}

	return corev1.Container{
		Name:            "import",
		Image:           target.Workload.Image,
		ImagePullPolicy: target.Workload.ImagePullPolicy,
		Command:         []string{"/bin/bash", "-ceu", importRequestScript()},
		Env:             env,
		VolumeMounts:    transferVolumeMounts(target.SecurityProfile),
	}, nil
}

func exportRequestContainer(request *fusekiv1alpha1.ExportRequest, target resolvedTransferTarget) (corev1.Container, string, error) {
	artifactRef := resolveExportArtifactRef(request)
	env, err := transferBaseEnvVars(target, transferDirectionExport)
	if err != nil {
		return corev1.Container{}, "", err
	}
	env = append(env,
		corev1.EnvVar{Name: "DATASET_NAME", Value: target.Dataset.Spec.Name},
		corev1.EnvVar{Name: "FUSEKI_EXPORT_URL", Value: transferDataURL(target.Workload.ServiceURL, target.Dataset.Spec.Name, request.Spec.Target.NamedGraph)},
		corev1.EnvVar{Name: "TRANSFER_SINK_TYPE", Value: string(request.Spec.Sink.Type)},
		corev1.EnvVar{Name: "TRANSFER_SINK_MEDIA_TYPE", Value: desiredExportMediaType(request)},
		corev1.EnvVar{Name: "TRANSFER_ARTIFACT_REF", Value: artifactRef},
		corev1.EnvVar{Name: "TRANSFER_OUTPUT_PATH", Value: path.Join(transferWorkspaceMountPath, path.Base(artifactRef))},
	)
	if request.Spec.Sink.URI != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SINK_URI", Value: request.Spec.Sink.URI})
	}
	if request.Spec.Sink.Path != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SINK_PATH", Value: request.Spec.Sink.Path})
	}
	if request.Spec.Sink.Compression != "" {
		env = append(env, corev1.EnvVar{Name: "TRANSFER_SINK_COMPRESSION", Value: request.Spec.Sink.Compression})
	}
	env, err = appendTransferSinkEnvVars(env, request.Spec.Sink, artifactRef)
	if err != nil {
		return corev1.Container{}, "", err
	}

	return corev1.Container{
		Name:            "export",
		Image:           target.Workload.Image,
		ImagePullPolicy: target.Workload.ImagePullPolicy,
		Command:         []string{"/bin/bash", "-ceu", exportRequestScript()},
		Env:             env,
		VolumeMounts:    transferVolumeMounts(target.SecurityProfile),
	}, artifactRef, nil
}

func transferBaseEnvVars(target resolvedTransferTarget, direction transferDirection) ([]corev1.EnvVar, error) {
	baseURL := target.Workload.ServiceURL
	if direction == transferDirectionImport {
		baseURL = secureServiceURL(baseURL, target.SecurityProfile)
	}
	env := []corev1.EnvVar{{Name: "FUSEKI_BASE_URL", Value: baseURL}}
	env = append(env, fusekiSecurityEnvVars(target.SecurityProfile)...)
	if target.AdminSecretRef != nil {
		env = append(env,
			corev1.EnvVar{Name: "FUSEKI_ADMIN_USER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *target.AdminSecretRef, Key: "username", Optional: ptrTo(true)}}},
			corev1.EnvVar{Name: "FUSEKI_ADMIN_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *target.AdminSecretRef, Key: "password", Optional: ptrTo(true)}}},
		)
	}
	return env, nil
}

func appendTransferSourceEnvVars(env []corev1.EnvVar, source fusekiv1alpha1.DataSourceSpec) ([]corev1.EnvVar, error) {
	if source.SecretRef != nil && source.SecretRef.Name != "" {
		optional := true
		switch source.Type {
		case fusekiv1alpha1.DataSourceTypeS3:
			bucket, objectPath, err := parseS3URI(source.URI)
			if err != nil {
				return nil, err
			}
			env = append(env,
				corev1.EnvVar{Name: "TRANSFER_S3_BUCKET", Value: bucket},
				corev1.EnvVar{Name: "TRANSFER_S3_OBJECT", Value: objectPath},
				corev1.EnvVar{Name: "TRANSFER_S3_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *source.SecretRef, Key: transferS3AccessKeyKey}}},
				corev1.EnvVar{Name: "TRANSFER_S3_SECRET_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *source.SecretRef, Key: transferS3SecretKeyKey}}},
				corev1.EnvVar{Name: "TRANSFER_S3_ENDPOINT", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *source.SecretRef, Key: transferS3EndpointKey, Optional: &optional}}},
				corev1.EnvVar{Name: "TRANSFER_S3_INSECURE", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *source.SecretRef, Key: transferS3InsecureKey, Optional: &optional}}},
			)
		case fusekiv1alpha1.DataSourceTypeURL:
			env = append(env,
				corev1.EnvVar{Name: "TRANSFER_SOURCE_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *source.SecretRef, Key: transferURLUsernameKey, Optional: &optional}}},
				corev1.EnvVar{Name: "TRANSFER_SOURCE_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *source.SecretRef, Key: transferURLPasswordKey, Optional: &optional}}},
				corev1.EnvVar{Name: "TRANSFER_SOURCE_AUTH_HEADER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *source.SecretRef, Key: transferURLAuthHeaderKey, Optional: &optional}}},
			)
		}
	} else if source.Type == fusekiv1alpha1.DataSourceTypeS3 {
		bucket, objectPath, err := parseS3URI(source.URI)
		if err != nil {
			return nil, err
		}
		env = append(env,
			corev1.EnvVar{Name: "TRANSFER_S3_BUCKET", Value: bucket},
			corev1.EnvVar{Name: "TRANSFER_S3_OBJECT", Value: objectPath},
		)
	}
	return env, nil
}

func appendTransferSinkEnvVars(env []corev1.EnvVar, sink fusekiv1alpha1.DataSinkSpec, artifactRef string) ([]corev1.EnvVar, error) {
	if sink.Type == fusekiv1alpha1.DataSinkTypeS3 {
		bucket, objectPath, err := parseS3URI(artifactRef)
		if err != nil {
			return nil, err
		}
		env = append(env,
			corev1.EnvVar{Name: "TRANSFER_S3_BUCKET", Value: bucket},
			corev1.EnvVar{Name: "TRANSFER_S3_OBJECT", Value: objectPath},
		)
	}
	if sink.SecretRef != nil && sink.SecretRef.Name != "" {
		optional := true
		if sink.Type == fusekiv1alpha1.DataSinkTypeS3 {
			env = append(env,
				corev1.EnvVar{Name: "TRANSFER_S3_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *sink.SecretRef, Key: transferS3AccessKeyKey}}},
				corev1.EnvVar{Name: "TRANSFER_S3_SECRET_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *sink.SecretRef, Key: transferS3SecretKeyKey}}},
				corev1.EnvVar{Name: "TRANSFER_S3_ENDPOINT", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *sink.SecretRef, Key: transferS3EndpointKey, Optional: &optional}}},
				corev1.EnvVar{Name: "TRANSFER_S3_INSECURE", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *sink.SecretRef, Key: transferS3InsecureKey, Optional: &optional}}},
			)
		}
	}
	return env, nil
}

func transferJobVolumes(securityProfile *fusekiv1alpha1.SecurityProfile) []corev1.Volume {
	volumes := []corev1.Volume{{Name: transferWorkspaceVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	if securityVolume := fusekiSecurityConfigVolume(securityProfile); securityVolume != nil {
		volumes = append(volumes, *securityVolume)
	}
	if tlsVolume := fusekiSecurityTLSVolume(securityProfile); tlsVolume != nil {
		volumes = append(volumes, *tlsVolume)
	}
	return volumes
}

func transferVolumeMounts(securityProfile *fusekiv1alpha1.SecurityProfile) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{{Name: transferWorkspaceVolumeName, MountPath: transferWorkspaceMountPath}}
	if securityMount := fusekiSecurityConfigVolumeMount(securityProfile); securityMount != nil {
		mounts = append(mounts, *securityMount)
	}
	if tlsMount := fusekiSecurityTLSVolumeMount(securityProfile); tlsMount != nil {
		mounts = append(mounts, *tlsMount)
	}
	return mounts
}

func reconcileImportJob(ctx context.Context, c client.Client, scheme *runtime.Scheme, request *fusekiv1alpha1.ImportRequest, target resolvedTransferTarget) (*batchv1.Job, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: request.JobName(), Namespace: request.Namespace}}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), job); err == nil {
		return job, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	container, err := importRequestContainer(request, target)
	if err != nil {
		return nil, err
	}
	job.Labels = transferRequestLabels("import", request.Name, target.Dataset.Name, target.Workload)
	job.Spec.BackoffLimit = ptrTo(int32(0))
	job.Spec.Template.ObjectMeta.Labels = mergeStringMaps(job.Labels, map[string]string{"job-name": job.Name, "batch.kubernetes.io/job-name": job.Name})
	job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	job.Spec.Template.Spec.Containers = []corev1.Container{container}
	job.Spec.Template.Spec.Volumes = transferJobVolumes(target.SecurityProfile)
	if err := controllerutil.SetControllerReference(request, job, scheme); err != nil {
		return nil, err
	}
	if err := c.Create(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func reconcileExportJob(ctx context.Context, c client.Client, scheme *runtime.Scheme, request *fusekiv1alpha1.ExportRequest, target resolvedTransferTarget) (*batchv1.Job, string, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: request.JobName(), Namespace: request.Namespace}}
	artifactRef := resolveExportArtifactRef(request)
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), job); err == nil {
		return job, artifactRef, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, "", err
	}

	container, resolvedArtifactRef, err := exportRequestContainer(request, target)
	if err != nil {
		return nil, "", err
	}
	job.Labels = transferRequestLabels("export", request.Name, target.Dataset.Name, target.Workload)
	job.Spec.BackoffLimit = ptrTo(int32(0))
	job.Spec.Template.ObjectMeta.Labels = mergeStringMaps(job.Labels, map[string]string{"job-name": job.Name, "batch.kubernetes.io/job-name": job.Name})
	job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	job.Spec.Template.Spec.Containers = []corev1.Container{container}
	job.Spec.Template.Spec.Volumes = transferJobVolumes(target.SecurityProfile)
	if err := controllerutil.SetControllerReference(request, job, scheme); err != nil {
		return nil, "", err
	}
	if err := c.Create(ctx, job); err != nil {
		return nil, "", err
	}
	return job, resolvedArtifactRef, nil
}

func transferRequestLabels(operation, requestName, datasetName string, workload resolvedTransferWorkload) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":          "fuseki-transfer",
		"app.kubernetes.io/instance":      requestName,
		"app.kubernetes.io/managed-by":    "fuseki-operator",
		"fuseki.apache.org/component":     operation,
		"fuseki.apache.org/request":       requestName,
		"fuseki.apache.org/dataset":       datasetName,
		"fuseki.apache.org/workload-kind": strings.ToLower(workload.Kind),
		"fuseki.apache.org/workload":      workload.Name,
	}
}

func transferJobProgress(job *batchv1.Job, jobName, prefix string) (string, metav1.ConditionStatus, string, string) {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return "Succeeded", metav1.ConditionTrue, prefix + "Completed", fmt.Sprintf("%s job %q completed successfully.", prefix, jobName)
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			message := condition.Message
			if message == "" {
				message = fmt.Sprintf("%s job %q failed.", prefix, jobName)
			}
			return "Failed", metav1.ConditionFalse, prefix + "Failed", message
		}
	}

	if job.Status.Active > 0 || job.Status.StartTime != nil {
		return "Running", metav1.ConditionFalse, prefix + "Running", fmt.Sprintf("%s job %q is running.", prefix, jobName)
	}

	return "Running", metav1.ConditionFalse, prefix + "Pending", fmt.Sprintf("%s job %q is pending.", prefix, jobName)
}

func transferRequestPhaseTerminal(phase string) bool {
	return phase == "Succeeded" || phase == "Failed"
}

func parseS3URI(raw string) (string, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", err
	}
	if parsed.Scheme != "s3" || parsed.Host == "" {
		return "", "", fmt.Errorf("invalid S3 URI %q", raw)
	}
	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}

func transferSourceBaseName(source fusekiv1alpha1.DataSourceSpec) string {
	switch source.Type {
	case fusekiv1alpha1.DataSourceTypeFilesystem:
		if source.Path != "" {
			return path.Base(source.Path)
		}
	case fusekiv1alpha1.DataSourceTypeURL, fusekiv1alpha1.DataSourceTypeS3:
		if source.URI != "" {
			if parsed, err := url.Parse(source.URI); err == nil {
				if base := path.Base(parsed.Path); base != "." && base != "/" && base != "" {
					return base
				}
			}
		}
	}
	return "source.rdf"
}

func desiredExportMediaType(request *fusekiv1alpha1.ExportRequest) string {
	if request.Spec.Sink.Format != "" {
		return request.Spec.Sink.Format
	}
	if request.Spec.Target.NamedGraph != "" {
		return "text/turtle"
	}
	return "application/n-quads"
}

func resolveExportArtifactRef(request *fusekiv1alpha1.ExportRequest) string {
	artifactName := request.Name + transferFileExtension(desiredExportMediaType(request))
	if strings.EqualFold(request.Spec.Sink.Compression, "gzip") && !strings.HasSuffix(artifactName, ".gz") {
		artifactName += ".gz"
	}
	if request.Spec.Sink.Type == fusekiv1alpha1.DataSinkTypeFilesystem {
		if strings.HasSuffix(request.Spec.Sink.Path, "/") {
			return strings.TrimSuffix(request.Spec.Sink.Path, "/") + "/" + artifactName
		}
		return request.Spec.Sink.Path
	}
	if strings.HasSuffix(request.Spec.Sink.URI, "/") {
		return strings.TrimSuffix(request.Spec.Sink.URI, "/") + "/" + artifactName
	}
	return request.Spec.Sink.URI
}

func transferFileExtension(mediaType string) string {
	switch strings.ToLower(mediaType) {
	case "text/turtle", "application/x-turtle":
		return ".ttl"
	case "application/n-triples":
		return ".nt"
	case "application/n-quads":
		return ".nq"
	case "application/trig":
		return ".trig"
	case "application/rdf+xml", "application/xml", "text/xml":
		return ".rdf"
	default:
		return ".rdf"
	}
}

func importRequestScript() string {
	script := transferScriptPrelude() + `
stage_dir='__TRANSFER_WORKSPACE__/source'
mkdir -p "${stage_dir}"
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
if [[ -z "${first_file}" ]]; then echo 'no import files were staged' >&2; exit 1; fi
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
  if [[ "${file}" == *.gz ]]; then
    gzip -cd "${file}" | curl_fuseki --silent --show-error --fail "${auth_args[@]}" -X POST -H "Content-Type: ${content_type}" --data-binary @- "${FUSEKI_IMPORT_URL}"
  else
    curl_fuseki --silent --show-error --fail "${auth_args[@]}" -X POST -H "Content-Type: ${content_type}" --data-binary "@${file}" "${FUSEKI_IMPORT_URL}"
  fi
done < <(find "${stage_dir}" -type f | sort)
`
	return strings.ReplaceAll(script, "__TRANSFER_WORKSPACE__", transferWorkspaceMountPath)
}

func exportRequestScript() string {
	return transferScriptPrelude() + `
wait_for_fuseki
auth_args=()
if [[ -n "${FUSEKI_ADMIN_USER:-}" ]] && [[ -n "${FUSEKI_ADMIN_PASSWORD:-}" ]]; then auth_args=(-u "${FUSEKI_ADMIN_USER}:${FUSEKI_ADMIN_PASSWORD}"); fi
output_path="${TRANSFER_OUTPUT_PATH}"
temp_path="${output_path}"
mkdir -p "$(dirname "${output_path}")"
if [[ "${TRANSFER_SINK_COMPRESSION:-}" == 'gzip' ]]; then temp_path="${output_path%.gz}"; fi
curl_fuseki --silent --show-error --fail "${auth_args[@]}" -H "Accept: ${TRANSFER_SINK_MEDIA_TYPE}" "${FUSEKI_EXPORT_URL}" -o "${temp_path}"
if [[ "${TRANSFER_SINK_COMPRESSION:-}" == 'gzip' ]]; then gzip -f "${temp_path}"; fi
case "${TRANSFER_SINK_TYPE}" in
  Filesystem)
    mkdir -p "$(dirname "${TRANSFER_ARTIFACT_REF}")"
    cp "${output_path}" "${TRANSFER_ARTIFACT_REF}"
    ;;
  S3)
    setup_s3_client
    destination="transfer/${TRANSFER_S3_BUCKET}"
    if [[ -n "${TRANSFER_S3_OBJECT:-}" ]]; then destination="${destination}/${TRANSFER_S3_OBJECT}"; fi
    "${TRANSFER_MC_BIN}" "${TRANSFER_MC_FLAGS[@]}" cp "${output_path}" "${destination}"
    ;;
  *)
    echo "unsupported sink type: ${TRANSFER_SINK_TYPE}" >&2
    exit 1
    ;;
esac
`
}

func transferScriptPrelude() string {
	return `set -euo pipefail
curl_fuseki() {
	if [[ -n "${SECURITY_PROFILE_TLS_CA_FILE:-}" ]] && [[ -f "${SECURITY_PROFILE_TLS_CA_FILE}" ]]; then curl --cacert "${SECURITY_PROFILE_TLS_CA_FILE}" "$@"; return; fi
	if [[ -n "${SECURITY_PROFILE_TLS_CERT_FILE:-}" ]] && [[ -f "${SECURITY_PROFILE_TLS_CERT_FILE}" ]]; then curl --cacert "${SECURITY_PROFILE_TLS_CERT_FILE}" "$@"; return; fi
	if [[ "${FUSEKI_BASE_URL#https://}" != "${FUSEKI_BASE_URL}" ]]; then curl -k "$@"; return; fi
	curl "$@"
}
wait_for_fuseki() {
	for attempt in $(seq 1 60); do
		if curl_fuseki --silent --fail "${FUSEKI_BASE_URL}/$/ping" >/dev/null 2>&1; then return 0; fi
		if [[ "${attempt}" -eq 60 ]]; then echo 'Fuseki service did not become ready in time' >&2; exit 1; fi
		sleep 2
	done
}
content_type_for_file() {
	case "$1" in
		*.ttl|*.ttl.gz) echo 'text/turtle' ;;
		*.nt|*.nt.gz) echo 'application/n-triples' ;;
		*.nq|*.nq.gz) echo 'application/n-quads' ;;
		*.trig|*.trig.gz) echo 'application/trig' ;;
		*.rdf|*.rdf.gz|*.xml|*.xml.gz) echo 'application/rdf+xml' ;;
		*) echo 'application/n-quads' ;;
	esac
}
download_mc() {
	export TRANSFER_MC_BIN=/tmp/mc
	if [[ -x "${TRANSFER_MC_BIN}" ]]; then return 0; fi
	case "$(uname -m)" in
		x86_64|amd64) mc_arch='linux-amd64' ;;
		aarch64|arm64) mc_arch='linux-arm64' ;;
		*) echo 'unsupported architecture for mc download' >&2; exit 1 ;;
	esac
	curl --silent --show-error --fail -L "https://dl.min.io/client/mc/release/${mc_arch}/mc" -o "${TRANSFER_MC_BIN}"
	chmod 700 "${TRANSFER_MC_BIN}"
}
setup_s3_client() {
	download_mc
	TRANSFER_MC_FLAGS=()
	if [[ "${TRANSFER_S3_INSECURE:-false}" == 'true' ]]; then TRANSFER_MC_FLAGS+=(--insecure); fi
	local endpoint="${TRANSFER_S3_ENDPOINT:-https://s3.amazonaws.com}"
	"${TRANSFER_MC_BIN}" "${TRANSFER_MC_FLAGS[@]}" alias set transfer "${endpoint}" "${TRANSFER_S3_ACCESS_KEY}" "${TRANSFER_S3_SECRET_KEY}" >/dev/null
}
`
}
