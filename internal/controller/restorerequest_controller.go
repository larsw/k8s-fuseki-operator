package controller

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

const restoreCompletedConditionType = "RestoreCompleted"

type RestoreRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=restorerequests,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=restorerequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=rdfdeltaservers;backuppolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch

func (r *RestoreRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var request fusekiv1alpha1.RestoreRequest
	if err := r.Get(ctx, req.NamespacedName, &request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if restoreRequestPhaseTerminal(request.Status.Phase) && request.Status.ObservedGeneration >= request.Generation {
		return ctrl.Result{}, nil
	}

	target, err := r.resolveTarget(ctx, &request)
	if err != nil {
		return ctrl.Result{}, err
	}

	configuredStatus := target.status
	configuredReason := target.reason
	configuredMessage := target.message
	restoreStatus := metav1.ConditionFalse
	restoreReason := "WaitingForTarget"
	restoreMessage := "Waiting for restore prerequisites."
	phase := "Pending"
	jobName := request.JobName()

	if target.server != nil && target.status == metav1.ConditionTrue {
		policyRef := request.Spec.BackupPolicyRef
		if policyRef == nil {
			policyRef = target.server.Spec.BackupPolicyRef
		}
		backupStatus, err := resolveBackupPolicyDependency(ctx, r.Client, request.Namespace, policyRef)
		if err != nil {
			return ctrl.Result{}, err
		}
		configuredStatus = backupStatus.Status
		configuredReason = backupStatus.Reason
		configuredMessage = backupStatus.Message

		if backupStatus.Status == metav1.ConditionTrue && backupStatus.Policy != nil {
			activeRestore, err := resolveActiveRestoreRequest(ctx, r.Client, request.Namespace, target.server.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			if activeRestore != nil && activeRestore.Name != request.Name {
				configuredStatus = metav1.ConditionFalse
				configuredReason = "RestoreAlreadyRunning"
				configuredMessage = fmt.Sprintf("RestoreRequest %q is already active for RDFDeltaServer %q.", activeRestore.Name, target.server.Name)
				restoreReason = "RestoreQueued"
				restoreMessage = configuredMessage
			} else {
				statefulSet := &appsv1.StatefulSet{}
				if err := r.Get(ctx, client.ObjectKey{Namespace: request.Namespace, Name: target.server.StatefulSetName()}, statefulSet); err != nil {
					if apierrors.IsNotFound(err) {
						configuredStatus = metav1.ConditionFalse
						configuredReason = "TargetStatefulSetNotFound"
						configuredMessage = fmt.Sprintf("Waiting for RDFDeltaServer StatefulSet %q.", target.server.StatefulSetName())
					} else {
						return ctrl.Result{}, err
					}
				} else if !rdfDeltaServerScaledDown(statefulSet) {
					phase = "Running"
					restoreReason = "ScalingDown"
					restoreMessage = fmt.Sprintf("Waiting for RDFDeltaServer %q to scale down before restore.", target.server.Name)
				} else {
					job, err := r.reconcileRestoreJob(ctx, &request, target.server, backupStatus.Policy)
					if err != nil {
						return ctrl.Result{}, err
					}
					phase, restoreStatus, restoreReason, restoreMessage = restoreJobProgress(job, request.JobName())
				}
			}
		}
	}

	updated := request.DeepCopy()
	updated.Status.ObservedGeneration = request.Generation
	updated.Status.Phase = phase
	updated.Status.TargetName = request.Spec.TargetRef.Name
	updated.Status.JobName = jobName
	updated.Status.ResolvedBackupRef = request.DesiredResolvedBackupRef()
	if configuredStatus != metav1.ConditionTrue {
		updated.Status.Phase = "Pending"
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             configuredStatus,
		Reason:             configuredReason,
		Message:            configuredMessage,
		ObservedGeneration: request.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               restoreCompletedConditionType,
		Status:             restoreStatus,
		Reason:             restoreReason,
		Message:            restoreMessage,
		ObservedGeneration: request.Generation,
	})

	if !reflect.DeepEqual(request.Status, updated.Status) {
		request.Status = updated.Status
		if err := r.Status().Update(ctx, &request); err != nil {
			return ctrl.Result{}, err
		}
	}

	if updated.Status.Phase == "Succeeded" || updated.Status.Phase == "Failed" {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: backupPolicyRequeueInterval}, nil
}

func (r *RestoreRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.RestoreRequest{}).
		Watches(&fusekiv1alpha1.RDFDeltaServer{}, handler.EnqueueRequestsFromMapFunc(r.requestsForRDFDeltaServer)).
		Watches(&fusekiv1alpha1.BackupPolicy{}, handler.EnqueueRequestsFromMapFunc(r.requestsForBackupPolicy)).
		Owns(&batchv1.Job{}).
		Complete(r)
}

type restoreTargetResolution struct {
	server  *fusekiv1alpha1.RDFDeltaServer
	status  metav1.ConditionStatus
	reason  string
	message string
}

func (r *RestoreRequestReconciler) resolveTarget(ctx context.Context, request *fusekiv1alpha1.RestoreRequest) (restoreTargetResolution, error) {
	switch request.Spec.TargetRef.Kind {
	case fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer:
		var server fusekiv1alpha1.RDFDeltaServer
		if err := r.Get(ctx, client.ObjectKey{Namespace: request.Namespace, Name: request.Spec.TargetRef.Name}, &server); err != nil {
			if apierrors.IsNotFound(err) {
				return restoreTargetResolution{status: metav1.ConditionFalse, reason: "TargetNotFound", message: fmt.Sprintf("Waiting for RDFDeltaServer %q.", request.Spec.TargetRef.Name)}, nil
			}
			return restoreTargetResolution{}, err
		}
		return restoreTargetResolution{server: &server, status: metav1.ConditionTrue, reason: "TargetResolved", message: fmt.Sprintf("RDFDeltaServer %q is resolved.", server.Name)}, nil
	default:
		return restoreTargetResolution{status: metav1.ConditionFalse, reason: "UnsupportedTargetKind", message: fmt.Sprintf("Restore target kind %q is not supported.", request.Spec.TargetRef.Kind)}, nil
	}
}

func (r *RestoreRequestReconciler) reconcileRestoreJob(ctx context.Context, request *fusekiv1alpha1.RestoreRequest, server *fusekiv1alpha1.RDFDeltaServer, policy *fusekiv1alpha1.BackupPolicy) (*batchv1.Job, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: request.JobName(), Namespace: request.Namespace}}
	err := r.Get(ctx, client.ObjectKeyFromObject(job), job)
	if err == nil {
		return job, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	job.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{
		"fuseki.apache.org/component": "restore",
		"fuseki.apache.org/restore":   request.Name,
	})
	job.Spec.BackoffLimit = ptrTo(int32(0))
	job.Spec.Template.ObjectMeta.Labels = mergeStringMaps(job.Labels, map[string]string{
		"job-name":                     job.Name,
		"batch.kubernetes.io/job-name": job.Name,
	})
	job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	job.Spec.Template.Spec.Containers = []corev1.Container{rdfDeltaRestoreContainer(request, server, policy)}
	job.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: rdfDeltaDataVolumeName,
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: rdfDeltaBackupPersistentVolumeClaimName(server),
		}},
	}}
	if err := controllerutil.SetControllerReference(request, job, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func rdfDeltaRestoreContainer(request *fusekiv1alpha1.RestoreRequest, server *fusekiv1alpha1.RDFDeltaServer, policy *fusekiv1alpha1.BackupPolicy) corev1.Container {
	return corev1.Container{
		Name:            "restore",
		Image:           policy.DesiredBackupImage(),
		ImagePullPolicy: policy.DesiredImagePullPolicy(),
		Command:         []string{"/bin/sh", "-ec", rdfDeltaRestoreScript()},
		Env: []corev1.EnvVar{
			{Name: "RDF_DELTA_STORAGE_PATH", Value: fusekiv1alpha1.DefaultRDFDeltaDataMountPath},
			{Name: "RDF_DELTA_BACKUP_PREFIX", Value: rdfDeltaBackupObjectPrefix(server, policy)},
			{Name: "BACKUP_OBJECT", Value: request.Spec.BackupObject},
			{Name: "S3_ENDPOINT", Value: policy.Spec.S3.Endpoint},
			{Name: "S3_BUCKET", Value: policy.Spec.S3.Bucket},
			{Name: "S3_INSECURE", Value: strconv.FormatBool(policy.Spec.S3.Insecure)},
			{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: policy.Spec.S3.CredentialsSecretRef, Key: backupPolicyAccessKeyKey}}},
			{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: policy.Spec.S3.CredentialsSecretRef, Key: backupPolicySecretKeyKey}}},
		},
		Resources: policy.Spec.Job.Resources,
		VolumeMounts: []corev1.VolumeMount{{
			Name:      rdfDeltaDataVolumeName,
			MountPath: fusekiv1alpha1.DefaultRDFDeltaDataMountPath,
		}},
	}
}

func rdfDeltaRestoreScript() string {
	return `
set -eu

mc_flags=""
if [ "${S3_INSECURE:-false}" = "true" ]; then
  mc_flags="--insecure"
fi

mc ${mc_flags} alias set backup "${S3_ENDPOINT}" "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" --api S3v4 >/dev/null

object_dir="${S3_BUCKET}/${RDF_DELTA_BACKUP_PREFIX}"
selected_object="${BACKUP_OBJECT:-}"
if [ -z "${selected_object}" ]; then
	listing="$(mc ${mc_flags} ls --json "backup/${object_dir}/" || true)"
	objects=""
	while IFS= read -r line; do
	  case "${line}" in
	    *'"key":"'*)
	      object="${line#*\"key\":\"}"
	      object="${object%%\"*}"
	      object="${object%/}"
	      [ -n "${object}" ] || continue
	      objects="${objects}${object}
"
	      ;;
	  esac
	done <<EOF
${listing}
EOF
	old_ifs="${IFS}"
	IFS='
'
	for object in ${objects}; do
	  [ -n "${object}" ] || continue
	  selected_object="${object}"
	done
	IFS="${old_ifs}"
fi

selected_object="${selected_object%/}"

if [ -z "${selected_object}" ]; then
  echo "no backup object found under ${object_dir}" >&2
  exit 1
fi

mkdir -p "${RDF_DELTA_STORAGE_PATH}"
for path in "${RDF_DELTA_STORAGE_PATH}"/.[!.]* "${RDF_DELTA_STORAGE_PATH}"/..?* "${RDF_DELTA_STORAGE_PATH}"/*; do
	[ -e "${path}" ] || continue
	rm -rf "${path}"
done
mc ${mc_flags} mirror --overwrite "backup/${object_dir}/${selected_object}/" "${RDF_DELTA_STORAGE_PATH}/"
`
}

func restoreJobProgress(job *batchv1.Job, jobName string) (string, metav1.ConditionStatus, string, string) {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return "Succeeded", metav1.ConditionTrue, "RestoreCompleted", fmt.Sprintf("Restore job %q completed successfully.", jobName)
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			message := condition.Message
			if message == "" {
				message = fmt.Sprintf("Restore job %q failed.", jobName)
			}
			return "Failed", metav1.ConditionFalse, "RestoreFailed", message
		}
	}

	if job.Status.Active > 0 {
		return "Running", metav1.ConditionFalse, "RestoreRunning", fmt.Sprintf("Restore job %q is running.", jobName)
	}

	return "Running", metav1.ConditionFalse, "RestorePending", fmt.Sprintf("Restore job %q is pending.", jobName)
}

func rdfDeltaServerScaledDown(statefulSet *appsv1.StatefulSet) bool {
	return statefulSet.Spec.Replicas != nil && *statefulSet.Spec.Replicas == 0 && statefulSet.Status.ReadyReplicas == 0
}

func restoreRequestPhaseTerminal(phase string) bool {
	return phase == "Succeeded" || phase == "Failed"
}

func (r *RestoreRequestReconciler) requestsForRDFDeltaServer(ctx context.Context, obj client.Object) []reconcile.Request {
	var requests fusekiv1alpha1.RestoreRequestList
	if err := r.List(ctx, &requests, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	result := make([]reconcile.Request, 0)
	for i := range requests.Items {
		request := &requests.Items[i]
		if restoreRequestPhaseTerminal(request.Status.Phase) {
			continue
		}
		if request.Spec.TargetRef.Kind != fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer || request.Spec.TargetRef.Name != obj.GetName() {
			continue
		}
		result = append(result, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)})
	}

	return result
}

func (r *RestoreRequestReconciler) requestsForBackupPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	var requests fusekiv1alpha1.RestoreRequestList
	if err := r.List(ctx, &requests, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	result := make([]reconcile.Request, 0)
	for i := range requests.Items {
		request := &requests.Items[i]
		if restoreRequestPhaseTerminal(request.Status.Phase) {
			continue
		}
		if request.Spec.BackupPolicyRef != nil && request.Spec.BackupPolicyRef.Name == obj.GetName() {
			result = append(result, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)})
			continue
		}
		if request.Spec.TargetRef.Kind != fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer {
			continue
		}
		var server fusekiv1alpha1.RDFDeltaServer
		if err := r.Get(ctx, client.ObjectKey{Namespace: request.Namespace, Name: request.Spec.TargetRef.Name}, &server); err != nil {
			continue
		}
		if server.Spec.BackupPolicyRef != nil && server.Spec.BackupPolicyRef.Name == obj.GetName() {
			result = append(result, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)})
		}
	}

	return result
}
