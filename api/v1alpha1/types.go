package v1alpha1

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultFusekiReplicas        int32 = 3
	DefaultFusekiHTTPPort        int32 = 3030
	DefaultRDFDeltaReplicas      int32 = 1
	DefaultRDFDeltaServicePort   int32 = 1066
	DefaultRDFDeltaRetentionDays int32 = 7
	DefaultDatasetType                 = DatasetTypeTDB2
	DefaultStorageSize                 = "20Gi"
	DefaultFusekiDataMountPath         = "/fuseki"
	DefaultRDFDeltaDataMountPath       = "/var/lib/rdf-delta"
	DefaultMetricsPath                 = "/$/metrics"
	DefaultMetricsScrapeInterval       = 30 * time.Second
)

// +kubebuilder:validation:Enum=TDB2
type DatasetType string

const (
	DatasetTypeTDB2 DatasetType = "TDB2"
)

type FusekiClusterSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	Replicas int32 `json:"replicas,omitempty"`

	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=3030
	HTTPPort int32 `json:"httpPort,omitempty"`

	RDFDeltaServerRef corev1.LocalObjectReference   `json:"rdfDeltaServerRef"`
	DatasetRefs       []corev1.LocalObjectReference `json:"datasetRefs,omitempty"`

	SecurityProfileRef *corev1.LocalObjectReference `json:"securityProfileRef,omitempty"`
	Resources          corev1.ResourceRequirements  `json:"resources,omitempty"`
	Storage            FusekiClusterStorageSpec     `json:"storage,omitempty"`
	LeaderElection     FusekiLeaderElectionSpec     `json:"leaderElection,omitempty"`
	Services           FusekiClusterServiceSpec     `json:"services,omitempty"`
	Observability      WorkloadObservabilitySpec    `json:"observability,omitempty"`
	Autoscaling        *FusekiAutoscalingSpec       `json:"autoscaling,omitempty"`

	// Affinity configures pod scheduling constraints for Fuseki server pods.
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
}

type FusekiClusterStorageSpec struct {
	ClassName *string `json:"className,omitempty"`

	// +kubebuilder:default=ReadWriteOnce
	AccessMode corev1.PersistentVolumeAccessMode `json:"accessMode,omitempty"`

	// +kubebuilder:default="20Gi"
	Size resource.Quantity `json:"size,omitempty"`
}

type FusekiLeaderElectionSpec struct {
	LeaseDuration metav1.Duration `json:"leaseDuration,omitempty"`
	RenewDeadline metav1.Duration `json:"renewDeadline,omitempty"`
	RetryPeriod   metav1.Duration `json:"retryPeriod,omitempty"`
}

type FusekiClusterServiceSpec struct {
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`

	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	ReadServiceName string `json:"readServiceName,omitempty"`
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	WriteServiceName string            `json:"writeServiceName,omitempty"`
	ReadAnnotations  map[string]string `json:"readAnnotations,omitempty"`
	WriteAnnotations map[string]string `json:"writeAnnotations,omitempty"`
}

type WorkloadObservabilitySpec struct {
	Metrics *WorkloadMetricsSpec `json:"metrics,omitempty"`
	Logging *WorkloadLoggingSpec `json:"logging,omitempty"`
}

type WorkloadMetricsSpec struct {
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	Path           string                      `json:"path,omitempty"`
	Service        WorkloadMetricsServiceSpec  `json:"service,omitempty"`
	ServiceMonitor *WorkloadServiceMonitorSpec `json:"serviceMonitor,omitempty"`
}

type WorkloadMetricsServiceSpec struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name,omitempty"`

	Annotations map[string]string `json:"annotations,omitempty"`
}

type WorkloadServiceMonitorSpec struct {
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	Interval metav1.Duration   `json:"interval,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

type WorkloadLoggingSpec struct {
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`
}

type FusekiClusterStatus struct {
	ObservedGeneration  int64              `json:"observedGeneration,omitempty"`
	Phase               string             `json:"phase,omitempty"`
	ConfigMapName       string             `json:"configMapName,omitempty"`
	HeadlessServiceName string             `json:"headlessServiceName,omitempty"`
	ReadServiceName     string             `json:"readServiceName,omitempty"`
	WriteServiceName    string             `json:"writeServiceName,omitempty"`
	WriteLeaseName      string             `json:"writeLeaseName,omitempty"`
	ActiveWritePod      string             `json:"activeWritePod,omitempty"`
	StatefulSetName     string             `json:"statefulSetName,omitempty"`
	MetricsServiceName  string             `json:"metricsServiceName,omitempty"`
	ReadyReplicas       int32              `json:"readyReplicas,omitempty"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type FusekiCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FusekiClusterSpec   `json:"spec,omitempty"`
	Status FusekiClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type FusekiClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FusekiCluster `json:"items"`
}

type RDFDeltaServerSpec struct {
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=1066
	ServicePort int32 `json:"servicePort,omitempty"`

	Resources       corev1.ResourceRequirements  `json:"resources,omitempty"`
	Storage         RDFDeltaStorageSpec          `json:"storage,omitempty"`
	BackupPolicyRef *corev1.LocalObjectReference `json:"backupPolicyRef,omitempty"`
	TLSSecretRef    *corev1.LocalObjectReference `json:"tlsSecretRef,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=7
	RetentionDays *int32 `json:"retentionDays,omitempty"`
}

type RDFDeltaStorageSpec struct {
	ClassName *string `json:"className,omitempty"`

	// +kubebuilder:default=ReadWriteOnce
	AccessMode corev1.PersistentVolumeAccessMode `json:"accessMode,omitempty"`

	// +kubebuilder:default="20Gi"
	Size resource.Quantity `json:"size,omitempty"`
}

type RDFDeltaServerStatus struct {
	ObservedGeneration  int64              `json:"observedGeneration,omitempty"`
	Phase               string             `json:"phase,omitempty"`
	ConfigMapName       string             `json:"configMapName,omitempty"`
	ServiceName         string             `json:"serviceName,omitempty"`
	HeadlessServiceName string             `json:"headlessServiceName,omitempty"`
	StatefulSetName     string             `json:"statefulSetName,omitempty"`
	BackupCronJobName   string             `json:"backupCronJobName,omitempty"`
	ActiveRestoreName   string             `json:"activeRestoreName,omitempty"`
	ReadyReplicas       int32              `json:"readyReplicas,omitempty"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type RDFDeltaServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RDFDeltaServerSpec   `json:"spec,omitempty"`
	Status RDFDeltaServerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type RDFDeltaServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RDFDeltaServer `json:"items"`
}

type DatasetSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// +kubebuilder:default=TDB2
	Type DatasetType `json:"type,omitempty"`

	DisplayName      string                        `json:"displayName,omitempty"`
	Preload          []DatasetPreloadSource        `json:"preload,omitempty"`
	Spatial          *JenaSpatialSpec              `json:"spatial,omitempty"`
	BackupPolicyRef  *corev1.LocalObjectReference  `json:"backupPolicyRef,omitempty"`
	SecurityPolicies []corev1.LocalObjectReference `json:"securityPolicies,omitempty"`
}

type DatasetPreloadSource struct {
	// +kubebuilder:validation:Pattern=`^(https?|s3)://.+`
	URI string `json:"uri"`

	Format    string                       `json:"format,omitempty"`
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

type JenaSpatialSpec struct {
	Enabled bool `json:"enabled,omitempty"`

	Assembler string `json:"assembler,omitempty"`
	// +kubebuilder:default=spatial
	SpatialIndexPath  string `json:"spatialIndexPath,omitempty"`
	AdditionalClasses string `json:"additionalClasses,omitempty"`
}

type DatasetStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	ConfigMapName      string             `json:"configMapName,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Dataset struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatasetSpec   `json:"spec,omitempty"`
	Status DatasetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type DatasetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Dataset `json:"items"`
}

// +kubebuilder:validation:Enum=FusekiCluster;FusekiServer
type EndpointTargetKind string

const (
	EndpointTargetKindFusekiCluster EndpointTargetKind = "FusekiCluster"
	EndpointTargetKindFusekiServer  EndpointTargetKind = "FusekiServer"
)

type EndpointTargetRef struct {
	Kind EndpointTargetKind `json:"kind"`

	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type EndpointServiceSpec struct {
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`

	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name,omitempty"`

	Annotations map[string]string `json:"annotations,omitempty"`
}

type EndpointSpec struct {
	TargetRef EndpointTargetRef `json:"targetRef"`

	SecurityProfileRef *corev1.LocalObjectReference `json:"securityProfileRef,omitempty"`
	Read               EndpointServiceSpec          `json:"read,omitempty"`
	Write              EndpointServiceSpec          `json:"write,omitempty"`
}

type EndpointStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	ReadServiceName    string             `json:"readServiceName,omitempty"`
	WriteServiceName   string             `json:"writeServiceName,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Endpoint struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EndpointSpec   `json:"spec,omitempty"`
	Status EndpointStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type EndpointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Endpoint `json:"items"`
}

type FusekiUISpec struct {
	TargetRef EndpointTargetRef    `json:"targetRef"`
	Service   EndpointServiceSpec  `json:"service,omitempty"`
	Ingress   *FusekiUIIngressSpec `json:"ingress,omitempty"`
	Gateway   *FusekiUIGatewaySpec `json:"gateway,omitempty"`
}

type FusekiUIIngressSpec struct {
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// +kubebuilder:default="/"
	Path string `json:"path,omitempty"`

	// +kubebuilder:validation:MinLength=1
	ClassName string `json:"className,omitempty"`

	Annotations map[string]string `json:"annotations,omitempty"`

	TLSSecretRef *corev1.LocalObjectReference `json:"tlsSecretRef,omitempty"`
}

type FusekiUIGatewayParentRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	Namespace string `json:"namespace,omitempty"`

	SectionName string `json:"sectionName,omitempty"`
}

type FusekiUIGatewaySpec struct {
	// +kubebuilder:validation:MinItems=1
	ParentRefs []FusekiUIGatewayParentRef `json:"parentRefs"`

	Hostnames []string `json:"hostnames,omitempty"`

	// +kubebuilder:default="/"
	Path string `json:"path,omitempty"`

	Annotations map[string]string `json:"annotations,omitempty"`
}

type FusekiUIStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	ServiceName        string             `json:"serviceName,omitempty"`
	IngressName        string             `json:"ingressName,omitempty"`
	HTTPRouteName      string             `json:"httpRouteName,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type FusekiUI struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FusekiUISpec   `json:"spec,omitempty"`
	Status FusekiUIStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type FusekiUIList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FusekiUI `json:"items"`
}

type SecurityOIDCSpec struct {
	IssuerURL string `json:"issuerURL,omitempty"`
	ClientID  string `json:"clientID,omitempty"`
}

type SecurityProfileSpec struct {
	AdminCredentialsSecretRef *corev1.LocalObjectReference `json:"adminCredentialsSecretRef,omitempty"`
	TLSSecretRef              *corev1.LocalObjectReference `json:"tlsSecretRef,omitempty"`
	OIDC                      *SecurityOIDCSpec            `json:"oidc,omitempty"`
	Authorization             *SecurityAuthorizationSpec   `json:"authorization,omitempty"`
}

type SecurityProfileStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	ConfigMapName      string             `json:"configMapName,omitempty"`
	AuthorizationMode  AuthorizationMode  `json:"authorizationMode,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type SecurityProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SecurityProfileSpec   `json:"spec,omitempty"`
	Status SecurityProfileStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SecurityProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SecurityProfile `json:"items"`
}

type BackupPolicySpec struct {
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`

	Suspend bool `json:"suspend,omitempty"`

	S3 BackupPolicyS3Spec `json:"s3"`

	Retention BackupPolicyRetentionSpec `json:"retention,omitempty"`
	Job       BackupPolicyJobSpec       `json:"job,omitempty"`
}

type BackupPolicyS3Spec struct {
	// +kubebuilder:validation:Pattern=`^https?://.+`
	Endpoint string `json:"endpoint"`

	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	Prefix string `json:"prefix,omitempty"`
	Region string `json:"region,omitempty"`

	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`

	Insecure bool `json:"insecure,omitempty"`
}

type BackupPolicyRetentionSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=7
	MaxBackups int32 `json:"maxBackups,omitempty"`
}

type BackupPolicyJobSpec struct {
	Image string `json:"image,omitempty"`

	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

type BackupPolicyStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type BackupPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupPolicySpec   `json:"spec,omitempty"`
	Status BackupPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type BackupPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupPolicy `json:"items"`
}

// +kubebuilder:validation:Enum=RDFDeltaServer
type RestoreRequestTargetKind string

const (
	RestoreRequestTargetKindRDFDeltaServer RestoreRequestTargetKind = "RDFDeltaServer"
)

type RestoreRequestTargetRef struct {
	Kind RestoreRequestTargetKind `json:"kind"`

	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type RestoreRequestSpec struct {
	TargetRef RestoreRequestTargetRef `json:"targetRef"`

	BackupPolicyRef *corev1.LocalObjectReference `json:"backupPolicyRef,omitempty"`

	BackupObject string `json:"backupObject,omitempty"`
}

type RestoreRequestStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	TargetName         string             `json:"targetName,omitempty"`
	JobName            string             `json:"jobName,omitempty"`
	ResolvedBackupRef  string             `json:"resolvedBackupRef,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type RestoreRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RestoreRequestSpec   `json:"spec,omitempty"`
	Status RestoreRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type RestoreRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RestoreRequest `json:"items"`
}

type FusekiServerSpec struct {
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=3030
	HTTPPort int32 `json:"httpPort,omitempty"`

	DatasetRefs        []corev1.LocalObjectReference `json:"datasetRefs,omitempty"`
	SecurityProfileRef *corev1.LocalObjectReference  `json:"securityProfileRef,omitempty"`
	Resources          corev1.ResourceRequirements   `json:"resources,omitempty"`
	Storage            FusekiClusterStorageSpec      `json:"storage,omitempty"`
	Service            FusekiServerServiceSpec       `json:"service,omitempty"`
	Observability      WorkloadObservabilitySpec     `json:"observability,omitempty"`
}

type FusekiServerServiceSpec struct {
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`

	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name        string            `json:"name,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type FusekiServerStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	ConfigMapName      string             `json:"configMapName,omitempty"`
	ServiceName        string             `json:"serviceName,omitempty"`
	DeploymentName     string             `json:"deploymentName,omitempty"`
	PVCName            string             `json:"pvcName,omitempty"`
	MetricsServiceName string             `json:"metricsServiceName,omitempty"`
	ReadyReplicas      int32              `json:"readyReplicas,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type FusekiServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FusekiServerSpec   `json:"spec,omitempty"`
	Status FusekiServerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type FusekiServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FusekiServer `json:"items"`
}

func init() {
	SchemeBuilder.Register(
		&FusekiCluster{},
		&FusekiClusterList{},
		&FusekiServer{},
		&FusekiServerList{},
		&RDFDeltaServer{},
		&RDFDeltaServerList{},
		&Dataset{},
		&DatasetList{},
		&Endpoint{},
		&EndpointList{},
		&FusekiUI{},
		&FusekiUIList{},
		&SecurityProfile{},
		&SecurityProfileList{},
		&BackupPolicy{},
		&BackupPolicyList{},
		&RestoreRequest{},
		&RestoreRequestList{},
	)
}

func (in *FusekiCluster) DesiredReplicas() int32 {
	if in.Spec.Replicas > 0 {
		return in.Spec.Replicas
	}

	return DefaultFusekiReplicas
}

func (in *FusekiCluster) DesiredHTTPPort() int32 {
	if in.Spec.HTTPPort > 0 {
		return in.Spec.HTTPPort
	}

	return DefaultFusekiHTTPPort
}

func (in *FusekiCluster) DesiredImagePullPolicy() corev1.PullPolicy {
	if in.Spec.ImagePullPolicy != "" {
		return in.Spec.ImagePullPolicy
	}

	return corev1.PullIfNotPresent
}

func (in *FusekiCluster) DesiredServiceType() corev1.ServiceType {
	if in.Spec.Services.Type != "" {
		return in.Spec.Services.Type
	}

	return corev1.ServiceTypeClusterIP
}

func (in *FusekiCluster) DesiredStorageAccessMode() corev1.PersistentVolumeAccessMode {
	if in.Spec.Storage.AccessMode != "" {
		return in.Spec.Storage.AccessMode
	}

	return corev1.ReadWriteOnce
}

func (in *FusekiCluster) DesiredStorageSize() resource.Quantity {
	if !in.Spec.Storage.Size.IsZero() {
		return in.Spec.Storage.Size.DeepCopy()
	}

	return resource.MustParse(DefaultStorageSize)
}

func (in *FusekiCluster) ReadServiceName() string {
	if in.Spec.Services.ReadServiceName != "" {
		return in.Spec.Services.ReadServiceName
	}

	return in.Name + "-read"
}

func (in *FusekiCluster) WriteServiceName() string {
	if in.Spec.Services.WriteServiceName != "" {
		return in.Spec.Services.WriteServiceName
	}

	return in.Name + "-write"
}

func (in *FusekiCluster) ConfigMapName() string {
	return in.Name + "-config"
}

func (in *FusekiCluster) ObservabilityMetricsEnabled() bool {
	if in.Spec.Observability.Metrics == nil {
		return false
	}
	if in.Spec.Observability.Metrics.Enabled == nil {
		return true
	}
	return *in.Spec.Observability.Metrics.Enabled
}

func (in *FusekiCluster) DesiredMetricsPath() string {
	if in.Spec.Observability.Metrics != nil && in.Spec.Observability.Metrics.Path != "" {
		return in.Spec.Observability.Metrics.Path
	}

	return DefaultMetricsPath
}

func (in *FusekiCluster) MetricsServiceName() string {
	if in.Spec.Observability.Metrics != nil && in.Spec.Observability.Metrics.Service.Name != "" {
		return in.Spec.Observability.Metrics.Service.Name
	}

	return in.Name + "-metrics"
}

func (in *FusekiCluster) ObservabilityServiceMonitorEnabled() bool {
	if !in.ObservabilityMetricsEnabled() || in.Spec.Observability.Metrics.ServiceMonitor == nil {
		return false
	}
	if in.Spec.Observability.Metrics.ServiceMonitor.Enabled == nil {
		return true
	}
	return *in.Spec.Observability.Metrics.ServiceMonitor.Enabled
}

func (in *FusekiCluster) DesiredMetricsInterval() time.Duration {
	if in.Spec.Observability.Metrics != nil && in.Spec.Observability.Metrics.ServiceMonitor != nil && in.Spec.Observability.Metrics.ServiceMonitor.Interval.Duration > 0 {
		return in.Spec.Observability.Metrics.ServiceMonitor.Interval.Duration
	}

	return DefaultMetricsScrapeInterval
}

func (in *FusekiCluster) HeadlessServiceName() string {
	return in.Name + "-headless"
}

func (in *FusekiCluster) StatefulSetName() string {
	return in.Name
}

func (in *FusekiCluster) WriteLeaseName() string {
	return in.Name + "-write"
}

func (in *FusekiCluster) DesiredLeaseDurationSeconds() int32 {
	if in.Spec.LeaderElection.LeaseDuration.Duration > 0 {
		return int32(in.Spec.LeaderElection.LeaseDuration.Duration.Seconds())
	}

	return 15
}

func (in *RDFDeltaServer) DesiredReplicas() int32 {
	if in.Spec.Replicas > 0 {
		return in.Spec.Replicas
	}

	return DefaultRDFDeltaReplicas
}

func (in *RDFDeltaServer) DesiredServicePort() int32 {
	if in.Spec.ServicePort > 0 {
		return in.Spec.ServicePort
	}

	return DefaultRDFDeltaServicePort
}

func (in *RDFDeltaServer) DesiredImagePullPolicy() corev1.PullPolicy {
	if in.Spec.ImagePullPolicy != "" {
		return in.Spec.ImagePullPolicy
	}

	return corev1.PullIfNotPresent
}

func (in *RDFDeltaServer) DesiredStorageAccessMode() corev1.PersistentVolumeAccessMode {
	if in.Spec.Storage.AccessMode != "" {
		return in.Spec.Storage.AccessMode
	}

	return corev1.ReadWriteOnce
}

func (in *RDFDeltaServer) DesiredStorageSize() resource.Quantity {
	if !in.Spec.Storage.Size.IsZero() {
		return in.Spec.Storage.Size.DeepCopy()
	}

	return resource.MustParse(DefaultStorageSize)
}

func (in *RDFDeltaServer) DesiredRetentionDays() int32 {
	if in.Spec.RetentionDays != nil && *in.Spec.RetentionDays > 0 {
		return *in.Spec.RetentionDays
	}

	return DefaultRDFDeltaRetentionDays
}

func (in *RDFDeltaServer) ServiceName() string {
	return in.Name
}

func (in *RDFDeltaServer) ConfigMapName() string {
	return in.Name + "-config"
}

func (in *RDFDeltaServer) HeadlessServiceName() string {
	return in.Name + "-headless"
}

func (in *RDFDeltaServer) StatefulSetName() string {
	return in.Name
}

func (in *RDFDeltaServer) BackupCronJobName() string {
	return in.Name + "-backup"
}

func (in *RDFDeltaServer) RestoreStatefulSetReplicas(activeRestore bool) int32 {
	if activeRestore {
		return 0
	}

	return in.DesiredReplicas()
}

func (in *Dataset) ConfigMapName() string {
	return in.Name + "-dataset-config"
}

func (in *Dataset) DesiredType() DatasetType {
	if in.Spec.Type != "" {
		return in.Spec.Type
	}

	return DefaultDatasetType
}

func (in *Dataset) DesiredSpatialIndexPath() string {
	if in.Spec.Spatial != nil && in.Spec.Spatial.SpatialIndexPath != "" {
		return in.Spec.Spatial.SpatialIndexPath
	}

	return "spatial"
}

func (in *Endpoint) ReadServiceName() string {
	if in.Spec.Read.Name != "" {
		return in.Spec.Read.Name
	}

	return in.Name + "-read"
}

func (in *Endpoint) WriteServiceName() string {
	if in.Spec.Write.Name != "" {
		return in.Spec.Write.Name
	}

	return in.Name + "-write"
}

func (in *Endpoint) DesiredReadServiceType() corev1.ServiceType {
	if in.Spec.Read.Type != "" {
		return in.Spec.Read.Type
	}

	return corev1.ServiceTypeClusterIP
}

func (in *Endpoint) DesiredWriteServiceType() corev1.ServiceType {
	if in.Spec.Write.Type != "" {
		return in.Spec.Write.Type
	}

	return corev1.ServiceTypeClusterIP
}

func (in *FusekiUI) ServiceName() string {
	if in.Spec.Service.Name != "" {
		return in.Spec.Service.Name
	}

	return in.Name
}

func (in *FusekiUI) DesiredServiceType() corev1.ServiceType {
	if in.Spec.Service.Type != "" {
		return in.Spec.Service.Type
	}

	return corev1.ServiceTypeClusterIP
}

func (in *FusekiUI) IngressName() string {
	return in.Name
}

func (in *FusekiUI) HTTPRouteName() string {
	return in.Name + "-route"
}

func (in *FusekiUI) DesiredIngressPath() string {
	if in.Spec.Ingress != nil && in.Spec.Ingress.Path != "" {
		return in.Spec.Ingress.Path
	}

	return "/"
}

func (in *FusekiUI) DesiredGatewayPath() string {
	if in.Spec.Gateway != nil && in.Spec.Gateway.Path != "" {
		return in.Spec.Gateway.Path
	}

	return "/"
}

func (in *FusekiServer) DesiredHTTPPort() int32 {
	if in.Spec.HTTPPort > 0 {
		return in.Spec.HTTPPort
	}

	return DefaultFusekiHTTPPort
}

func (in *FusekiServer) DesiredImagePullPolicy() corev1.PullPolicy {
	if in.Spec.ImagePullPolicy != "" {
		return in.Spec.ImagePullPolicy
	}

	return corev1.PullIfNotPresent
}

func (in *FusekiServer) DesiredServiceType() corev1.ServiceType {
	if in.Spec.Service.Type != "" {
		return in.Spec.Service.Type
	}

	return corev1.ServiceTypeClusterIP
}

func (in *FusekiServer) DesiredStorageAccessMode() corev1.PersistentVolumeAccessMode {
	if in.Spec.Storage.AccessMode != "" {
		return in.Spec.Storage.AccessMode
	}

	return corev1.ReadWriteOnce
}

func (in *FusekiServer) DesiredStorageSize() resource.Quantity {
	if !in.Spec.Storage.Size.IsZero() {
		return in.Spec.Storage.Size.DeepCopy()
	}

	return resource.MustParse(DefaultStorageSize)
}

func (in *FusekiServer) ObservabilityMetricsEnabled() bool {
	if in.Spec.Observability.Metrics == nil {
		return false
	}
	if in.Spec.Observability.Metrics.Enabled == nil {
		return true
	}
	return *in.Spec.Observability.Metrics.Enabled
}

func (in *FusekiServer) DesiredMetricsPath() string {
	if in.Spec.Observability.Metrics != nil && in.Spec.Observability.Metrics.Path != "" {
		return in.Spec.Observability.Metrics.Path
	}

	return DefaultMetricsPath
}

func (in *FusekiServer) MetricsServiceName() string {
	if in.Spec.Observability.Metrics != nil && in.Spec.Observability.Metrics.Service.Name != "" {
		return in.Spec.Observability.Metrics.Service.Name
	}

	return in.Name + "-metrics"
}

func (in *FusekiServer) ObservabilityServiceMonitorEnabled() bool {
	if !in.ObservabilityMetricsEnabled() || in.Spec.Observability.Metrics.ServiceMonitor == nil {
		return false
	}
	if in.Spec.Observability.Metrics.ServiceMonitor.Enabled == nil {
		return true
	}
	return *in.Spec.Observability.Metrics.ServiceMonitor.Enabled
}

func (in *FusekiServer) DesiredMetricsInterval() time.Duration {
	if in.Spec.Observability.Metrics != nil && in.Spec.Observability.Metrics.ServiceMonitor != nil && in.Spec.Observability.Metrics.ServiceMonitor.Interval.Duration > 0 {
		return in.Spec.Observability.Metrics.ServiceMonitor.Interval.Duration
	}

	return DefaultMetricsScrapeInterval
}

func (in *FusekiServer) ConfigMapName() string {
	return in.Name + "-config"
}

func (in *FusekiServer) ServiceName() string {
	if in.Spec.Service.Name != "" {
		return in.Spec.Service.Name
	}

	return in.Name
}

func (in *FusekiServer) DeploymentName() string {
	return in.Name
}

func (in *FusekiServer) PersistentVolumeClaimName() string {
	return in.Name + "-data"
}

func (in *SecurityProfile) ConfigMapName() string {
	return in.Name + "-security"
}

func (in *SecurityProfile) DesiredOIDCIssuerURL() string {
	if in.Spec.OIDC != nil && in.Spec.OIDC.IssuerURL != "" {
		return in.Spec.OIDC.IssuerURL
	}

	return ""
}

func (in *SecurityProfile) DesiredOIDCClientID() string {
	if in.Spec.OIDC != nil && in.Spec.OIDC.ClientID != "" {
		return in.Spec.OIDC.ClientID
	}

	return ""
}

func (in *BackupPolicy) DesiredBackupImage() string {
	if in.Spec.Job.Image != "" {
		return in.Spec.Job.Image
	}

	return "minio/mc:RELEASE.2025-07-21T05-28-08Z"
}

func (in *BackupPolicy) DesiredImagePullPolicy() corev1.PullPolicy {
	if in.Spec.Job.ImagePullPolicy != "" {
		return in.Spec.Job.ImagePullPolicy
	}

	return corev1.PullIfNotPresent
}

func (in *BackupPolicy) DesiredSuccessfulJobsHistoryLimit() int32 {
	if in.Spec.Job.SuccessfulJobsHistoryLimit != nil {
		return *in.Spec.Job.SuccessfulJobsHistoryLimit
	}

	return 3
}

func (in *BackupPolicy) DesiredFailedJobsHistoryLimit() int32 {
	if in.Spec.Job.FailedJobsHistoryLimit != nil {
		return *in.Spec.Job.FailedJobsHistoryLimit
	}

	return 1
}

func (in *BackupPolicy) DesiredMaxBackups() int32 {
	if in.Spec.Retention.MaxBackups > 0 {
		return in.Spec.Retention.MaxBackups
	}

	return 7
}

func (in *RestoreRequest) JobName() string {
	return in.Name + "-restore"
}

func (in *RestoreRequest) DesiredResolvedBackupRef() string {
	if in.Spec.BackupObject != "" {
		return in.Spec.BackupObject
	}

	return "latest"
}
