package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=Local;Ranger
type AuthorizationMode string

const (
	AuthorizationModeLocal  AuthorizationMode = "Local"
	AuthorizationModeRanger AuthorizationMode = "Ranger"
)

// +kubebuilder:validation:XValidation:rule="self.mode != 'Ranger' || has(self.ranger)",message="ranger settings are required when authorization mode is Ranger"
// +kubebuilder:validation:XValidation:rule="self.mode != 'Local' || !has(self.ranger)",message="ranger settings are only supported in Ranger mode"
type SecurityAuthorizationSpec struct {
	// +kubebuilder:default=Local
	Mode AuthorizationMode `json:"mode,omitempty"`

	Ranger *RangerAuthorizationSpec `json:"ranger,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="has(self.authSecretRef) && self.authSecretRef.name != ”",message="authSecretRef is required for Ranger authorization"
type RangerAuthorizationSpec struct {
	// +kubebuilder:validation:Pattern=`^https?://.+`
	AdminURL string `json:"adminURL,omitempty"`

	// +kubebuilder:validation:MinLength=1
	ServiceName string `json:"serviceName,omitempty"`

	AuthSecretRef *corev1.LocalObjectReference `json:"authSecretRef,omitempty"`
	// +kubebuilder:default="30s"
	PollInterval metav1.Duration `json:"pollInterval,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!self.enabled || self.maxReplicas > 0",message="maxReplicas is required when autoscaling is enabled"
// +kubebuilder:validation:XValidation:rule="!self.enabled || has(self.minReplicas)",message="minReplicas is required when autoscaling is enabled"
// +kubebuilder:validation:XValidation:rule="!self.enabled || has(self.targetCPUUtilizationPercentage) || has(self.targetMemoryUtilizationPercentage)",message="at least one autoscaling target must be configured when autoscaling is enabled"
// +kubebuilder:validation:XValidation:rule="!has(self.minReplicas) || self.maxReplicas == 0 || self.minReplicas <= self.maxReplicas",message="minReplicas must be less than or equal to maxReplicas"
type FusekiAutoscalingSpec struct {
	Enabled bool `json:"enabled,omitempty"`

	// +kubebuilder:validation:Minimum=1
	MinReplicas *int32 `json:"minReplicas,omitempty"`

	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetCPUUtilizationPercentage *int32 `json:"targetCPUUtilizationPercentage,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetMemoryUtilizationPercentage *int32 `json:"targetMemoryUtilizationPercentage,omitempty"`
}

// +kubebuilder:validation:Enum=User;Group;OIDCClaim
type SecuritySubjectType string

const (
	SecuritySubjectTypeUser      SecuritySubjectType = "User"
	SecuritySubjectTypeGroup     SecuritySubjectType = "Group"
	SecuritySubjectTypeOIDCClaim SecuritySubjectType = "OIDCClaim"
)

// +kubebuilder:validation:Enum=Allow;Deny
type SecurityPolicyEffect string

const (
	SecurityPolicyEffectAllow SecurityPolicyEffect = "Allow"
	SecurityPolicyEffectDeny  SecurityPolicyEffect = "Deny"
)

// +kubebuilder:validation:Enum=Simple;Accumulo
type SecurityPolicyExpressionType string

const (
	SecurityPolicyExpressionTypeSimple   SecurityPolicyExpressionType = "Simple"
	SecurityPolicyExpressionTypeAccumulo SecurityPolicyExpressionType = "Accumulo"
)

// +kubebuilder:validation:Enum=Query;Update;Read;Write;Admin
type SecurityPolicyAction string

const (
	SecurityPolicyActionQuery  SecurityPolicyAction = "Query"
	SecurityPolicyActionUpdate SecurityPolicyAction = "Update"
	SecurityPolicyActionRead   SecurityPolicyAction = "Read"
	SecurityPolicyActionWrite  SecurityPolicyAction = "Write"
	SecurityPolicyActionAdmin  SecurityPolicyAction = "Admin"
)

// +kubebuilder:validation:XValidation:rule="self.datasetRef.name != ”",message="datasetRef.name is required"
type DatasetAccessTarget struct {
	DatasetRef corev1.LocalObjectReference `json:"datasetRef"`
	NamedGraph string                      `json:"namedGraph,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.type != 'OIDCClaim' || size(self.claim) > 0",message="claim is required for OIDCClaim subjects"
// +kubebuilder:validation:XValidation:rule="self.type == 'OIDCClaim' || size(self.value) > 0",message="value is required for non-OIDCClaim subjects"
type SecuritySubject struct {
	Type  SecuritySubjectType `json:"type"`
	Value string              `json:"value,omitempty"`
	Claim string              `json:"claim,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="size(self.actions) > 0",message="at least one action is required"
// +kubebuilder:validation:XValidation:rule="size(self.subjects) > 0",message="at least one subject is required"
// +kubebuilder:validation:XValidation:rule="size(self.expression) > 0",message="expression is required"
type SecurityPolicyRule struct {
	Target  DatasetAccessTarget    `json:"target"`
	Actions []SecurityPolicyAction `json:"actions,omitempty"`
	// +kubebuilder:default=Allow
	Effect SecurityPolicyEffect `json:"effect,omitempty"`
	// +kubebuilder:default=Simple
	ExpressionType SecurityPolicyExpressionType `json:"expressionType,omitempty"`
	Expression     string                       `json:"expression,omitempty"`
	Subjects       []SecuritySubject            `json:"subjects,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="size(self.rules) > 0",message="at least one rule is required"
type SecurityPolicySpec struct {
	Description string               `json:"description,omitempty"`
	Rules       []SecurityPolicyRule `json:"rules,omitempty"`
}

type SecurityPolicyStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type SecurityPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SecurityPolicySpec   `json:"spec,omitempty"`
	Status SecurityPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SecurityPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SecurityPolicy `json:"items"`
}

// +kubebuilder:validation:Enum=Inline;ConfigMap
type SHACLSourceType string

const (
	SHACLSourceTypeInline    SHACLSourceType = "Inline"
	SHACLSourceTypeConfigMap SHACLSourceType = "ConfigMap"
)

// +kubebuilder:validation:Enum=Reject;ReportOnly
type SHACLFailureAction string

const (
	SHACLFailureActionReject     SHACLFailureAction = "Reject"
	SHACLFailureActionReportOnly SHACLFailureAction = "ReportOnly"
)

// +kubebuilder:validation:XValidation:rule="self.type != 'Inline' || size(self.inline) > 0",message="inline SHACL content is required for Inline sources"
// +kubebuilder:validation:XValidation:rule="self.type != 'Inline' || !has(self.configMapRef)",message="configMapRef is not allowed for Inline sources"
// +kubebuilder:validation:XValidation:rule="self.type != 'ConfigMap' || (has(self.configMapRef) && self.configMapRef.name != ”)",message="configMapRef is required for ConfigMap sources"
// +kubebuilder:validation:XValidation:rule="self.type != 'ConfigMap' || size(self.key) > 0",message="key is required for ConfigMap sources"
// +kubebuilder:validation:XValidation:rule="self.type != 'ConfigMap' || size(self.inline) == 0",message="inline content is not allowed for ConfigMap sources"
type SHACLSource struct {
	Type SHACLSourceType `json:"type,omitempty"`

	Inline string `json:"inline,omitempty"`

	ConfigMapRef *corev1.LocalObjectReference `json:"configMapRef,omitempty"`
	Key          string                       `json:"key,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="size(self.sources) > 0",message="at least one SHACL source is required"
type SHACLPolicySpec struct {
	Sources []SHACLSource `json:"sources,omitempty"`
	// +kubebuilder:default=Reject
	FailureAction SHACLFailureAction `json:"failureAction,omitempty"`
	ReportFormat  string             `json:"reportFormat,omitempty"`
}

type SHACLPolicyStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type SHACLPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SHACLPolicySpec   `json:"spec,omitempty"`
	Status SHACLPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SHACLPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SHACLPolicy `json:"items"`
}

// +kubebuilder:validation:Enum=URL;S3;Filesystem
type DataSourceType string

const (
	DataSourceTypeURL        DataSourceType = "URL"
	DataSourceTypeS3         DataSourceType = "S3"
	DataSourceTypeFilesystem DataSourceType = "Filesystem"
)

// +kubebuilder:validation:Enum=S3;Filesystem
type DataSinkType string

const (
	DataSinkTypeS3         DataSinkType = "S3"
	DataSinkTypeFilesystem DataSinkType = "Filesystem"
)

// +kubebuilder:validation:XValidation:rule="self.type == 'Filesystem' ? size(self.path) > 0 : size(self.uri) > 0",message="filesystem sources require path and URL or S3 sources require uri"
// +kubebuilder:validation:XValidation:rule="self.type == 'Filesystem' ? size(self.uri) == 0 : size(self.path) == 0",message="filesystem sources cannot set uri and URL or S3 sources cannot set path"
type DataSourceSpec struct {
	Type DataSourceType `json:"type"`

	URI       string                       `json:"uri,omitempty"`
	Path      string                       `json:"path,omitempty"`
	Format    string                       `json:"format,omitempty"`
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.type == 'Filesystem' ? size(self.path) > 0 : size(self.uri) > 0",message="filesystem sinks require path and S3 sinks require uri"
// +kubebuilder:validation:XValidation:rule="self.type == 'Filesystem' ? size(self.uri) == 0 : size(self.path) == 0",message="filesystem sinks cannot set uri and S3 sinks cannot set path"
type DataSinkSpec struct {
	Type DataSinkType `json:"type"`

	URI         string                       `json:"uri,omitempty"`
	Path        string                       `json:"path,omitempty"`
	Format      string                       `json:"format,omitempty"`
	Compression string                       `json:"compression,omitempty"`
	SecretRef   *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

type ImportRequestSpec struct {
	Target DatasetAccessTarget `json:"target"`
	Source DataSourceSpec      `json:"source"`
}

type ImportRequestStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	JobName            string             `json:"jobName,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ImportRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImportRequestSpec   `json:"spec,omitempty"`
	Status ImportRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ImportRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImportRequest `json:"items"`
}

type ExportRequestSpec struct {
	Target DatasetAccessTarget `json:"target"`
	Sink   DataSinkSpec        `json:"sink"`
}

type ExportRequestStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	JobName            string             `json:"jobName,omitempty"`
	ArtifactRef        string             `json:"artifactRef,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ExportRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ExportRequestSpec   `json:"spec,omitempty"`
	Status ExportRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ExportRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ExportRequest `json:"items"`
}

// +kubebuilder:validation:XValidation:rule="has(self.shaclPolicyRef) && self.shaclPolicyRef.name != ”",message="shaclPolicyRef is required"
type IngestPipelineSpec struct {
	Target         DatasetAccessTarget          `json:"target"`
	Source         DataSourceSpec               `json:"source"`
	SHACLPolicyRef *corev1.LocalObjectReference `json:"shaclPolicyRef,omitempty"`
	Schedule       string                       `json:"schedule,omitempty"`
	Suspend        bool                         `json:"suspend,omitempty"`
}

type IngestPipelineStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	LastRunTime        *metav1.Time       `json:"lastRunTime,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type IngestPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IngestPipelineSpec   `json:"spec,omitempty"`
	Status IngestPipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type IngestPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IngestPipeline `json:"items"`
}

// +kubebuilder:validation:XValidation:rule="self.rdfDeltaServerRef.name != ”",message="rdfDeltaServerRef.name is required"
type ChangeSubscriptionSpec struct {
	RDFDeltaServerRef corev1.LocalObjectReference `json:"rdfDeltaServerRef"`
	Target            *DatasetAccessTarget        `json:"target,omitempty"`
	Sink              DataSinkSpec                `json:"sink"`
	Suspend           bool                        `json:"suspend,omitempty"`
}

type ChangeSubscriptionStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	LastCheckpoint     string             `json:"lastCheckpoint,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ChangeSubscription struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ChangeSubscriptionSpec   `json:"spec,omitempty"`
	Status ChangeSubscriptionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ChangeSubscriptionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ChangeSubscription `json:"items"`
}

func init() {
	SchemeBuilder.Register(
		&SecurityPolicy{},
		&SecurityPolicyList{},
		&SHACLPolicy{},
		&SHACLPolicyList{},
		&ImportRequest{},
		&ImportRequestList{},
		&ExportRequest{},
		&ExportRequestList{},
		&IngestPipeline{},
		&IngestPipelineList{},
		&ChangeSubscription{},
		&ChangeSubscriptionList{},
	)
}

func (in *FusekiCluster) AutoscalingEnabled() bool {
	return in.Spec.Autoscaling != nil && in.Spec.Autoscaling.Enabled
}

func (in *SecurityProfile) DesiredAuthorizationMode() AuthorizationMode {
	if in.Spec.Authorization != nil && in.Spec.Authorization.Mode != "" {
		return in.Spec.Authorization.Mode
	}

	return AuthorizationModeLocal
}

func (in *SecurityProfile) RangerAuthorizationEnabled() bool {
	return in.DesiredAuthorizationMode() == AuthorizationModeRanger && in.Spec.Authorization != nil && in.Spec.Authorization.Ranger != nil
}

func (in *SecurityPolicyRule) DesiredEffect() SecurityPolicyEffect {
	if in.Effect != "" {
		return in.Effect
	}

	return SecurityPolicyEffectAllow
}

func (in *SecurityPolicyRule) DesiredExpressionType() SecurityPolicyExpressionType {
	if in.ExpressionType != "" {
		return in.ExpressionType
	}

	return SecurityPolicyExpressionTypeSimple
}

func (in *SHACLPolicy) DesiredFailureAction() SHACLFailureAction {
	if in.Spec.FailureAction != "" {
		return in.Spec.FailureAction
	}

	return SHACLFailureActionReject
}

func (in *ImportRequest) JobName() string {
	return in.Name + "-import"
}

func (in *ExportRequest) JobName() string {
	return in.Name + "-export"
}
