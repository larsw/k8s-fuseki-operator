{{- define "fuseki-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fuseki-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "fuseki-operator.name" . -}}
{{- end -}}
{{- end -}}

{{- define "fuseki-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.name -}}
{{- .Values.serviceAccount.name -}}
{{- else -}}
{{- include "fuseki-operator.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "fuseki-operator.labels" -}}
app.kubernetes.io/name: fuseki-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
control-plane: controller-manager
{{- end -}}

{{- define "fuseki-operator.selectorLabels" -}}
app.kubernetes.io/name: fuseki-operator
control-plane: controller-manager
{{- end -}}
