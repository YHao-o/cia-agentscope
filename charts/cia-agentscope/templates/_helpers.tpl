{{/* Expand the chart name. */}}
{{- define "cia-agentscope.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Web names and selectors intentionally differ from the backend selectors. */}}
{{- define "cia-agentscope.web.fullname" -}}
{{- printf "%s-web" (include "cia-agentscope.fullname" . | trunc 59 | trimSuffix "-") -}}
{{- end }}

{{- define "cia-agentscope.web.selectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-web" (include "cia-agentscope.name" . | trunc 59 | trimSuffix "-") }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: web
{{- end }}

{{- define "cia-agentscope.web.labels" -}}
helm.sh/chart: {{ include "cia-agentscope.chart" . }}
{{ include "cia-agentscope.web.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "cia-agentscope.web.serviceAccountName" -}}
{{- if .Values.web.serviceAccount.create -}}
{{- default (include "cia-agentscope.web.fullname" .) .Values.web.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.web.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "cia-agentscope.web.logClaimName" -}}
{{- default (printf "%s-logs" (include "cia-agentscope.web.fullname" . | trunc 58 | trimSuffix "-")) .Values.web.log.persistence.persistentVolumeClaim.existingClaim -}}
{{- end }}

{{- define "cia-agentscope.web.backend" -}}
{{- printf "%s:%v" (default (include "cia-agentscope.fullname" .) .Values.web.backend.serviceName) (default .Values.service.port .Values.web.backend.servicePort) -}}
{{- end }}
{{/* Create a stable fully qualified application name. */}}
{{- define "cia-agentscope.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Chart label value. */}}
{{- define "cia-agentscope.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Common labels. */}}
{{- define "cia-agentscope.labels" -}}
helm.sh/chart: {{ include "cia-agentscope.chart" . }}
{{ include "cia-agentscope.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Immutable selector labels. */}}
{{- define "cia-agentscope.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cia-agentscope.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Fully qualified application image. */}}
{{- define "cia-agentscope.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end }}

{{/* ServiceAccount name. */}}
{{- define "cia-agentscope.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cia-agentscope.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Secret name, generated only for development mode. */}}
{{- define "cia-agentscope.secretName" -}}
{{- if .Values.secret.create }}
{{- include "cia-agentscope.fullname" . }}
{{- else }}
{{- required "existingSecret.name is required when secret.create=false" .Values.existingSecret.name }}
{{- end }}
{{- end }}
