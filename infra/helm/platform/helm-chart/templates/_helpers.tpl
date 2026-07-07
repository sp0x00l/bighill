{{- define "bighill-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride -}}
{{- end -}}

{{- define "bighill-platform.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "bighill-platform.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
