{{- define "ingestion-service.name" -}}
{{- default .Chart.Name .Values.nameOverride -}}
{{- end -}}

{{- define "ingestion-service.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "ingestion-service.name" . -}}
{{- if eq $name "" -}}
{{- $name = "ingestion-service" -}}
{{- end -}}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
