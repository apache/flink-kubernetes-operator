################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

{{/*
Expand the name of the chart.
*/}}
{{- define "flink-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "flink-operator.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "flink-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "flink-operator.labels" -}}
{{ include "flink-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ include "flink-operator.chart" . }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "flink-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flink-operator.name" . }}
{{- end }}

{{/*
Create the path of the operator image to use
*/}}
{{- define "flink-operator.imagePath" -}}
{{- if .Values.image.digest }}
{{- .Values.image.repository }}@{{ .Values.image.digest }}
{{- else }}
{{- .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end }}
{{- end }}

{{/*
Create the name of the operator role to use
*/}}
{{- define "flink-operator.roleName" -}}
{{- if .Values.rbac.operatorRole.create }}
{{- default (include "flink-operator.fullname" .) .Values.rbac.operatorRole.name }}
{{- else }}
{{- default "default" .Values.rbac.operatorRole.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the operator role binding to use
*/}}
{{- define "flink-operator.roleBindingName" -}}
{{- if .Values.rbac.operatorRoleBinding.create }}
{{- default (include "flink-operator.fullname" .) .Values.rbac.operatorRoleBinding.name }}
{{- else }}
{{- default "default" .Values.rbac.operatorRoleBinding.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the job role to use
*/}}
{{- define "flink-operator.jobRoleName" -}}
{{- if .Values.rbac.jobRoleBinding.create }}
{{- default (include "flink-operator.fullname" .) .Values.rbac.jobRole.name }}
{{- else }}
{{- default "default" .Values.rbac.jobRole.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the job role to use
*/}}
{{- define "flink-operator.jobRoleBindingName" -}}
{{- if .Values.rbac.jobRole.create }}
{{- default (include "flink-operator.fullname" .) .Values.rbac.jobRoleBinding.name }}
{{- else }}
{{- default "default" .Values.rbac.jobRoleBinding.name }}
{{- end }}
{{- end }}


{{/*
Create the name of the operator service account to use
*/}}
{{- define "flink-operator.serviceAccountName" -}}
{{- if .Values.operatorServiceAccount.create }}
{{- default (include "flink-operator.fullname" .) .Values.operatorServiceAccount.name }}
{{- else }}
{{- default "default" .Values.operatorServiceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the job service account to use
*/}}
{{- define "flink-operator.jobServiceAccountName" -}}
{{- if .Values.jobServiceAccount.create }}
{{- default (include "flink-operator.fullname" .) .Values.jobServiceAccount.name }}
{{- else }}
{{- default "default" .Values.jobServiceAccount.name }}
{{- end }}
{{- end }}

{{/*
Determine role scope based on name
*/}}
{{- define "flink-operator.roleScope" -}}
{{- if contains ":" .role  }}
{{- printf "ClusterRole" }}
{{- else }}
{{- printf "Role" }}
{{- end }}
{{- end }}

{{- define "flink-operator.validating-webhook-enabled" -}}
{{- if hasKey .Values.webhook "validator" }}
{{- if .Values.webhook.validator.create }}
{{- printf "true" }}
{{- else }}
{{- printf "false" }}
{{- end }}
{{- else }}
{{- if or (.Values.webhook.create) }}
{{- printf "true" }}
{{- else }}
{{- printf "false" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "flink-operator.mutating-webhook-enabled" -}}
{{- if hasKey .Values.webhook "mutator" }}
{{- if .Values.webhook.mutator.create }}
{{- printf "true" }}
{{- else }}
{{- printf "false" }}
{{- end }}
{{- else }}
{{- if or (.Values.webhook.create) }}
{{- printf "true" }}
{{- else }}
{{- printf "false" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "flink-operator.webhook-enabled" -}}
{{- if or (eq (include "flink-operator.validating-webhook-enabled" .) "true") (eq (include "flink-operator.mutating-webhook-enabled" .) "true") }}
{{- printf "true" }}
{{- else }}
{{- printf "false" }}
{{- end }}
{{- end }}
