{{- /*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/ -}}

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
