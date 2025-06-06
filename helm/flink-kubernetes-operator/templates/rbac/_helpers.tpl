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
Determine role scope based on name
*/}}
{{- define "flink-operator.roleScope" -}}
{{- if contains ":" .role  }}
{{- printf "ClusterRole" }}
{{- else }}
{{- printf "Role" }}
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
RBAC rules used to create the operator (cluster)role based on the scope
*/}}
{{- define "flink-operator.rbacRules" }}
rules:
  - apiGroups:
      - ""
    resources:
      - pods
      - services
      - events
      - configmaps
      - secrets
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
      - deletecollection
{{- if .Values.rbac.nodesRule.create }}
  - apiGroups:
    - ""
    resources:
      - nodes
    verbs:
      - list
{{- end }}
  - apiGroups:
      - apps
    resources:
      - deployments
      - deployments/finalizers
      - replicasets
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
  - apiGroups:
      - apps
    resources:
      - deployments/scale
    verbs:
      - get
      - update
      - patch
  - apiGroups:
      - extensions
    resources:
      - deployments
      - ingresses
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
  - apiGroups:
      - flink.apache.org
    resources:
      - flinkdeployments
      - flinkdeployments/finalizers
      - flinksessionjobs
      - flinksessionjobs/finalizers
      - flinkstatesnapshots
      - flinkstatesnapshots/finalizers
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
  - apiGroups:
      - flink.apache.org
    resources:
      - flinkdeployments/status
      - flinksessionjobs/status
      - flinkstatesnapshots/status
    verbs:
      - get
      - update
      - patch
  - apiGroups:
      - networking.k8s.io
    resources:
      - ingresses
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
  - apiGroups:
      - coordination.k8s.io
    resources:
      - leases
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
{{- end }}

{{/*
RBAC rules used to create the job (cluster)role based on the scope
*/}}
{{- define "flink-operator.jobRbacRules" }}
rules:
  - apiGroups:
      - ""
    resources:
      - pods
      - configmaps
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
  - apiGroups:
      - apps
    resources:
      - deployments
      - deployments/finalizers
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
{{- end }}
