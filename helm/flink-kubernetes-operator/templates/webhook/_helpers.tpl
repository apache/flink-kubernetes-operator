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

{{- define "flink-operator.webhook-enabled" -}}
{{- if or (eq (include "flink-operator.validating-webhook-enabled" .) "true") (eq (include "flink-operator.mutating-webhook-enabled" .) "true") }}
{{- printf "true" }}
{{- else }}
{{- printf "false" }}
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
