---
title: "Resource Exceptions"
weight: 1
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Resource Exceptions

A raw stack trace dumped into a status field is hard to consume. Stack traces can be very large, the most useful detail is often buried in a nested cause, and an exception type alone rarely tells whether the problem is in the job, in the cluster, or in the operator.

To solve this, the operator funnels every failure through a single, consistent path before it touches the resource. It turns a Java `Throwable` into a bounded, structured JSON document and writes it to `status.error`. The result is consistent across every failure path, safe to store on the resource (it is length-bounded), and rich enough to drive automation such as alerting or label-based routing.

When something goes wrong with a `FlinkDeployment`, `FlinkSessionJob`, or `FlinkStateSnapshot`, that `status.error` field is the first place to look. It allows diagnosing problems with nothing more than `kubectl`, without digging through operator logs or attaching to the cluster.

The example below shows how to query the field for a `FlinkDeployment`:

```bash
kubectl get flinkdeployment my-deployment -o jsonpath='{.status.error}'
```

## Failure Scenarios

For `FlinkDeployment` and `FlinkSessionJob`, several distinct failures all funnel into `status.error`. They fall into two groups.

- **Operator-side failures:** These are problems the operator hits while managing the resource, listed in roughly the order they arise over a resource's lifecycle.

  - **Spec validation error:** The submitted spec is invalid (`ValidationException`). The error is recorded only when it differs from the one already stored, and the spec is reset to the last reconciled version.
  - **Deployment bring-up failure:** The JobManager or TaskManager pods failed to start, for example due to an image pull error, scheduling failure, or init error (`DeploymentFailedException`). The operator additionally flips the JobManager status to `ERROR` and the job state to `RECONCILING`, and keeps the Kubernetes reason in the metadata.
  - **Missing JobManager during observation:** The JobManager deployment is gone while the resource still expects it to be running (`MissingJobManagerException`).
  - **Upgrade failure:** An in-place or savepoint upgrade could not complete (`UpgradeFailureException`).
  - **Generic reconcile or observe error:** Any other exception that escapes the reconcile or observe phase is rethrown as a `ReconciliationException` and recorded on each retry attempt through the JOSDK error hook.

- **Observed job failure:** This one is not an operator error.

  - **Job failure on the cluster:** When the observer finds the job in `FAILED` state, it records the job's serialized throwable (the job's own crash) on the resource and emits a warning event.

For `FlinkStateSnapshot`, there is a single failure path, handled through the snapshot controller's error hook. It triggers when reconciling a snapshot fails, for example when the trigger REST call fails, the snapshot cannot complete, or the referenced job is unavailable at runtime. This path does more than record the error. It also sets `state` to `FAILED`, increments the `failures` counter (which the controller compares against `spec.backoffLimit`), stamps `resultTimestamp`, and emits a `SavepointError` or `CheckpointError` event.

{{< hint info >}}
Spec validation errors only surface in `status.error` when the validating webhook is not running. With the webhook enabled, an invalid spec is rejected at submission time, so it never reaches the operator to be reconciled and recorded.
{{< /hint >}}

## Structure of the Recorded Exception

The value stored in `status.error` is a JSON serialization of a `FlinkResourceException`. The object has the following shape.

```json
{
  "type": "org.apache.flink.kubernetes.operator.exception.DeploymentFailedException",
  "message": "[flink-main-container] back-off 10s restarting failed container=flink-main-container pod=basic-example-...",
  "stackTrace": "org.apache.flink.kubernetes.operator.exception.DeploymentFailedException: ...",
  "additionalMetadata": {
    "reason": "CrashLoopBackOff"
  },
  "throwableList": []
}
```

| Field                | Description                                                                                                                                 |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `type`               | Fully qualified class name of the top-level throwable.                                                                                      |
| `message`            | The throwable's message.                                                                                                                    |
| `stackTrace`         | The full stack trace as a string. Only present when stack traces are enabled (see [Configuration](#configuration)).                         |
| `additionalMetadata` | A map of enriched, structured details extracted from the exception (see [Metadata Enrichment](#metadata-enrichment)).                       |
| `throwableList`      | A flattened list of the causal chain (the nested causes), each converted to the same structure. This is where the root cause usually lives. |

{{< hint info >}}
Fields are omitted from the JSON when they are null, so a minimal error may contain only `type` and `message`. The `throwableList` holds the causal chain of the recorded exception. A failure with no underlying cause, such as the `DeploymentFailedException` above, has an empty `throwableList`. A stack trace is attached only to the top-level entry, never to the nested causes.
{{< /hint >}}

## Metadata Enrichment

Beyond type, message, and stack trace, the operator pulls structured signals out of the exception into `additionalMetadata`. This is what makes the field useful for automation rather than just human reading.

- **HTTP response code:** When the failure is a `RestClientException` (a failed call to the Flink REST API), the HTTP status code is recorded under `httpResponseCode`.
- **Deployment reason:** When the failure is a `DeploymentFailedException` (pods could not start), the Kubernetes reason (for example `CrashLoopBackOff` or `ImagePullBackOff`) is recorded under `reason`.
- **Labels:** The `kubernetes.operator.exception.label.mapper` option allows attaching custom labels based on the exception message. Each entry is a regex matched against the messages in the exception chain, and the value is added to a `labels` list when the regex matches. For example, mapping `.*Heartbeat of TaskManager timed out.*` to `RECOVERABLE` lets downstream tooling distinguish transient failures from permanent ones without parsing free-form messages.

The enrichment logic is intentionally open-ended and can be extended to surface more structured details over time.

## Configuration

The size and verbosity of the recorded exception are bounded by operator configuration. All keys carry the `kubernetes.operator.` prefix. These settings are read from the operator's own configuration, not from a resource's `spec.flinkConfiguration`, so changing them means updating the operator ConfigMap.

| Key                                                      | Default | Description                                                                                                   |
|----------------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------|
| `kubernetes.operator.exception.stacktrace.enabled`       | `false` | Whether the top-level stack trace is included in `status.error`. Disabled by default to keep the field small. |
| `kubernetes.operator.exception.stacktrace.max.length`    | `2048`  | Maximum length of the stack trace string before it is truncated.                                              |
| `kubernetes.operator.exception.field.max.length`         | `2048`  | Maximum length of each individual exception field (type, message, metadata values).                           |
| `kubernetes.operator.exception.throwable.list.max.count` | `2`     | Maximum number of causes captured in `throwableList`.                                                         |
| `kubernetes.operator.exception.label.mapper`             | (empty) | Regex-to-label rules used to tag exceptions (see [Metadata Enrichment](#metadata-enrichment)).                |

For the full system configuration reference, see [Configuration]({{< ref "docs/deployment/configuration#system-configuration" >}}).

## Fallback Behavior

If the structured exception cannot be serialized to JSON for any reason, the operator does not lose the error: it falls back to writing a plain string into `status.error` instead. When the failure is wrapped in a `ReconciliationException`, the underlying cause is unwrapped first so the recorded message points at the real problem rather than the wrapper.

The net effect is that `status.error` always carries something useful, with a rich structured document in the common case and a readable string in the worst case.