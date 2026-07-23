---
title: "State"
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

# State

The Flink Kubernetes Operator keeps no durable state inside its own pod. Everything it needs to reconcile, observe, and autoscale the Flink resources it manages is persisted on the Kubernetes API server. This makes the operator pod disposable: it can crash, be rescheduled, or be scaled to zero and back without losing track of anything, and deleting a Flink resource cleans up all of its state along with it.

Operator state lives in three places:

- **Operator configuration ConfigMap**: the operator's own configuration. This is input managed by the Helm chart rather than state the operator writes, and is covered by the [Configuration]({{< ref "docs/deployment/configuration" >}}) page.
- **Custom resource `status`**: the source of truth for each resource's lifecycle, job status, and reconciliation history. There is one per `FlinkDeployment`, `FlinkSessionJob`, `FlinkStateSnapshot`, and `FlinkBlueGreenDeployment`.
- **Autoscaler ConfigMap**: per-resource scaling history and metrics, present only when the autoscaler is active.

The managed Flink clusters additionally carry ConfigMaps of their own, holding the Flink configuration, pod templates, and HA metadata, covered under [Flink Cluster ConfigMaps](#flink-cluster-configmaps). The operator does not store any state in annotations on the Flink resources, and the default Kubernetes deployment uses no external database or object store. The [standalone autoscaler](#standalone-autoscaler-state) is the one exception: it can persist its state in a relational database.

## Operator Configuration

The operator reads its own configuration from a single ConfigMap named `flink-operator-config` in the operator's namespace, installed and managed by the Helm chart and mounted into the operator pod. This is configuration input rather than state the operator writes, so it is not owned by any custom resource, and its lifecycle follows the Helm release instead of the Flink resources.

Inspect it with:

```bash
kubectl get configmap flink-operator-config -n <operator-namespace> -o yaml
```

The operator reads it on startup and can optionally reload it at runtime. For the configuration format, dynamic reload, and per-namespace or per-Flink-version overrides, see the [Configuration]({{< ref "docs/deployment/configuration" >}}) page.

## Custom Resource Status

Every resource the operator manages carries a `status` subresource, and it is the primary record of what the operator knows about that resource. The operator writes it through the Kubernetes status subresource API rather than a normal resource update. Two things follow from that:

- Refreshing observed fields such as `jobStatus` or `clusterInfo` does not increment `metadata.generation`, so a real spec change is never confused with a routine status update.
- The record lives on the API server, so the operator can rebuild its full picture from `status` alone after a restart.

The operator also skips writes when nothing changed, so a resource in steady state does not produce constant status churn.

Inspect the status of any resource with:

```bash
kubectl get flinkdeployment <name> -o jsonpath='{.status}' | jq
```

The fields most often inspected are:

| Field                        | Meaning                                                                                                                                                                           |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `lifecycleState`             | The operator's high-level view of the resource. One of `CREATED`, `SUSPENDED`, `UPGRADING`, `DEPLOYED`, `STABLE`, `ROLLING_BACK`, `ROLLED_BACK`, `FAILED`, `DELETING`, `DELETED`. |
| `jobStatus.state`            | The Flink job's own state (`RUNNING`, `FINISHED`, `FAILED`, and so on), alongside its `jobId`, start and update times, and the last savepoint and checkpoint info.                |
| `jobManagerDeploymentStatus` | Health of the JobManager deployment. One of `READY`, `DEPLOYED_NOT_READY`, `DEPLOYING`, `MISSING`, `ERROR`. Present on `FlinkDeployment` only.                                    |
| `reconciliationStatus.state` | Where the resource sits in the reconcile cycle. One of `DEPLOYED`, `UPGRADING`, `ROLLING_BACK`, `ROLLED_BACK`.                                                                    |
| `error`                      | The last reconciliation error, if any.                                                                                                                                            |

`reconciliationStatus` also stores the last reconciled spec and the last spec the operator considered stable, as JSON snapshots. The operator diffs against the reconciled snapshot to decide whether an upgrade is needed, and restores the stable snapshot on rollback. They are operator-internal and not meant to be edited, but reading `lastReconciledSpec` is a reliable way to see exactly what the operator last acted on, for example when debugging why a spec change did or did not trigger an upgrade.

The two snapshot-oriented resources track their own progress here as well. `FlinkStateSnapshot` records the snapshot lifecycle through `state` (`TRIGGER_PENDING`, `IN_PROGRESS`, `COMPLETED`, `FAILED`, `ABANDONED`), the final `path`, and a `failures` counter compared against `spec.backoffLimit`. `FlinkBlueGreenDeployment` records its transition through `blueGreenState`.

For the complete field-by-field schema of every status type, see the [Status section]({{< ref "docs/custom-resource/reference#status" >}}) of the Custom Resource Reference.

## Autoscaler State

When the [autoscaler]({{< ref "docs/managing/autoscaler" >}}) runs inside the operator, it keeps its per-resource working state in a dedicated ConfigMap next to the Flink resource:

| Property  | Value                                                                                               |
|-----------|-----------------------------------------------------------------------------------------------------|
| Name      | `autoscaler-<resource-name>`                                                                        |
| Namespace | Same as the parent `FlinkDeployment` or `FlinkSessionJob`                                           |
| Labels    | `component=autoscaler`, `app=<resource-name>`                                                       |
| Owner     | The parent resource, so Kubernetes deletes the ConfigMap automatically when the resource is removed |

It is created lazily on the first scaling evaluation, so a resource that has never been autoscaled has no autoscaler ConfigMap yet. Its `data` map holds the autoscaler's working memory:

| Key                    | Contents                                                                                                                                                                                                           |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `scalingHistory`       | Past scaling decisions per vertex, used to detect ineffective scaling and oscillation.                                                                                                                             |
| `scalingTracking`      | In-flight and recently completed rescale operations, used to estimate job restart time.                                                                                                                            |
| `collectedMetrics`     | The rolling window of metric samples (processing rate, target rate, busy time) that scaling decisions are based on. This is the largest entry and the first to be trimmed when the ConfigMap nears its size limit. |
| `parallelismOverrides` | The current recommended parallelism per vertex, applied back to the running job.                                                                                                                                   |
| `configOverrides`      | Memory-tuning recommendations (managed memory, network memory, JVM overhead, metaspace).                                                                                                                           |
| `delayedScaleDown`     | When each vertex first became eligible for scale-down, backing the configurable scale-down delay.                                                                                                                  |

List and inspect these ConfigMaps with:

```bash
kubectl get configmap autoscaler-<name> -o yaml
kubectl get configmap -l component=autoscaler -n <namespace>
```

{{< hint warning >}}
The scaling history, scaling tracking, and collected metrics are always stored compressed and Base64-encoded, so they are not human-readable. Do not hand-edit this ConfigMap. Editing or deleting it can reset the parallelism of every autoscaled vertex and discard the scaling history the autoscaler depends on. A Kubernetes ConfigMap cannot exceed roughly 1 MB, so the operator caps its stored state at about that size and trims the oldest collected metrics first as the cap is approached. Trimming only shortens the metric window used for the next scaling decision. The scaling history and current parallelism overrides are left untouched.
{{< /hint >}}

To read one of the compressed values while debugging, Base64-decode and decompress it. For example, to inspect the scaling history behind past decisions:

```bash
kubectl get configmap autoscaler-<name> -o jsonpath='{.data.scalingHistory}' | base64 -d | gunzip
```

The same applies to `scalingTracking` and `collectedMetrics`. The remaining values (`parallelismOverrides`, `configOverrides`, `delayedScaleDown`) are stored as plain text and can be read directly.

{{< hint info >}}
Turning autoscaling off for a resource removes its ConfigMap, and deleting the resource removes it through the owner reference. No manual cleanup is needed either way.
{{< /hint >}}

### Standalone Autoscaler State

The autoscaler can also run as a [standalone process]({{< ref "docs/managing/autoscaler#standalone-autoscaler" >}}) outside the operator, for example against a non-Kubernetes Flink cluster. In that mode it has no custom resources to attach state to, so instead of the ConfigMap above it persists its state in a pluggable store, chosen with `autoscaler.standalone.state-store.type`:

| Store     | `state-store.type` | Use case                                                                                                           |
|-----------|--------------------|--------------------------------------------------------------------------------------------------------------------|
| In-memory | `MEMORY` (default) | State is held on the process heap and discarded when the process restarts, so it suits tests and short-lived runs. |
| JDBC      | `JDBC`             | State is persisted in a relational database and survives restarts. Recommended for production.                     |

The JDBC store keeps one row per job and state type in a single `t_flink_autoscaler_state_store` table, which must be provisioned ahead of time. Unlike the Kubernetes ConfigMap store, the values are plain YAML with no compression. Select the store and point it at the database with:

```yaml
autoscaler.standalone.state-store.type: JDBC
autoscaler.standalone.jdbc.url: jdbc:mysql://<host>:<port>/<db>
autoscaler.standalone.jdbc.username: <user>
autoscaler.standalone.jdbc.password-env-variable: JDBC_PWD
```

See [Standalone Autoscaler]({{< ref "docs/managing/autoscaler#standalone-autoscaler" >}}) for the full setup, including the table definition and JDBC driver requirements.

## Flink Cluster ConfigMaps

Besides the operator's own state, every managed Flink cluster carries ConfigMaps in the resource's namespace. They exist because the Flink processes read plain files, not custom resources: the spec's content is rendered into mountable ConfigMaps together with the defaults and the keys the operator and Flink inject, so like the operator configuration they are inputs rather than state the operator writes:

- **`flink-config-<name>`**: the Flink configuration and logging configuration mounted by the JobManager and TaskManagers, generated from `spec.flinkConfiguration` and `spec.logConfiguration`. It is owned by the JobManager Deployment and regenerated on every deployment and upgrade, so manual edits do not survive: configuration changes belong in the spec.
- **`pod-template-<name>`**: present when [pod templates]({{< ref "docs/custom-resource/pod-template" >}}) are used, carrying the generated pod template files the JobManager applies when it creates TaskManager pods. Owned by the JobManager Deployment as well.
- **HA metadata ConfigMaps**: present when [high availability]({{< ref "docs/deployment/leader-election#flink-job-high-availability" >}}) is enabled, holding the leader and recovery metadata a successor JobManager needs. Deliberately ownerless so they survive failures and deletions, as described under the [Deployment Overview]({{< ref "docs/deployment/overview" >}}).

{{< hint info >}}
How these ConfigMaps are managed depends on the [operator deployment mode]({{< ref "docs/deployment/overview#operator-deployment-modes" >}}). In Native mode, Flink's own Kubernetes integration creates and mounts them, with the JobManager referencing them at runtime as it spawns TaskManager pods. In Standalone mode, Flink is unaware of Kubernetes: the operator materializes `flink-config-<name>` itself while building the JobManager and TaskManager Deployments, and pod templates are merged directly into those Deployments, so `pod-template-<name>` exists in Native mode only.
{{< /hint >}}

## Recovery and Cleanup

Because all of this state lives on the API server, the operator process is disposable. When an operator pod starts, it rebuilds its picture entirely from what is already stored:

1. It lists every managed resource and reads each `status` subresource.
2. It resumes the reconcile cycle from `reconciliationStatus`, using the last reconciled spec to detect any outstanding changes.
3. It re-attaches to running Flink clusters using the recorded `jobStatus` and cluster info, instead of resubmitting them.
4. It reads each autoscaler ConfigMap again on the next scaling evaluation.

{{< hint info >}}
Deleting a Flink resource is equally self-contained. Kubernetes garbage-collects the resource's `status` along with the resource itself, and cascades to the autoscaler ConfigMap through its owner reference. No external cleanup job is required.
{{< /hint >}}
