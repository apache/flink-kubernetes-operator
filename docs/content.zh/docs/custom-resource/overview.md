---
title: "Overview"
weight: 1
type: docs
aliases:
- /custom-resource/overview.html
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

# Overview

The user-facing API of the Flink Kubernetes Operator is a small set of Kubernetes Custom Resources, all in the `flink.apache.org` API group, version `v1beta1`, and all namespaced:

| Resource (kind)            | Short name   | Purpose                                                                                       |
|----------------------------|--------------|-----------------------------------------------------------------------------------------------|
| `FlinkDeployment`          | `flinkdep`   | Runs a Flink Application cluster (one managed job) or a bare Session cluster.                 |
| `FlinkSessionJob`          | `sessionjob` | Runs a single job on an existing Session cluster.                                             |
| `FlinkStateSnapshot`       | `flinksnp`   | Represents a savepoint or checkpoint taken against a job. Experimental.                       |
| `FlinkBlueGreenDeployment` | `flinkbgdep` | Performs a zero-downtime, blue/green rollout over two child `FlinkDeployment`s. Experimental. |

The short names are usable with kubectl, for example `kubectl get flinkdep`.

Custom Resources are extensions of the Kubernetes API that define new object types. Once installed, the operator continuously watches these objects and reconciles each one, driving its observed state toward the declared desired state. Deploying the operator itself is covered under [Helm → Installation]({{< ref "docs/deployment/helm/installation" >}}), and the day-to-day operation of the declared resources under [Job Management]({{< ref "docs/managing/job-management" >}}).

This page first maps the relationships and ownership between the resources, then walks each one with the same shape: purpose, `kind`, a minimal example, and tables of the top-level `spec` and `status` fields. It closes with how spec changes are detected and applied. For the exhaustive field list, see the auto-generated [Reference]({{< ref "docs/custom-resource/reference" >}}). For the Flink and operator deployment modes a `FlinkDeployment` can take, see the [Deployment → Overview]({{< ref "docs/deployment/overview" >}}).

## Relationships and Ownership

Every one of these resources is a Kubernetes Custom Resource, so each carries three top-level parts: `metadata` (name, namespace, labels), `spec` (the authored desired state), and `status` (the observed state the operator writes back). The operator's job is to drive `status` toward `spec`.

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/cr-relationships.svg" alt="How the custom resources relate" >}}

The four resources form a small functional hierarchy:

- A `FlinkDeployment` stands on its own and provides the cluster, either running its own application job or serving as a shared session cluster.
- A `FlinkSessionJob` targets a session cluster by name through `spec.deploymentName`, and a single session cluster can serve many session jobs independently.
- A `FlinkStateSnapshot` references a job of either kind through `spec.jobReference` and tracks a single savepoint or checkpoint operation against it.
- A `FlinkBlueGreenDeployment` runs no job itself: it creates and owns two child `FlinkDeployment` resources and alternates the active role between them.

These links carry lifecycle dependencies. The child deployments of a `FlinkBlueGreenDeployment` are owned by their parent, so deleting the parent removes both children with everything below them. A session cluster is depended on by its jobs: deleting the cluster `FlinkDeployment` is blocked by default while `FlinkSessionJob` resources are still running on it, as described under [Job Management]({{< ref "docs/managing/job-management#deleting" >}}). A `FlinkStateSnapshot` needs its referenced job only while the snapshot executes, but deleting a savepoint resource also disposes the savepoint data when `spec.savepoint.disposeOnDelete` is enabled, which is the default, as described under [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).

Below the custom resources sit the native Kubernetes resources they materialize into. A `FlinkDeployment` brings up the JobManager Deployment, the cluster's Services and ConfigMaps, and, when configured, a generated [Ingress]({{< ref "docs/custom-resource/ingress" >}}), with what exactly runs depending on the [operator deployment mode]({{< ref "docs/deployment/overview#operator-deployment-modes" >}}). The operator manages these itself rather than leaving them to plain Kubernetes garbage collection: deleting a resource removes everything it created, as described under [Job Management → Deleting]({{< ref "docs/managing/job-management#deleting" >}}), and the ConfigMaps involved, including the HA metadata with its retention rules, are detailed under [State → Flink Cluster ConfigMaps]({{< ref "docs/operations/state#flink-cluster-configmaps" >}}). Resources merely referenced by a spec, user-provided ConfigMaps, Secrets, or volumes mounted through the pod templates, sit outside this contract entirely: the operator neither owns nor watches them.

`FlinkDeployment` and `FlinkSessionJob` share the same job-related spec and status structure (`spec.job`, `spec.flinkConfiguration`, `spec.restartNonce`, and the common status fields), which is why job-level behavior applies to both.

## FlinkDeployment

`FlinkDeployment` defines a Flink application cluster (a single managed job) or a bare session cluster. It is the most common entry point. Whether it is Application or Session is decided by whether `spec.job` is set, and the cluster runs in the Native or Standalone operator deployment mode, set with `spec.mode`. Both choices are explained in the [Deployment → Overview]({{< ref "docs/deployment/overview" >}}).

A minimal example of the `FlinkDeployment` kind, short name `flinkdep`:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: default
  name: basic-example
spec:
  image: flink:1.20
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
  serviceAccount: flink
  jobManager:
    resources:
      requests:
        memory: "2048Mi"
        cpu: "1"
  taskManager:
    resources:
      requests:
        memory: "2048Mi"
        cpu: "1"
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: stateless
    state: running
```

The resource is applied like any Kubernetes object, for example `kubectl apply -f basic-example.yaml`. More samples live in the [examples directory](https://github.com/apache/flink-kubernetes-operator/tree/main/examples).

Top-level `spec` fields:

| Field                | Type              | Notes                                                                                                                        |
|----------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------|
| `image`              | String            | Flink Docker image.                                                                                                          |
| `imagePullPolicy`    | String            | Image pull policy.                                                                                                           |
| `serviceAccount`     | String            | Kubernetes service account for the Flink pods.                                                                               |
| `flinkVersion`       | enum              | Flink version of the image (`v1_20`, `v2_0`, ...).                                                                           |
| `mode`               | enum              | `native` (default) or `standalone`, see [Deployment → Overview]({{< ref "docs/deployment/overview" >}}).                     |
| `jobManager`         | `JobManagerSpec`  | JobManager replicas, resources, and pod template, see [Pod Template]({{< ref "docs/custom-resource/pod-template" >}}).       |
| `taskManager`        | `TaskManagerSpec` | TaskManager replicas, resources, and pod template, see [Pod Template]({{< ref "docs/custom-resource/pod-template" >}}).      |
| `podTemplate`        | `PodTemplateSpec` | Base pod template shared by JobManager and TaskManager, see [Pod Template]({{< ref "docs/custom-resource/pod-template" >}}). |
| `ingress`            | `IngressSpec`     | Optional ingress for the Flink Web UI, see [Ingress]({{< ref "docs/custom-resource/ingress" >}}).                            |
| `logConfiguration`   | Map               | Log configuration overrides, see [Logging]({{< ref "docs/operations/logging" >}}).                                           |
| `job`                | `JobSpec`         | Present for Application clusters, omitted for Session, see [Job Management]({{< ref "docs/managing/job-management" >}}).     |
| `flinkConfiguration` | Map               | Flink configuration overrides, see [Configuration]({{< ref "docs/deployment/configuration" >}}).                             |
| `restartNonce`       | Long              | Change to trigger a full restart.                                                                                            |

Top-level `status` fields:

| Field                        | Type              | Notes                                                                                                              |
|------------------------------|-------------------|--------------------------------------------------------------------------------------------------------------------|
| `lifecycleState`             | enum              | The resource lifecycle state, see [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}). |
| `reconciliationStatus`       | object            | The last reconciled spec and the reconciliation state.                                                             |
| `jobStatus`                  | `JobStatus`       | State and snapshot information of the running job.                                                                 |
| `error`                      | String            | Last recorded error.                                                                                               |
| `observedGeneration`         | Long              | Last spec generation the operator acted on.                                                                        |
| `jobManagerDeploymentStatus` | enum              | State of the JobManager Kubernetes Deployment.                                                                     |
| `clusterInfo`                | Map               | Version, revision, and resource information of the running cluster.                                                |
| `taskManager`                | `TaskManagerInfo` | TaskManager label selector and replica count, application mode only.                                               |
| `conditions`                 | List              | Kubernetes-style conditions summarizing the resource state.                                                        |

The `status` subresource is written only by the operator and is its reporting contract: the lifecycle states, their transitions, and what they mean are documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

## FlinkSessionJob

`FlinkSessionJob` defines a single job submitted to an existing Session cluster. It links to its target cluster through `spec.deploymentName`, carries no cluster fields (no `image`, `jobManager`, or `taskManager`), and a Session cluster can run many of these independently.

A minimal example of the `FlinkSessionJob` kind, short name `sessionjob`:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkSessionJob
metadata:
  name: basic-session-job-example
spec:
  deploymentName: basic-session-cluster
  job:
    jarURI: https://repo1.maven.org/maven2/org/apache/flink/flink-examples-streaming/2.0.0/flink-examples-streaming-2.0.0-TopSpeedWindowing.jar
    parallelism: 4
    upgradeMode: stateless
```

Top-level `spec` fields:

| Field                | Type      | Notes                                                                                                      |
|----------------------|-----------|------------------------------------------------------------------------------------------------------------|
| `deploymentName`     | String    | Name of the target Session cluster `FlinkDeployment`.                                                      |
| `job`                | `JobSpec` | Always present here, see [Job Management]({{< ref "docs/managing/job-management" >}}).                     |
| `flinkConfiguration` | Map       | Job-level Flink configuration overrides, see [Configuration]({{< ref "docs/deployment/configuration" >}}). |
| `restartNonce`       | Long      | Change to trigger a restart.                                                                               |

Top-level `status` fields:

| Field                  | Type        | Notes                                                                                                              |
|------------------------|-------------|--------------------------------------------------------------------------------------------------------------------|
| `lifecycleState`       | enum        | The resource lifecycle state, see [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}). |
| `reconciliationStatus` | object      | The last reconciled spec and the reconciliation state.                                                             |
| `jobStatus`            | `JobStatus` | State and snapshot information of the running job.                                                                 |
| `error`                | String      | Last recorded error.                                                                                               |
| `observedGeneration`   | Long        | Last spec generation the operator acted on.                                                                        |

The `JobSpec` is the same as a `FlinkDeployment`, but a `FlinkSessionJob` `jarURI` may also point to a remote filesystem, downloaded via the [Flink filesystem](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/overview/) mechanism. By default the `jarURI` is restricted to the `https` scheme, and hosts that resolve to loopback, link-local, site-local, wildcard, or multicast addresses (such as `169.254.169.254`) are rejected. The allowlist can be extended through `kubernetes.operator.user.artifacts.allowed-schemes`, an operator-level option that is ignored in a CR's `flinkConfiguration`. A `FlinkDeployment` `jarURI` is not validated, since application clusters typically reference a JAR shipped inside the image. The full fetch policy, including the host restrictions and custom fetch headers, is described under [Security → Artifact Fetching]({{< ref "docs/deployment/security#artifact-fetching" >}}).

To fetch jars from other filesystems, extend the base image with the relevant filesystem plugin, as described under [Additional Dependencies]({{< ref "docs/deployment/plugins#additional-dependencies" >}}):

```dockerfile
FROM apache/flink-kubernetes-operator
ENV FLINK_PLUGINS_DIR=/opt/flink/plugins
COPY flink-hadoop-fs-1.20.0.jar $FLINK_PLUGINS_DIR/hadoop-fs/
```

When the operator is installed with Helm, a `postStart` hook can download the required plugins instead.

## FlinkStateSnapshot

{{< hint warning >}}
`FlinkStateSnapshot` is an experimental resource, enabled by default through `kubernetes.operator.snapshot.resource.enabled`.
{{< /hint >}}

`FlinkStateSnapshot` represents one savepoint or checkpoint operation against a target `FlinkDeployment` or `FlinkSessionJob`. The operator creates these for periodic and upgrade snapshots, and creating one manually triggers a savepoint or checkpoint. It supersedes the deprecated `savepointInfo` and `checkpointInfo` status fields.

A minimal example of the `FlinkStateSnapshot` kind, short name `flinksnp`:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkStateSnapshot
metadata:
  name: example-savepoint
spec:
  backoffLimit: 2
  jobReference:
    kind: FlinkDeployment
    name: basic-example
  savepoint: {}   # use checkpoint: {} for a checkpoint instead
```

Exactly one of `savepoint` or `checkpoint` is set. An empty `savepoint: {}` uses the savepoint path configured on the job.

Top-level `spec` fields:

| Field          | Type             | Notes                                                                                                                                                  |
|----------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `jobReference` | `JobReference`   | Target job: `kind` (`FlinkDeployment` or `FlinkSessionJob`) and `name`. Optional when referencing an existing savepoint.                               |
| `savepoint`    | `SavepointSpec`  | Savepoint request (`path`, `formatType` default `CANONICAL`, `disposeOnDelete` default `true`, `alreadyExists`). Mutually exclusive with `checkpoint`. |
| `checkpoint`   | `CheckpointSpec` | Checkpoint request, an empty marker. Mutually exclusive with `savepoint`.                                                                              |
| `backoffLimit` | int              | Retry limit, default `-1` for unlimited retries with exponential backoff, `0` for none.                                                                |

Top-level `status` fields:

| Field              | Type   | Notes                                                                    |
|--------------------|--------|--------------------------------------------------------------------------|
| `state`            | enum   | `TRIGGER_PENDING`, `IN_PROGRESS`, `COMPLETED`, `FAILED`, or `ABANDONED`. |
| `triggerId`        | String | Flink REST trigger id.                                                   |
| `triggerTimestamp` | String | When the operation was triggered.                                        |
| `resultTimestamp`  | String | When it completed.                                                       |
| `path`             | String | Final snapshot location.                                                 |
| `error`            | String | Error message, if any.                                                   |
| `failures`         | int    | Retry counter, compared against `backoffLimit`.                          |

A snapshot moves from `TRIGGER_PENDING` to `IN_PROGRESS` and then to one of `COMPLETED`, `FAILED`, or `ABANDONED`. The full mechanics (periodic, upgrade, and manual snapshots, disposal on delete, and history cleanup) are covered in [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).

## FlinkBlueGreenDeployment

{{< hint warning >}}
`FlinkBlueGreenDeployment` is experimental.
{{< /hint >}}

`FlinkBlueGreenDeployment` is a zero-downtime, blue/green rollout wrapper for stateful Flink applications, the concept behind it is covered under [Zero-Downtime Upgrades]({{< ref "docs/concepts/zero-downtime-upgrades" >}}). It maintains two child `FlinkDeployment`s, Blue and Green, with one active at a time. When the spec changes it brings up the inactive color from a savepoint, switches over once it is healthy, and deletes the previous deployment after a configurable delay.

A minimal example of the `FlinkBlueGreenDeployment` kind, short name `flinkbgdep`:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  name: basic-bg-example
spec:
  configuration:
    kubernetes.operator.bluegreen.deployment-deletion.delay: "2s"
  template:
    spec:
      image: flink:1.20
      flinkVersion: v1_20
      serviceAccount: flink
      jobManager:
        resources:
          requests:
            memory: "2048Mi"
            cpu: "1"
      taskManager:
        resources:
          requests:
            memory: "2048Mi"
            cpu: "1"
      job:
        jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
        parallelism: 2
        upgradeMode: savepoint
```

The embedded `template.spec` is a full `FlinkDeploymentSpec`, so anything valid for a `FlinkDeployment` is valid there.

Top-level `spec` fields:

| Field           | Type                          | Notes                                                                                                |
|-----------------|-------------------------------|------------------------------------------------------------------------------------------------------|
| `template`      | `FlinkDeploymentTemplateSpec` | The child deployment to roll out: `metadata` plus a full `FlinkDeploymentSpec`.                      |
| `configuration` | Map                           | Blue/green controller tuning, for example `kubernetes.operator.bluegreen.deployment-deletion.delay`. |
| `ingress`       | `IngressSpec`                 | Optional ingress, named per color to avoid collisions.                                               |

Top-level `status` fields:

| Field                      | Type        | Notes                                                                                           |
|----------------------------|-------------|-------------------------------------------------------------------------------------------------|
| `blueGreenState`           | enum        | The transition state, for example `ACTIVE_BLUE`, `TRANSITIONING_TO_GREEN`, `SAVEPOINTING_BLUE`. |
| `jobStatus`                | `JobStatus` | Status of the active job.                                                                       |
| `lastReconciledSpec`       | String      | Last spec the controller acted on.                                                              |
| `lastReconciledTimestamp`  | String      | When that happened.                                                                             |
| `deploymentReadyTimestamp` | String      | When the new color became ready.                                                                |
| `abortTimestamp`           | String      | When a transition was aborted, if any.                                                          |
| `savepointTriggerId`       | String      | Trigger id for the switchover savepoint.                                                        |
| `error`                    | String      | Error message, if any.                                                                          |

The deployment states, spec change handling, and configuration are documented under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}), and the controller internals under [Blue/Green Controller]({{< ref "docs/internals/controllers#blue-green-controller" >}}).

## Spec Diffing

A spec change does not always mean a restart. On every reconcile cycle the operator diffs the desired spec against the last successfully reconciled one, kept under `status.reconciliationStatus`, and classifies every changed field with one of four effects:

| Effect             | Triggered by                                                                                                                                                                                                                                                                                                     |
|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Nothing            | Settings the operator reads on the fly without touching the job: `flinkConfiguration` keys under `kubernetes.operator.*`, `job.autoscaler.*` and `parallelism.default`, and job fields such as `upgradeMode`, `initialSavepointPath` and `allowNonRestoredState`. The change is recorded, nothing is redeployed. |
| Scale              | Pure parallelism changes: `spec.job.parallelism` and `spec.taskManager.replicas` in Standalone mode, the `pipeline.jobvertex-parallelism-overrides` configuration key in Native mode. Applied in place when the cluster supports it, otherwise executed as a regular upgrade.                                    |
| Upgrade            | Every other field. The job is suspended and restored with the new spec according to its `upgradeMode`, as described under [Job Management]({{< ref "docs/managing/job-management#upgrades" >}}).                                                                                                                 |
| Savepoint redeploy | A changed `spec.job.savepointRedeployNonce`. The job is fully redeployed from the savepoint named in `spec.job.initialSavepointPath`.                                                                                                                                                                            |

The effects form an escalation ladder, listed above in the implementation's own order, from no disruption to a full redeploy from state. When a change touches several fields, the resulting action is the effect that minimally subsumes all of them, the highest one found: a single upgrade-classified field turns the whole change into an upgrade, no matter how many ignorable fields changed alongside it.

The diff sees only the spec itself. Changing the content of a referenced ConfigMap, Secret, or volume changes nothing in the spec, so it triggers no action from the operator, rolling such a change out requires a manual restart. That is what `spec.restartNonce` is for: changing it produces an upgrade-classified diff with an otherwise unchanged spec, making it the manual restart trigger, described under [Job Management → Restarting Without a Spec Change]({{< ref "docs/managing/job-management#restarting-without-a-spec-change" >}}).

A `FlinkBlueGreenDeployment` maps the same classification onto its own actions, deciding between patching the active deployment in place and starting a full blue/green transition, as covered under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments#spec-change-behavior" >}}). The full decision mechanics live under [Spec Diff and Upgrade Decisions]({{< ref "docs/internals/controllers#spec-diff-and-upgrade-decisions" >}}).
