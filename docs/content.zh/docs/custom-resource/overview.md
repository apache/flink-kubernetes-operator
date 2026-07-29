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

This page first maps how the resources relate, then walks each one with the same shape: purpose, a minimal example, and the fields that define it. The complete, always-current field list lives in the auto-generated [Reference]({{< ref "docs/custom-resource/reference" >}}), which this page links into rather than repeats. It closes with how spec changes are detected and applied. For the Flink and operator deployment modes a `FlinkDeployment` can take, see the [Deployment → Overview]({{< ref "docs/deployment/overview" >}}).

## Resource Relationships

Each resource is a Kubernetes Custom Resource with the usual `metadata`, `spec` (the authored desired state), and `status` (the observed state the operator writes back). The four kinds fall into three concerns:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/cr-relationships.svg" alt="How the custom resources relate" >}}

- **Cluster and Job Management**: a `FlinkDeployment` provides the cluster, either running its own application job or serving as a session cluster that many `FlinkSessionJob`s submit their jobs to.
- **Snapshot Management**: a `FlinkStateSnapshot` captures one savepoint or checkpoint of a job of either kind.
- **Blue/Green Deployments**: a `FlinkBlueGreenDeployment` owns two child `FlinkDeployment`s and shifts the active role between them for zero-downtime rollouts.

`FlinkDeployment` and `FlinkSessionJob` share the same job spec and status structure, which is why job-level behavior applies to both. Deletion is ordered and cascading: a session cluster will not delete while jobs still run on it, and a blue/green parent takes its children with it. The exact deletion rules are covered under [Job Management → Deleting]({{< ref "docs/managing/job-management#deleting" >}}), and what the operator materializes and cleans up in the cluster under [State]({{< ref "docs/operations/state" >}}).

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

Beyond `spec.job` and `spec.mode` above, the fields that shape a `FlinkDeployment` are `spec.image` and `spec.flinkVersion` for the runtime, `spec.jobManager` and `spec.taskManager` for cluster sizing (see [Pod Template]({{< ref "docs/custom-resource/pod-template" >}})), and `spec.flinkConfiguration` for arbitrary Flink settings.

The `status` is written only by the operator and is its reporting contract, with `status.lifecycleState` and `status.jobStatus` the fields to watch. The lifecycle states and their transitions are documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}), and every spec and status field, with types and defaults, in the auto-generated [Reference]({{< ref "docs/custom-resource/reference" >}}).

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

Beyond `spec.deploymentName` above, a `FlinkSessionJob` carries a `spec.job` and optional `spec.flinkConfiguration`, and its `status` mirrors a `FlinkDeployment`'s job-level fields such as `status.lifecycleState` and `status.jobStatus`. Every field is listed in the [Reference]({{< ref "docs/custom-resource/reference" >}}).

Its `spec.job.jarURI` may point to a remote filesystem, unlike a `FlinkDeployment` where the JAR typically ships inside the image. Remote fetching is restricted by default (the `https` scheme only, with internal addresses blocked) and configurable, as described under [Security → Artifact Fetching]({{< ref "docs/deployment/security#artifact-fetching" >}}). To fetch from other filesystems, extend the base image with the relevant plugin, see [Additional Dependencies]({{< ref "docs/deployment/plugins#additional-dependencies" >}}).

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

`spec.jobReference` names the target job and `spec.backoffLimit` bounds retries. The `status.state` moves from `TRIGGER_PENDING` to `IN_PROGRESS` and then to `COMPLETED`, `FAILED`, or `ABANDONED`, with `status.path` holding the final location. The full mechanics (periodic, upgrade, and manual snapshots, disposal on delete, and history cleanup) are covered in [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}), and every field in the [Reference]({{< ref "docs/custom-resource/reference" >}}).

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

Alongside `spec.template`, `spec.configuration` tunes the rollout (for example `kubernetes.operator.bluegreen.deployment-deletion.delay`), and `status.blueGreenState` reports the transition (`ACTIVE_BLUE`, `TRANSITIONING_TO_GREEN`, and so on). The deployment states, spec change handling, and configuration are documented under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}), the controller internals under [Blue/Green Controller]({{< ref "docs/internals/controllers#blue-green-controller" >}}), and every field in the [Reference]({{< ref "docs/custom-resource/reference" >}}).

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
