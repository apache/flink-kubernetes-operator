---
title: "Glossary"
weight: 6
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

# Glossary

This glossary defines the core terms used throughout the Flink Kubernetes Operator documentation. Each entry is a short definition plus a link to the page that covers the term in full. Flink runtime terms such as JobManager, TaskManager, and parallelism are defined in the [Apache Flink glossary](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/glossary/). The entries below focus on operator-specific concepts.

#### Autoscaler

The operator component that scales each job vertex to the parallelism needed to clear backpressure and hold a target utilization, driven by the vertex's measured processing rate and incoming data rate. See [Autoscaler]({{< ref "docs/managing/autoscaler" >}}).

#### Autotuning

Automatic tuning of a TaskManager's memory pools (heap, managed, network, and JVM overhead) from observed usage, applied alongside autoscaling. See [Autotuning]({{< ref "docs/managing/autotuning" >}}).

#### Blue/Green Deployment

A zero-downtime rollout, modeled by a `FlinkBlueGreenDeployment`, that keeps two child deployments (blue and green): the new color is started from a savepoint and becomes active only once it is healthy, after which the previous one is removed. See [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}).

#### Controller

The Java Operator SDK controller for a given custom resource (for example the `FlinkDeployment` controller) that runs the reconcile cycle, wiring together the [observer](#observation) and the [reconciler](#reconciler). See [Controllers]({{< ref "docs/internals/controllers" >}}).

#### Custom Resource

One of the operator's Kubernetes custom resources in the `flink.apache.org` API group: `FlinkDeployment`, `FlinkSessionJob`, `FlinkStateSnapshot`, or `FlinkBlueGreenDeployment`. See [Custom Resource]({{< ref "docs/custom-resource/overview" >}}).

#### Deployed Config

The effective Flink configuration the operator builds from the desired spec and uses when deploying or upgrading a job, as opposed to the [observed config](#observed-config) read back from a running deployment. See [Reconcilers]({{< ref "docs/internals/controllers#reconcilers" >}}).

#### JobManager Deployment Status

The observed state of a Flink JobManager's Kubernetes deployment, one of `READY`, `DEPLOYED_NOT_READY`, `DEPLOYING`, `MISSING`, or `ERROR`. See [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

#### Leader Election

The mechanism that allows several operator replicas to run at once with exactly one active reconciler, backed by a Kubernetes Lease that a standby takes over when the leader is lost. See [High Availability]({{< ref "docs/deployment/leader-election#operator-high-availability" >}}).

#### Lifecycle State

The operator's high-level computed state for a resource, one of `CREATED`, `SUSPENDED`, `UPGRADING`, `DEPLOYED`, `STABLE`, `ROLLING_BACK`, `ROLLED_BACK`, `FAILED`, `DELETING`, or `DELETED`. See [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

#### Native and Standalone Mode

The two operator deployment modes, selected with `spec.mode`: in Native mode Flink itself requests TaskManager pods from Kubernetes, while in Standalone mode the operator creates every resource and Flink is unaware of Kubernetes. See [Operator Deployment Modes]({{< ref "docs/deployment/overview#operator-deployment-modes" >}}).

#### Observation

The phase of the reconcile cycle in which the operator polls the Flink REST API and the Kubernetes API to refresh a resource's status (job status, JobManager deployment status, snapshot progress, and cluster health), carried out by the observer. See [Observers]({{< ref "docs/internals/controllers#observers" >}}).

#### Observed Config

The Flink configuration the operator derives from the currently deployed resource and uses during [observation](#observation), as opposed to the [deployed config](#deployed-config) built from the desired spec. See [Controllers]({{< ref "docs/internals/controllers" >}}).

#### Reconciler

The component that compares the desired spec against the last reconciled spec and carries out the resulting action (deploy, upgrade, savepoint, scale, or rollback) through the Flink service abstraction. See [Reconcilers]({{< ref "docs/internals/controllers#reconcilers" >}}).

#### Reconciliation

The operator's continuous control loop: it observes the actual state of a resource, compares it against the desired spec, and acts to close the gap, then repeats. See [Controllers]({{< ref "docs/internals/controllers" >}}).

#### Reconciliation State

The status field recording where a resource sits in the reconcile cycle, one of `DEPLOYED`, `UPGRADING`, `ROLLING_BACK`, or `ROLLED_BACK`, tracked alongside the last reconciled and last stable specs. See [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

#### Rollback

Automatic reversion to the last stable spec when an upgrade fails to become healthy within the readiness timeout. Requires high availability to be enabled. See [Rollbacks]({{< ref "docs/managing/job-management#rollbacks" >}}).

#### Snapshot

A savepoint or checkpoint captured from a running job, modeled by the `FlinkStateSnapshot` resource and triggered by upgrades, manually through trigger nonces, or periodically. See [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).

#### Spec and Status

The two halves of every custom resource: the spec is the desired state authored by the user, and the status subresource is the observed state written back by the operator, which continuously reconciles status toward spec. See [Custom Resource]({{< ref "docs/custom-resource/overview" >}}).

#### Spec Diff

The per-field classification of a spec change into a `DiffType`, one of `IGNORE`, `SCALE`, `UPGRADE`, or `SAVEPOINT_REDEPLOY`, which decides what action the reconciler takes. See [Spec Diff and Upgrade Decisions]({{< ref "docs/internals/controllers#spec-diff-and-upgrade-decisions" >}}).

#### True Processing Rate

The measured record rate a job vertex could sustain if it spent all of its time processing, serving as the autoscaler's view of the vertex's current capacity. See [Busy Time and Observed Rates]({{< ref "docs/managing/autoscaler#busy-time-and-observed-rates" >}}).

#### Upgrade Mode

The strategy used to restart a job when its spec changes, one of `savepoint`, `last-state`, or `stateless`, set through `spec.job.upgradeMode`. See [Upgrades]({{< ref "docs/managing/job-management#upgrades" >}}).

#### Utilization

The relation of a vertex's incoming data rate to the capacity that serves it: the autoscaler sizes the required capacity so the normal load consumes the configured target share. See [Utilization Band]({{< ref "docs/managing/autoscaler#utilization-band" >}}).
