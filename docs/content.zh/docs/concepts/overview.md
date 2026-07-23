---
title: "Overview"
weight: 1
type: docs
aliases:
- /concepts/overview.html
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

Apache Flink already runs natively on Kubernetes. It can bring up a JobManager and TaskManagers and submit a job on a running cluster. But a streaming application lives for months, and that native integration effectively stops at deployment. Everything that comes afterward, noticing that a job is unhealthy and restarting it, taking a savepoint before an upgrade and restoring from it, rolling back a bad release, resizing the job as load shifts, is still left to a person. In production that means someone who understands Flink has to stay in the loop, indefinitely.

The Flink Kubernetes Operator exists to take that work off human hands. It automates the day-to-day operation of Flink, deploying applications, keeping them healthy, upgrading them safely, and scaling them to demand, turning what would otherwise be constant manual effort into a managed, largely hands-off process. It applies equally to long-running streaming jobs and to bounded batch jobs. Out of the box it runs on Kubernetes in either [native or standalone mode]({{< ref "docs/deployment/overview#operator-deployment-modes" >}}): using Flink's native Kubernetes integration, or deploying and managing the Flink pods directly.

The automation is only part of it. Across the full load range, from saturation to idle, the operator optimizes for both performance and cost. Under load it holds pipelines to their throughput targets and rolls out upgrades with no dip, readying the new version before the switchover. When a pipeline quiets down or goes idle, it scales parallelism to the minimum and trims cluster resources to match, so nothing keeps paying for capacity it no longer needs. Run continuously and pushed to their limit, these automatic behaviors go well beyond what hands-on operation could sustain. And all of it runs from a single lightweight instance: one operator, on a modest resource footprint, is built to manage hundreds or thousands of pipelines at once.

The operator is built on the Kubernetes [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/). Instead of being driven command by command, the desired Flink application is expressed declaratively as a [custom resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/), and the operator continuously reconciles the running cluster toward that desired state, observing what is actually running, comparing it against the declaration, and acting to close any gap. Flink applications become first-class Kubernetes objects, managed and secured with the same tooling as any other workload.

The operator also stays close to Flink itself. It integrates with the latest Flink versions and their features as they arrive, and aims to keep pace with everything Flink offers rather than lagging behind it. The supported versions are listed under [Compatibility → Supported Flink Versions]({{< ref "docs/deployment/compatibility#supported-flink-versions" >}}).

The operator's aim is to reduce human intervention to a minimum. That does not remove the need to understand it: knowing what the operator can do, how it manages a job's lifecycle, and where its limits lie is what makes it safe to trust with production workloads. The rest of this documentation builds that understanding.

## Operator Deliverables

The operator is built to be useful the moment it is installed, with no add-on components or custom automation required around it. A default installation already covers the essentials of running Flink in production:

- **[Lifecycle Management]({{< ref "docs/concepts/lifecycle-management" >}})**: the operator runs the full life of an application, deploying it, keeping it running, and cleanly suspending or deleting it. It upgrades jobs statefully or statelessly, rolls back failed upgrades, and restarts unhealthy jobs on its own, so routine operations and common failures no longer need a person on call.
- **[Zero-Downtime Upgrades]({{< ref "docs/concepts/zero-downtime-upgrades" >}})**: for jobs that cannot tolerate the brief restart of a normal upgrade, blue/green deployments bring the new version up alongside the old one and switch over only once it is proven healthy, so an upgrade never interrupts the stream.
- **[Autoscaling]({{< ref "docs/concepts/autoscaling" >}})**: the operator measures each job's real workload and continuously right-sizes its parallelism, adding capacity under load and releasing it as demand falls, keeping jobs on their throughput targets without over-provisioning.
- **Kubernetes-Native Operations**: everything an application needs to run well in a cluster is treated as a first-class concern, from Helm installation, RBAC, and high availability to metrics, logging, and ingress, so the operator fits existing Kubernetes practices instead of standing apart from them.

Taken together, these turn a Flink application from something that has to be watched and operated by hand into a workload that largely runs itself, shifting effort away from keeping the pipeline alive and toward improving what it does.

## Operator Capabilities

Each highlight above is backed by a broader set of concrete features. The groups below organize them by concern, from deployment and autoscaling to configuration, operations, and installation, with each feature linking to the page that documents it in full.

### Deployment and Lifecycle
- **[Custom Resources]({{< ref "docs/custom-resource/overview" >}})**: application clusters, session clusters, and session jobs declared as first-class Kubernetes objects, with snapshots and blue/green transitions expressed the same way.
- **[Job Management]({{< ref "docs/managing/job-management" >}})**: starting, suspending, resuming, upgrading, and deleting jobs, with stateful and stateless upgrade modes, automatic restarts and recovery, and rollbacks for upgrades that fail to stabilize.
- **[Snapshot Management]({{< ref "docs/managing/snapshot-management" >}})**: savepoints and checkpoints triggered and tracked declaratively through custom resources.
- **[Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}})**: a second deployment brought up beside the running one and switched over only once healthy, keeping the stream uninterrupted.
- **[High Availability]({{< ref "docs/deployment/leader-election" >}})**: Flink's Kubernetes HA services with standby JobManagers for the jobs, and leader election for the operator itself.

### Autoscaling
- **[Autoscaler]({{< ref "docs/managing/autoscaler" >}})**: per-vertex parallelism continuously right-sized from observed utilization, scaling up under load and down as demand falls.
- **[Autotuning]({{< ref "docs/managing/autotuning" >}})**: TaskManager memory right-sized from observed usage, applied alongside scaling actions without extra restarts.

### Configuration and Extensibility
- **[Configuration]({{< ref "docs/deployment/configuration" >}})**: default configuration with dynamic updates, per-job overrides, and environment variables.
- **[Pod Template]({{< ref "docs/custom-resource/pod-template" >}})**: native pod definitions with base, JobManager, and TaskManager layering.
- **[Ingress]({{< ref "docs/custom-resource/ingress" >}})**: dynamic ingress templates for reaching the Flink web UI, alongside Flink's native [service expose types](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui).
- **[Plugins]({{< ref "docs/deployment/plugins" >}})**: custom validators, mutators, and listeners for the custom resources, and pluggable phases of the autoscaler loop.

### Operations and Observability
- **[Metrics]({{< ref "docs/operations/metrics" >}})**: built on the [Flink metric system](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics), with pluggable reporters and detailed resource and Kubernetes API metrics.
- **[Logging]({{< ref "docs/operations/logging" >}})**: default and per-resource logging configuration for the operator and the Flink deployments it manages.
- **[Events]({{< ref "docs/operations/events" >}})**: Kubernetes events on the custom resources tracing deployments, upgrades, snapshots, and scaling decisions.
- **[State]({{< ref "docs/operations/state" >}})**: the resource status, autoscaler records, and Flink cluster ConfigMaps the operator maintains, with their recovery and cleanup behavior.

### Installation and Security
- **[Helm Installation]({{< ref "docs/deployment/helm/installation" >}})**: chart-driven installation with [RBAC]({{< ref "docs/deployment/helm/rbac" >}}) generated for the operator and its jobs, and extensive customization.
- **[Security]({{< ref "docs/deployment/security" >}})**: service account boundaries, pod security, webhook and Flink connection TLS, and keeping credentials out of the configuration.
- **Official Images**: published to [GHCR](https://ghcr.io/apache/flink-kubernetes-operator) and [DockerHub](https://hub.docker.com/r/apache/flink-kubernetes-operator).
