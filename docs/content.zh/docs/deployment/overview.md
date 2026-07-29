---
title: "Overview"
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

# Overview

The Flink Kubernetes Operator deploys and manages Flink clusters on Kubernetes directly from custom resources. The desired cluster is described in a `FlinkDeployment` or `FlinkSessionJob`, and the operator reconciles it into the running Kubernetes objects that make up a Flink cluster.

A managed Flink cluster has few moving parts:
- The operator watches the custom resources and reconciles each one toward its declared spec.
- The JobManager it deploys coordinates the job, hosts the Flink REST API and Web UI, and manages checkpoints, while the TaskManagers carry the actual workload.
- Durable state such as checkpoint and savepoint storage lives in external systems, and the only client involved is `kubectl` or any other Kubernetes tooling applying a resource: there is no separate Flink client process to run.

Everything the operator needs lands in the cluster at installation time. Helm installs it into a designated operator namespace and registers the cluster-scoped custom resource definitions, while the Flink resources themselves live in one or more managed namespaces. Whether the operator watches the entire cluster or a fixed list of namespaces is also decided at installation time ([`watchNamespaces`]({{< ref "docs/deployment/helm/installation#watching-only-specific-namespaces" >}})), and that choice determines whether Helm creates a single ClusterRole or per-namespace Roles. What this adds up to on a real cluster is easiest to see laid out. The figure below shows every Kubernetes object present in a running installation with a single `FlinkDeployment` named `flink-dep`, in the default Native operator deployment mode:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/deployment/deployment-overview.svg" alt="Flink Kubernetes Operator deployment architecture" >}}

The figure reads in the order things come into existence:
- At cluster scope sit the four CRDs of the `flink.apache.org` API group, registered by Helm.
- The operator namespace holds the operator's own footprint: a single Deployment whose pod runs the operator and webhook containers, the [configuration ConfigMap]({{< ref "docs/deployment/configuration" >}}) they read, and the webhook's Service with its two TLS Secrets, `webhook-server-cert` carrying the certificate and `flink-operator-webhook-secret` the password of its keystore.
- The managed namespace holds the `FlinkDeployment` itself and everything materialized from it: the JobManager Deployment with its ReplicaSet and pods, the bare TaskManager pods, the REST Service, and the ConfigMaps that carry the Flink configuration, the pod template, the HA metadata, and the [autoscaler state]({{< ref "docs/operations/state#autoscaler-state" >}}).
- The entry points frame the picture: `kubectl` or Helm reaches everything through the Kubernetes API server, while a browser reaches the Flink Web UI through the optional Ingress in front of the REST Service.

{{< hint info >}}
Dashed borders mark objects that a default installation does not have. Each appears only when its feature is used:
- the Ingress, when `spec.ingress` is set
- the standby JobManager pod, when the JobManager replicas are raised
- the HA metadata ConfigMap, when high availability is enabled
- the autoscaler ConfigMap, when autoscaling is enabled
- the pod-template ConfigMap, when pod templates are used
- the Lease, when operator leader election is enabled
{{< /hint >}}

To keep the figure readable, a few objects are omitted: the internal Service (`flink-dep`) through which TaskManagers reach the JobManager, the RBAC objects (covered under [RBAC]({{< ref "docs/deployment/helm/rbac" >}})), the Mutating and Validating webhook configurations, any PersistentVolumeClaims mounted for the checkpoint and savepoint storage directories (present only when snapshot storage lives on volumes instead of external object storage), and Helm's own release-history Secrets (`sh.helm.release.v1.*`), which belong to Helm rather than the operator.

The figure also deliberately shows a single `FlinkDeployment`, because the other three resources barely change it:
- A `FlinkSessionJob` creates no Kubernetes objects of its own: the operator submits the job over the Flink REST API to a session cluster that an existing `FlinkDeployment` already provides.
- A `FlinkStateSnapshot` likewise only triggers savepoints or checkpoints through the REST API, with the results written to the configured snapshot storage.
- A `FlinkBlueGreenDeployment` multiplies the picture instead: it creates and owns two child `FlinkDeployment` resources, one per color, each with the full object tree shown above.

The object tree in the figure is also the cleanup story. Deleting the `FlinkDeployment` removes everything below it: the resource owns the JobManager Deployment and the autoscaler ConfigMap, and the JobManager Deployment in turn owns the Services, the `flink-config-*` and `pod-template-*` ConfigMaps, and the TaskManager pods that Flink's native integration creates. The one deliberate exception is the HA metadata ConfigMap. It has no owner precisely so that it survives failures and deletions, keeping the recovery data safe, and Flink removes it only when a job terminates gracefully (see the [High Availability limitations]({{< ref "docs/deployment/leader-election#limitations" >}})).

In the Standalone operator deployment mode the picture changes in exactly one place: TaskManagers are not bare pods but a `flink-dep-taskmanager` Deployment with its own ReplicaSet and pods, created and sized by the operator.

{{< hint warning >}}
Helm does not upgrade or delete CRDs on release upgrades ([Helm CRD handling](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/)). CRD upgrades are a manual step of the [operator upgrade process]({{< ref "docs/operations/upgrade" >}}).
{{< /hint >}}

## Operator Deployment Modes

A `FlinkDeployment` is materialized on Kubernetes in one of two operator deployment modes, selected with `spec.mode` ([`KubernetesDeploymentMode`]({{< ref "docs/custom-resource/reference#kubernetesdeploymentmode" >}})). Both modes exist in Flink itself, as the [Native Kubernetes](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/) and [Standalone Kubernetes](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/kubernetes/) resource providers. What the operator adds is making them a per-resource choice and taking over the resource management that standalone otherwise leaves to the user. The mode decides who creates and owns the cluster's Kubernetes resources, how elastic the cluster is, and whether Flink itself is allowed to talk to the Kubernetes API.

### Native Mode

Native is the default, used whenever `spec.mode` is unset. The operator delegates cluster creation to Flink's [Native Kubernetes Integration](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/): it starts the JobManager, and from there Flink drives its own resources, requesting TaskManager pods from the Kubernetes API when the job needs them and releasing them when it does not. This is what makes the cluster elastic: parallelism changes, autoscaling decisions, and recovery all translate into TaskManager pods appearing and disappearing without the operator's involvement.

Because Flink itself calls the Kubernetes API, the JobManager runs with a service account permitted to manage pods and ConfigMaps, the `flink` service account installed by Helm (see [RBAC]({{< ref "docs/deployment/helm/rbac" >}})). Native is the recommended mode for standard use.

### Standalone Mode

In Standalone mode the operator creates every Kubernetes resource itself: the JobManager Deployment and a TaskManager Deployment with a fixed number of replicas taken from `spec.taskManager.replicas`. Flink is not aware that it runs on Kubernetes and makes no Kubernetes API calls at all. Scaling is therefore the operator's job, and it means a redeployment: a change to the job parallelism or to `spec.taskManager.replicas` upgrades the cluster like any other spec change. The single exception is Flink's [Reactive Mode](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/elastic_scaling/#reactive-mode), available only for standalone application clusters: with `scheduler-mode: reactive` the operator just updates the replica count of the TaskManager Deployment, and the job adapts its parallelism to the TaskManagers that appear.

The replica count is also exposed through the Kubernetes scale subresource: `kubectl scale flinkdeployment <name> --replicas=<n>` and a [HorizontalPodAutoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) targeting the resource both drive `spec.taskManager.replicas` directly, a natural pairing with Reactive Mode. In Native mode the field has no effect, there the TaskManager count follows the job's slot needs.

The main reason to choose Standalone is isolation. A Flink cluster with no Kubernetes API access cannot create or modify anything in the cluster, which adds a layer of protection when running unknown or external user code.

## Flink Deployment Modes

The operator runs Flink jobs in one of two modes, mirroring Flink's own [deployment modes](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/overview/#deployment-modes), so only the operator-specific mapping is covered here.

### Application Mode

Each application gets its own dedicated cluster, and the job's `main()` runs on the JobManager. In the operator this is a `FlinkDeployment` with `spec.job` set. The cluster starts when the application is submitted and is torn down when it finishes, which gives strong resource isolation between applications. Application is the recommended mode for production jobs.

### Session Mode

A session cluster is long-lived and shared by many jobs. In the operator this is a `FlinkDeployment` with no `spec.job` (the bare cluster) plus one or more `FlinkSessionJob` resources submitted to it. It has lower per-job overhead, at the cost of weaker isolation, since a cluster-level failure affects every job running on it.

Jobs reach a session cluster only as jar artifacts: the operator fetches `spec.job.jarURI`, over `https` by default, with other schemes such as `s3` or `hdfs` enabled as described under [Security → Artifact Fetching]({{< ref "docs/deployment/security#artifact-fetching" >}}), and submits it through the Flink REST API. Other submission channels, such as the SQL Gateway or uploads through the Web UI, are not managed by the operator: jobs submitted that way still run, but as unmanaged jobs outside the resource lifecycle.

{{< hint info >}}
The job cancel button in the Flink Web UI is disabled by default for managed deployments, the operator sets `web.cancel.enable: false`. A managed job's lifecycle belongs to its custom resource, and a cancellation behind the operator's back would only be observed as an unexpected job failure. Stopping a job is done by suspending or deleting the resource, and the default can still be overridden through `spec.flinkConfiguration`.
{{< /hint >}}

For job lifecycle and upgrade behavior in both modes, see [Job Management]({{< ref "docs/managing/job-management" >}}).
