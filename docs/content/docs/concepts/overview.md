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
Flink Kubernetes Operator acts as a control plane to manage the complete deployment lifecycle of Apache Flink applications. Although Flink’s native Kubernetes integration already allows you to directly deploy Flink applications on a running Kubernetes(k8s) cluster, [custom resources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) and the [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) have also become central to a Kubernetes native deployment experience.

Flink Kubernetes Operator aims to capture the responsibilities of a human operator who is managing Flink deployments. Human operators have deep knowledge of how Flink deployments ought to behave, how to start clusters, how to deploy jobs, how to upgrade them and how to react if there are problems. The main goal of the operator is the automation of these activities, which cannot be achieved through the Flink native integration alone.

# Features
## Core
- Fully-automated [Job Lifecycle Management](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/job-management/)
  - Running, suspending and deleting applications
  - Stateful and stateless application upgrades
  - Triggering and managing savepoints
  - Handling errors, rolling-back broken upgrades
- Multiple Flink version support: v1.13, v1.14, v1.15
- [Deployment Modes](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/overview/#application-deployments):
  - Application cluster
  - Session cluster
  - Session job
- Built-in [High Availability](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/)   
- Extensible validation framework
  - Webhook and Operator based validation  
  - [Custom validators](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/validator/)  
- Advanced [Configuration](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/configuration/) management
  - Default configurations with dynamic updates
  - Per job configuration
  - Environment variables
- POD augmentation via [Pod Templates](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/pod-template/)
  - Native Kubernetes POD definitions
  - Layering (Base/JobManager/TaskManager overrides)
## Operations
- Operator [Metrics](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/metrics-logging/#metrics)
  - Utilizes the well-established [Flink Metric System](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics)
  - Pluggable metrics reporters
- Fully-customizable [Logging](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/metrics-logging/#logging)
  - Default log configuration  
  - Per job log configuration
  - Sidecar based log forwarders
- Flink Web UI and REST Endpoint Access
  - Fully supported Flink Native Kubernetes [service expose types](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui)
  - Dynamic [Ingress templates](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/ingress/)
- [Helm based installation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/helm/)
  - Automated [RBAC configuration](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/rbac/)
  - Advanced customization techniques 
- Up-to-date public repositories 
  - Helm [https://downloads.apache.org/flink/flink-kubernetes-operator-1.0.0/](https://downloads.apache.org/flink/flink-kubernetes-operator-1.0.0)
  - Docker [ghcr.io/apache/flink-kubernetes-operator](http://ghcr.io/apache/flink-kubernetes-operator)

# Known Issues & Limitations

## JobManager High-availability
The Operator leverages [Kubernetes HA Services](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/) for providing High-availability for Flink jobs. The HA solution can benefit form using additional [Standby replicas](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/overview/), it will result in a faster recovery time, but Flink jobs will still restart when the Leader JobManager goes down.

## Standalone Kubernetes Support
The Operator does not support [Standalone Kubernetes](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/kubernetes/) deployments yet

## JobResultStore Resource Leak
To mitigate the impact of [FLINK-27569](https://issues.apache.org/jira/browse/FLINK-27569) the operator introduced a workaround [FLINK-27573](https://issues.apache.org/jira/browse/FLINK-27573) by setting `job-result-store.delete-on-commit=false` and a unique value for `job-result-store.storage-path` for every cluster launch. The storage path for older runs must be cleaned up manually, keeping the latest directory always:
```shell
ls -lth /tmp/flink/ha/job-result-store/basic-checkpoint-ha-example/
total 0
drwxr-xr-x 2 9999 9999 40 May 12 09:51 119e0203-c3a9-4121-9a60-d58839576f01 <- must be retained
drwxr-xr-x 2 9999 9999 60 May 12 09:46 a6031ec7-ab3e-4b30-ba77-6498e58e6b7f
drwxr-xr-x 2 9999 9999 60 May 11 15:11 b6fb2a9c-d1cd-4e65-a9a1-e825c4b47543
```
