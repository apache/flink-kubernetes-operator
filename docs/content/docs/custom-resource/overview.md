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

# FlinkDeployment Overview

The core user facing API of the Flink Kubernetes Operator is the FlinkDeployment Custom Resource (CR).

Custom Resources are extensions of the Kubernetes API and define new object types. In our case the FlinkDeployment CR defines Flink Application and Session cluster deployments.

Once the Flink Kubernetes Operator is installed and running in your Kubernetes environment, it will continuously watch FlinkDeployment objects submitted by the user to detect new deployments and changes to existing ones. In case you haven't deployed the operator yet, please check out the [quickstart]({{< ref "docs/try-flink-kubernetes-operator/quick-start" >}}) for detailed instructions on how to get started.

FlinkDeployment objects are defined in YAML format by the user and must contain the following required fields:

```
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: namespace-of-my-deployment
  name: my-deployment
spec:
  // Deployment specs of your Flink Session/Application
```

The `apiVersion`, `kind` fields have fixed values while `metadata` and `spec` control the actual Flink deployment.

The Flink operator will subsequently add status information to your FlinkDeployment object based on the observed deployment state:

```
kubectl get flinkdeployment my-deployment -o yaml
```

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  ...
spec:
  ...
status:
  jobManagerDeploymentStatus: READY
  jobStatus:
    jobId: 93dfe8199a35d5503f4048a1a999c704
    jobName: State machine job
    savepointInfo: {}
    state: RUNNING
    updateTime: "1647351134601"
  reconciliationStatus:
    lastReconciledSpec:
      ...
    success: true
```

Users can use the status of the FlinkDeployment to gauge the health of their deployments and any executed operation.

## FlinkDeployment spec overview

The `spec` is the most important part of the `FlinkDeployment` as it describes the desired Flink Application or Session cluster.
The spec contains all the information the operator need to deploy and manage your Flink deployments, including docker images, configurations, desired state etc.

Most deployments will define at least the following fields:
 - `image` : Docker used to run Flink job and task manager processes
 - `flinkVersion` : Flink version used in the image (`v1_14`, `v1_15`...)
 - `serviceAccount` : Kubernetes service account used by the Flink pods
 - `taskManager, jobManager` : Job and Task manager pod resource specs (cpu, memory, etc.)
 - `flinkConfiguration` : Map of Flink configuration overrides such as HA and checkpointing configs
 - `job` : Job Spec for Application deployments

The Flink Kubernetes Operator supports two main types of deployments: **Application** and **Session**

Application deployments manage a single job deployment in Application mode while Session deployments manage Flink Session clusters without providing any job management for it. The type of cluster created depends on the `spec` provided by the user as we will see in the next sections.

### Application Deployments

To create an Application deployment users must define the `job` (JobSpec) field in their deployment spec.

Required fields:
 - `jarURI` : URI of the job jar
 - `parallelism` : Parallelism of the job
 - `upgradeMode` : Upgrade mode of the job (stateless/savepoint/last-state)
 - `state` : Desired state of the job (running/suspended)

Minimal example:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: default
  name: basic-example
spec:
  image: flink:1.14
  flinkVersion: v1_14
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
  serviceAccount: flink
  jobManager:
    replicas: 1
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: stateless
    state: running
```

Once created FlinkDeployment yamls can be submitted through kubectl:

```bash
kubectl apply -f your-deployment.yaml
```

### Session Cluster Deployments

Session clusters use a similar spec to application clusters with the only difference that `job` is not defined.

For Session clusters the operator only provides very basic management and monitoring that cover:
 - Start Session cluster
 - Monitor overall cluster health
 - Stop / Delete Session clsuter

## Further information

 - [Job Management and Stateful upgrades]({{< ref "docs/custom-resource/job-management" >}})
 - [Deployment customoziation and pod templates]({{< ref "docs/custom-resource/pod-template" >}})
 - [Full Reference]({{< ref "docs/custom-resource/reference" >}})
 - [Examples](https://github.com/apache/flink-kubernetes-operator/tree/main/examples)
