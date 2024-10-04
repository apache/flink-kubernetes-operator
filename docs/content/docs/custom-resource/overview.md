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

The core user facing API of the Flink Kubernetes Operator is the FlinkDeployment and FlinkSessionJob Custom Resources (CR).

Custom Resources are extensions of the Kubernetes API and define new object types. In our case the FlinkDeployment CR defines Flink Application and Session cluster deployments. The FlinkSessionJob CR defines the session job on the Session cluster and each Session cluster can run multiple FlinkSessionJob.

Once the Flink Kubernetes Operator is installed and running in your Kubernetes environment, it will continuously watch FlinkDeployment and FlinkSessionJob objects submitted by the user to detect new CR and changes to existing ones. In case you haven't deployed the operator yet, please check out the [quickstart]({{< ref "docs/try-flink-kubernetes-operator/quick-start" >}}) for detailed instructions on how to get started.

With these two Custom Resources, we can support two different operational models:

- Flink application managed by the `FlinkDeployment`
- Empty Flink session managed by the `FlinkDeployment` + multiple jobs managed by the `FlinkSessionJobs`. The operations on the session jobs are independent of each other.

To help managing snapshots, there is another CR called FlinkStateSnapshot. This can be created by the operator in case of periodic and upgrade savepoints/checkpoints, or manually by the user to trigger a savepoint/checkpoint for a job.
FlinkStateSnapshots will always have a FlinkDeployment or FlinkSessionJob linked to them in their spec.

## FlinkDeployment

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
  clusterInfo:
    ...
  jobManagerDeploymentStatus: READY
  jobStatus:
    ...
  reconciliationStatus:
    ...
```

While the status contains a lot of information that the operator tracks about the deployment, it is considered to be internal to the operator logic and users should only rely on it with care.

## FlinkDeployment spec overview

The `spec` is the most important part of the `FlinkDeployment` as it describes the desired Flink Application or Session cluster.
The spec contains all the information the operator need to deploy and manage your Flink deployments, including docker images, configurations, desired state etc.

Most deployments will define at least the following fields:
 - `image` : Docker used to run Flink job and task manager processes
 - `flinkVersion` : Flink version used in the image (`v1_15`, `v1_16`, `v1_17`, `v1_18`, ...)
 - `serviceAccount` : Kubernetes service account used by the Flink pods
 - `taskManager, jobManager` : Job and Task manager pod resource specs (cpu, memory, ephemeralStorage)
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
  image: flink:1.17
  flinkVersion: v1_17
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
  serviceAccount: flink
  jobManager:
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
 - Stop / Delete Session cluster

### Cluster Deployment Modes
On-top of the deployment types the Flink Kubernetes Operator also supports two modes of deployments: **Native** and **Standalone**

Native cluster deployment is the default deployment mode and uses Flink's built in integration with Kubernetes when deploying the cluster. This integration means the Flink cluster communicates directly with Kubernetes and allows it to manage Kubernetes resources, e.g. dynamically allocate and de-allocate TaskManager pods.

For standard Operator use running your own Flink Jobs Native mode is recommended.

Standalone cluster deployment simply uses Kubernetes as an orchestration platform that the Flink cluster is running on. Flink is unaware that it is running on Kubernetes and therefore all Kubernetes resources need to be managed externally, by the Kubernetes Operator.

In Standalone mode the Flink cluster doesn't have access to the Kubernetes cluster so this can increase security. If unknown or external code is being ran on the Flink cluster then Standalone mode adds another layer of security.

The deployment mode can be set using the `mode` field in the deployment spec.

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
...
spec:
  ...
  mode: standalone


```

## FlinkSessionJob

The FlinkSessionJob have a similar structure to FlinkDeployment with the following required fields:

```
apiVersion: flink.apache.org/v1beta1
kind: FlinkSessionJob
metadata:
  name: basic-session-job-example
spec:
  deploymentName: basic-session-cluster
  job:
    jarURI: https://repo1.maven.org/maven2/org/apache/flink/flink-examples-streaming_2.12/1.16.1/flink-examples-streaming_2.12-1.16.1-TopSpeedWindowing.jar
    parallelism: 4
    upgradeMode: stateless
```

### FlinkSessionJob spec overview

The spec contains the information to submit a session job to the session cluster. Mostly, it will define at least the following fields:

 - deploymentName: The name of the target session cluster's CR
 - job: The specification for the Session Job

The job specification has the same structure in FlinkSessionJobs and FlinkDeployments, but in FlinkSessionJobs the jarUri can contain remote sources too.
It leverages the [Flink filesystem](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/overview/) mechanism to download the jar and submit to the session cluster.
So the FlinkSessionJob must be run with an existing session cluster managed by the FlinkDeployment.

To support jar from different filesystems, you should extend the base docker image as below, and put the related filesystem jar to the plugin dir and deploy the operator.
For example, to support the hadoop fs resource:

```shell script
FROM apache/flink-kubernetes-operator
ENV FLINK_PLUGINS_DIR=/opt/flink/plugins
COPY flink-hadoop-fs-1.19-SNAPSHOT.jar $FLINK_PLUGINS_DIR/hadoop-fs/
```

Alternatively, if you use helm to install flink-kubernetes-operator, it allows you to specify a postStart hook to download the required plugins.

## Further information

 - [Snapshots]({{< ref "docs/custom-resource/snapshots" >}})
 - [Job Management and Stateful upgrades]({{< ref "docs/custom-resource/job-management" >}})
 - [Deployment customization and pod templates]({{< ref "docs/custom-resource/pod-template" >}})
 - [Full Reference]({{< ref "docs/custom-resource/reference" >}})
 - [Examples](https://github.com/apache/flink-kubernetes-operator/tree/main/examples)
