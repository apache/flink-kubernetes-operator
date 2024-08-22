---
title: "Reference"
weight: 4
type: docs
aliases:
- /custom-resource/reference.html
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

# FlinkDeployment Reference

This page serves as a full reference for FlinkDeployment custom resource definition including all the possible configuration parameters.

## FlinkDeployment
**Class**: org.apache.flink.kubernetes.operator.crd.FlinkDeployment

**Description**: Custom resource that represents both Application and Session deployments.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| spec | org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec | Spec that describes a Flink application or session cluster deployment. |
| status | org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus | Last observed status of the Flink deployment. |

## Spec

### FlinkDeploymentSpec
**Class**: org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec

**Description**: Spec that describes a Flink application or session cluster deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| job | org.apache.flink.kubernetes.operator.api.spec.JobSpec | Job specification for application deployments/session job. Null for session clusters. |
| restartNonce | java.lang.Long | Nonce used to manually trigger restart for the cluster/session job. In order to trigger restart, change the number to a different non-null value. |
| flinkConfiguration | java.util.Map<java.lang.String,java.lang.String> | Flink configuration overrides for the Flink deployment or Flink session job. |
| image | java.lang.String | Flink docker image used to start the Job and TaskManager pods. |
| imagePullPolicy | java.lang.String | Image pull policy of the Flink docker image. |
| serviceAccount | java.lang.String | Kubernetes service used by the Flink deployment. |
| flinkVersion | org.apache.flink.kubernetes.operator.api.spec.FlinkVersion | Flink image version. |
| ingress | org.apache.flink.kubernetes.operator.api.spec.IngressSpec | Ingress specs. |
| podTemplate | io.fabric8.kubernetes.api.model.PodTemplateSpec | Base pod template for job and task manager pods. Can be overridden by the jobManager and taskManager pod templates. |
| jobManager | org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec | JobManager specs. |
| taskManager | org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec | TaskManager specs. |
| logConfiguration | java.util.Map<java.lang.String,java.lang.String> | Log configuration overrides for the Flink deployment. Format logConfigFileName -> configContent. |
| mode | org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode | Deployment mode of the Flink cluster, native or standalone. |

### FlinkSessionJobSpec
**Class**: org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec

**Description**: Spec that describes a Flink session job.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| job | org.apache.flink.kubernetes.operator.api.spec.JobSpec | Job specification for application deployments/session job. Null for session clusters. |
| restartNonce | java.lang.Long | Nonce used to manually trigger restart for the cluster/session job. In order to trigger restart, change the number to a different non-null value. |
| flinkConfiguration | java.util.Map<java.lang.String,java.lang.String> | Flink configuration overrides for the Flink deployment or Flink session job. |
| deploymentName | java.lang.String | The name of the target session cluster deployment. |

### FlinkVersion
**Class**: org.apache.flink.kubernetes.operator.api.spec.FlinkVersion

**Description**: Enumeration for supported Flink versions.

| Value | Docs |
| ----- | ---- |
| v1_13 | No longer supported since 1.7 operator release. |
| v1_14 | No longer supported since 1.7 operator release. |
| v1_15 |  |
| v1_16 |  |
| v1_17 |  |
| v1_18 |  |
| v1_19 |  |
| v1_20 |  |

### IngressSpec
**Class**: org.apache.flink.kubernetes.operator.api.spec.IngressSpec

**Description**: Ingress spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| template | java.lang.String | Ingress template for the JobManager service. |
| className | java.lang.String | Ingress className for the Flink deployment. |
| annotations | java.util.Map<java.lang.String,java.lang.String> | Ingress annotations. |
| labels | java.util.Map<java.lang.String,java.lang.String> | Ingress labels. |
| tls | java.util.List<io.fabric8.kubernetes.api.model.networking.v1.IngressTLS> | Ingress tls. |

### JobManagerSpec
**Class**: org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec

**Description**: JobManager spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| resource | org.apache.flink.kubernetes.operator.api.spec.Resource | Resource specification for the JobManager pods. |
| replicas | int | Number of JobManager replicas. Must be 1 for non-HA deployments. |
| podTemplate | io.fabric8.kubernetes.api.model.PodTemplateSpec | JobManager pod template. It will be merged with FlinkDeploymentSpec.podTemplate. |

### JobSpec
**Class**: org.apache.flink.kubernetes.operator.api.spec.JobSpec

**Description**: Flink job spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jarURI | java.lang.String | Optional URI of the job jar within the Flink docker container. For example: local:///opt/flink/examples/streaming/StateMachineExample.jar. If not specified the job jar should be available in the system classpath. |
| parallelism | int | Parallelism of the Flink job. |
| entryClass | java.lang.String | Fully qualified main class name of the Flink job. |
| args | java.lang.String[] | Arguments for the Flink job main class. |
| state | org.apache.flink.kubernetes.operator.api.spec.JobState | Desired state for the job. |
| savepointTriggerNonce | java.lang.Long | Nonce used to manually trigger savepoint for the running job. In order to trigger a savepoint, change the number to a different non-null value. |
| initialSavepointPath | java.lang.String | Savepoint path used by the job the first time it is deployed or during savepoint redeployments (triggered by changing the savepointRedeployNonce). |
| checkpointTriggerNonce | java.lang.Long | Nonce used to manually trigger checkpoint for the running job. In order to trigger a checkpoint, change the number to a different non-null value. |
| upgradeMode | org.apache.flink.kubernetes.operator.api.spec.UpgradeMode | Upgrade mode of the Flink job. |
| allowNonRestoredState | java.lang.Boolean | Allow checkpoint state that cannot be mapped to any job vertex in tasks. |
| savepointRedeployNonce | java.lang.Long | Nonce used to trigger a full redeployment of the job from the savepoint path specified in initialSavepointPath. In order to trigger redeployment, change the number to a different non-null value. Rollback is not possible after redeployment. |

### JobState
**Class**: org.apache.flink.kubernetes.operator.api.spec.JobState

**Description**: Enum describing the desired job state.

| Value | Docs |
| ----- | ---- |
| running | Job is expected to be processing data. |
| suspended | Processing is suspended with the intention of continuing later. |

### KubernetesDeploymentMode
**Class**: org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode

**Description**: Enum to control Flink deployment mode on Kubernetes.

| Value | Docs |
| ----- | ---- |
| native | Deploys Flink using Flinks native Kubernetes support. Only supported for newer versions of Flink |
| standalone | Deploys Flink on-top of kubernetes in standalone mode. |

### Resource
**Class**: org.apache.flink.kubernetes.operator.api.spec.Resource

**Description**: Resource spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| cpu | java.lang.Double | Amount of CPU allocated to the pod. |
| memory | java.lang.String | Amount of memory allocated to the pod. Example: 1024m, 1g |
| ephemeralStorage | java.lang.String | Amount of ephemeral storage allocated to the pod. Example: 1024m, 2G |

### TaskManagerSpec
**Class**: org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec

**Description**: TaskManager spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| resource | org.apache.flink.kubernetes.operator.api.spec.Resource | Resource specification for the TaskManager pods. |
| replicas | java.lang.Integer | Number of TaskManager replicas. If defined, takes precedence over parallelism |
| podTemplate | io.fabric8.kubernetes.api.model.PodTemplateSpec | TaskManager pod template. It will be merged with FlinkDeploymentSpec.podTemplate. |

### UpgradeMode
**Class**: org.apache.flink.kubernetes.operator.api.spec.UpgradeMode

**Description**: Enum to control Flink job upgrade behavior.

| Value | Docs |
| ----- | ---- |
| savepoint | Job is upgraded by first taking a savepoint of the running job, shutting it down and restoring from the savepoint. |
| last-state | Job is upgraded using any latest checkpoint or savepoint available. |
| stateless | Job is upgraded with empty state. |

## Status

### Checkpoint
**Class**: org.apache.flink.kubernetes.operator.api.status.Checkpoint

**Description**: Represents information about a finished checkpoint.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| timeStamp | long | Millisecond timestamp at the start of the checkpoint operation. |
| triggerType | org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType | Checkpoint trigger mechanism. |
| formatType | org.apache.flink.kubernetes.operator.api.status.CheckpointType | Checkpoint format. |
| triggerNonce | java.lang.Long | Nonce value used when the checkpoint was triggered manually {@link SnapshotTriggerType#MANUAL}, null for other types of checkpoint. |

### CheckpointInfo
**Class**: org.apache.flink.kubernetes.operator.api.status.CheckpointInfo

**Description**: Stores checkpoint-related information.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| lastCheckpoint | org.apache.flink.kubernetes.operator.api.status.Checkpoint | Last completed checkpoint by the operator. |
| triggerId | java.lang.String | Trigger id of a pending checkpoint operation. |
| triggerTimestamp | java.lang.Long | Trigger timestamp of a pending checkpoint operation. |
| triggerType | org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType | Checkpoint trigger mechanism. |
| formatType | org.apache.flink.kubernetes.operator.api.status.CheckpointType | Checkpoint format. |
| lastPeriodicCheckpointTimestamp | long | Trigger timestamp of last periodic checkpoint operation. |

### CheckpointType
**Class**: org.apache.flink.kubernetes.operator.api.status.CheckpointType

**Description**: Checkpoint format type.

| Value | Docs |
| ----- | ---- |
| FULL |  |
| INCREMENTAL |  |
| UNKNOWN | Checkpoint format unknown, if the checkpoint was not triggered by the operator. |
| description | org.apache.flink.configuration.description.InlineElement |  |

### FlinkDeploymentReconciliationStatus
**Class**: org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentReconciliationStatus

**Description**: Status of the last reconcile step for the flink deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| reconciliationTimestamp | long | Epoch timestamp of the last successful reconcile operation. |
| lastReconciledSpec | java.lang.String | Last reconciled deployment spec. Used to decide whether further reconciliation steps are necessary. |
| lastStableSpec | java.lang.String | Last stable deployment spec according to the specified stability condition. If a rollback strategy is defined this will be the target to roll back to. |
| state | org.apache.flink.kubernetes.operator.api.status.ReconciliationState | Deployment state of the last reconciled spec. |

### FlinkDeploymentStatus
**Class**: org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus

**Description**: Last observed status of the Flink deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jobStatus | org.apache.flink.kubernetes.operator.api.status.JobStatus | Last observed status of the Flink job on Application/Session cluster. |
| error | java.lang.String | Error information about the FlinkDeployment/FlinkSessionJob. |
| observedGeneration | java.lang.Long | Last observed generation of the FlinkDeployment/FlinkSessionJob. |
| lifecycleState | org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState | Lifecycle state of the Flink resource (including being rolled back, failed etc.). |
| clusterInfo | java.util.Map<java.lang.String,java.lang.String> | Information from running clusters. |
| jobManagerDeploymentStatus | org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus | Last observed status of the JobManager deployment. |
| reconciliationStatus | org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentReconciliationStatus | Status of the last reconcile operation. |
| taskManager | org.apache.flink.kubernetes.operator.api.status.TaskManagerInfo | Information about the TaskManagers for the scale subresource. |

### FlinkSessionJobReconciliationStatus
**Class**: org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobReconciliationStatus

**Description**: Status of the last reconcile step for the flink sessionjob.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| reconciliationTimestamp | long | Epoch timestamp of the last successful reconcile operation. |
| lastReconciledSpec | java.lang.String | Last reconciled deployment spec. Used to decide whether further reconciliation steps are necessary. |
| lastStableSpec | java.lang.String | Last stable deployment spec according to the specified stability condition. If a rollback strategy is defined this will be the target to roll back to. |
| state | org.apache.flink.kubernetes.operator.api.status.ReconciliationState | Deployment state of the last reconciled spec. |

### FlinkSessionJobStatus
**Class**: org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus

**Description**: Last observed status of the Flink Session job.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jobStatus | org.apache.flink.kubernetes.operator.api.status.JobStatus | Last observed status of the Flink job on Application/Session cluster. |
| error | java.lang.String | Error information about the FlinkDeployment/FlinkSessionJob. |
| observedGeneration | java.lang.Long | Last observed generation of the FlinkDeployment/FlinkSessionJob. |
| lifecycleState | org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState | Lifecycle state of the Flink resource (including being rolled back, failed etc.). |
| reconciliationStatus | org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobReconciliationStatus | Status of the last reconcile operation. |

### JobManagerDeploymentStatus
**Class**: org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus

**Description**: Status of the Flink JobManager Kubernetes deployment.

| Value | Docs |
| ----- | ---- |
| READY | JobManager is running and ready to receive REST API calls. |
| DEPLOYED_NOT_READY | JobManager is running but not ready yet to receive REST API calls. |
| DEPLOYING | JobManager process is starting up. |
| MISSING | JobManager deployment not found, probably not started or killed by user. |
| ERROR | Deployment in terminal error, requires spec change for reconciliation to continue. |

### JobStatus
**Class**: org.apache.flink.kubernetes.operator.api.status.JobStatus

**Description**: Last observed status of the Flink job within an application deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jobName | java.lang.String | Name of the job. |
| jobId | java.lang.String | Flink JobId of the Job. |
| state | java.lang.String | Last observed state of the job. |
| startTime | java.lang.String | Start time of the job. |
| updateTime | java.lang.String | Update time of the job. |
| savepointInfo | org.apache.flink.kubernetes.operator.api.status.SavepointInfo | Information about pending and last savepoint for the job. |
| checkpointInfo | org.apache.flink.kubernetes.operator.api.status.CheckpointInfo | Information about pending and last checkpoint for the job. |

### ReconciliationState
**Class**: org.apache.flink.kubernetes.operator.api.status.ReconciliationState

**Description**: Current state of the reconciliation.

| Value | Docs |
| ----- | ---- |
| DEPLOYED | The lastReconciledSpec is currently deployed. |
| UPGRADING | The spec is being upgraded. |
| ROLLING_BACK | In the process of rolling back to the lastStableSpec. |
| ROLLED_BACK | Rolled back to the lastStableSpec. |

### Savepoint
**Class**: org.apache.flink.kubernetes.operator.api.status.Savepoint

**Description**: Represents information about a finished savepoint.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| timeStamp | long | Millisecond timestamp at the start of the savepoint operation. |
| location | java.lang.String | External pointer of the savepoint can be used to recover jobs. |
| triggerType | org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType | Savepoint trigger mechanism. |
| formatType | org.apache.flink.kubernetes.operator.api.status.SavepointFormatType | Savepoint format. |
| triggerNonce | java.lang.Long | Nonce value used when the savepoint was triggered manually {@link SnapshotTriggerType#MANUAL}, null for other types of savepoints. |

### SavepointFormatType
**Class**: org.apache.flink.kubernetes.operator.api.status.SavepointFormatType

**Description**: Savepoint format type.

| Value | Docs |
| ----- | ---- |
| CANONICAL | A canonical, common for all state backends format. |
| NATIVE | A format specific for the chosen state backend. |
| UNKNOWN | Savepoint format unknown, if the savepoint was not triggered by the operator. |

### SavepointInfo
**Class**: org.apache.flink.kubernetes.operator.api.status.SavepointInfo

**Description**: Stores savepoint related information.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| lastSavepoint | org.apache.flink.kubernetes.operator.api.status.Savepoint | Last completed savepoint by the operator. |
| triggerId | java.lang.String | Trigger id of a pending savepoint operation. |
| triggerTimestamp | java.lang.Long | Trigger timestamp of a pending savepoint operation. |
| triggerType | org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType | Savepoint trigger mechanism. |
| formatType | org.apache.flink.kubernetes.operator.api.status.SavepointFormatType | Savepoint format. |
| savepointHistory | java.util.List<org.apache.flink.kubernetes.operator.api.status.Savepoint> | List of recent savepoints. |
| lastPeriodicSavepointTimestamp | long | Trigger timestamp of last periodic savepoint operation. |

### SnapshotTriggerType
**Class**: org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType

**Description**: Snapshot trigger mechanism.

| Value | Docs |
| ----- | ---- |
| MANUAL | Snapshot manually triggered by changing a triggerNonce. |
| PERIODIC | Snapshot periodically triggered by the operator. |
| UPGRADE | Snapshot triggered during stateful upgrade. |
| UNKNOWN | Snapshot trigger mechanism unknown, such as savepoint retrieved directly from Flink job. |

### TaskManagerInfo
**Class**: org.apache.flink.kubernetes.operator.api.status.TaskManagerInfo

**Description**: Last observed status of the Flink job within an application deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| labelSelector | java.lang.String | TaskManager label selector. |
| replicas | int | Number of TaskManager replicas if defined in the spec. |
