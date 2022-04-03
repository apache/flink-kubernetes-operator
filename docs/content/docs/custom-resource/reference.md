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
**Class**: org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec

**Description**: Spec that describes a Flink application or session cluster deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| image | java.lang.String | Flink docker image used to start the Job and TaskManager pods. |
| imagePullPolicy | java.lang.String | Image pull policy of the Flink docker image. |
| serviceAccount | java.lang.String | Kubernetes service used by the Flink deployment. |
| flinkVersion | org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion | Flink image version. |
| ingress | org.apache.flink.kubernetes.operator.crd.spec.IngressSpec | Ingress specs. |
| flinkConfiguration | java.util.Map<java.lang.String,java.lang.String> | Flink configuration overrides for the Flink deployment. |
| podTemplate | io.fabric8.kubernetes.api.model.Pod | Base pod template for job and task manager pods. Can be overridden by the jobManager and  taskManager pod templates. |
| jobManager | org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec | JobManager specs. |
| taskManager | org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec | TaskManager specs. |
| job | org.apache.flink.kubernetes.operator.crd.spec.JobSpec | Job specification for application deployments. Null for session clusters. |
| restartNonce | java.lang.Long | Nonce used to manually trigger restart for the cluster. In order to trigger restart, change  the number to anything other than the current value. |
| logConfiguration | java.util.Map<java.lang.String,java.lang.String> | Log configuration overrides for the Flink deployment. Format logConfigFileName ->  configContent. |

### FlinkSessionJobSpec
**Class**: org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec

**Description**: Spec that describes a Flink session job.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| clusterId | java.lang.String | The cluster id of the target session cluster. When deployed using the operator the cluster id  is the name of the deployment. |
| job | org.apache.flink.kubernetes.operator.crd.spec.JobSpec | A specification of a job . |

### FlinkVersion
**Class**: org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion

**Description**: Enumeration for supported Flink versions.

| Value | Docs |
| ----- | ---- |
| v1_14 |  |
| v1_15 |  |
| v1_16 |  |

### IngressSpec
**Class**: org.apache.flink.kubernetes.operator.crd.spec.IngressSpec

**Description**: Ingress spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| template | java.lang.String | Ingress template for the JobManager service. |
| className | java.lang.String | Ingress className for the Flink deployment. |
| annotations | java.util.Map<java.lang.String,java.lang.String> | Ingress annotations. |

### JobManagerSpec
**Class**: org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec

**Description**: JobManager spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| resource | org.apache.flink.kubernetes.operator.crd.spec.Resource | Resource specification for the JobManager pods. |
| replicas | int | Number of JobManager replicas. Must be 1 for non-HA deployments. |
| podTemplate | io.fabric8.kubernetes.api.model.Pod | JobManager pod template. It will be merged with FlinkDeploymentSpec.podTemplate. |

### JobSpec
**Class**: org.apache.flink.kubernetes.operator.crd.spec.JobSpec

**Description**: Flink job spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jarURI | java.lang.String | URI of the job jar within the Flink docker container. For example: Example:  local:///opt/flink/examples/streaming/StateMachineExample.jar |
| parallelism | int | Parallelism of the Flink job. |
| entryClass | java.lang.String | Fully qualified main class name of the Flink job. |
| args | java.lang.String[] | Arguments for the Flink job main class. |
| state | org.apache.flink.kubernetes.operator.crd.spec.JobState | Desired state for the job. |
| savepointTriggerNonce | java.lang.Long | Nonce used to manually trigger savepoint for the running job. In order to trigger a  savepoint, change the number to anything other than the current value. |
| initialSavepointPath | java.lang.String | Savepoint path used by the job the first time it is deployed. Upgrades/redeployments will not  be affected. |
| upgradeMode | org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode | Upgrade mode of the Flink job. |

### JobState
**Class**: org.apache.flink.kubernetes.operator.crd.spec.JobState

**Description**: Enum describing the desired job state.

| Value | Docs |
| ----- | ---- |
| RUNNING | Job is expected to be processing data. |
| SUSPENDED | Processing is suspended with the intention of continuing later. |

### Resource
**Class**: org.apache.flink.kubernetes.operator.crd.spec.Resource

**Description**: Resource spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| cpu | double | Amount of CPU allocated to the pod. |
| memory | java.lang.String | Amount of memory allocated to the pod. Example: 1024m, 1g |

### TaskManagerSpec
**Class**: org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec

**Description**: TaskManager spec.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| resource | org.apache.flink.kubernetes.operator.crd.spec.Resource | Resource specification for the TaskManager pods. |
| podTemplate | io.fabric8.kubernetes.api.model.Pod | TaskManager pod template. It will be merged with FlinkDeploymentSpec.podTemplate. |

### UpgradeMode
**Class**: org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode

**Description**: Enum to control Flink job upgrade behavior.

| Value | Docs |
| ----- | ---- |
| SAVEPOINT | Job is upgraded by first taking a savepoint of the running job, shutting it down and  restoring from the savepoint. |
| LAST_STATE | Job is upgraded using any latest checkpoint or savepoint available. |
| STATELESS | Job is upgraded with empty state. |

## Status

### FlinkDeploymentStatus
**Class**: org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus

**Description**: Last observed status of the Flink deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jobStatus | org.apache.flink.kubernetes.operator.crd.status.JobStatus | Last observed status of the Flink job on Application deployments. |
| jobManagerDeploymentStatus | org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus | Last observed status of the JobManager deployment. |
| reconciliationStatus | org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus | Status of the last reconcile operation. |

### FlinkSessionJobReconciliationStatus
**Class**: org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobReconciliationStatus

**Description**: Status of the last reconcile step for the session job.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| success | boolean | True if last reconciliation step was successful. |
| error | java.lang.String | If success == false, error information about the reconciliation failure. |
| lastReconciledSpec | org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec | Last reconciled job spec. Used to decide whether further reconciliation steps are necessary. |

### FlinkSessionJobStatus
**Class**: org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus

**Description**: Last observed status of the Flink Session job.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jobStatus | org.apache.flink.kubernetes.operator.crd.status.JobStatus | Last observed status of the job. |
| reconciliationStatus | org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobReconciliationStatus | Status of the last reconcile operation. |

### JobManagerDeploymentStatus
**Class**: org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus

**Description**: Status of the Flink JobManager Kubernetes deployment.

| Value | Docs |
| ----- | ---- |
| READY | JobManager is running and ready to receive REST API calls. |
| DEPLOYED_NOT_READY | JobManager is running but not ready yet to receive REST API calls. |
| DEPLOYING | JobManager process is starting up. |
| MISSING | JobManager deployment not found, probably not started or killed by user. |
| ERROR | Deployment in terminal error, requires spec change for reconciliation to continue. |

### JobStatus
**Class**: org.apache.flink.kubernetes.operator.crd.status.JobStatus

**Description**: Last observed status of the Flink job within an application deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| jobName | java.lang.String | Name of the job. |
| jobId | java.lang.String | Flink JobId of the Job. |
| state | java.lang.String | Last observed state of the job. |
| startTime | java.lang.String | Start time of the job. |
| updateTime | java.lang.String | Update time of the job. |
| savepointInfo | org.apache.flink.kubernetes.operator.crd.status.SavepointInfo | Information about pending and last savepoint for the job. |

### ReconciliationStatus
**Class**: org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus

**Description**: Status of the last reconcile step for the deployment.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| success | boolean | True if last reconciliation step was successful. |
| error | java.lang.String | If success == false, error information about the reconciliation failure. |
| lastReconciledSpec | org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec | Last reconciled deployment spec. Used to decide whether further reconciliation steps are  necessary. |

### Savepoint
**Class**: org.apache.flink.kubernetes.operator.crd.status.Savepoint

**Description**: Represents information about a finished savepoint.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| timeStamp | long | Millisecond timestamp at the start of the savepoint operation. |
| location | java.lang.String | External pointer of the savepoint can be used to recover jobs. |

### SavepointInfo
**Class**: org.apache.flink.kubernetes.operator.crd.status.SavepointInfo

**Description**: Stores savepoint related information.

| Parameter | Type | Docs |
| ----------| ---- | ---- |
| lastSavepoint | org.apache.flink.kubernetes.operator.crd.status.Savepoint | Last completed savepoint by the operator. |
| triggerId | java.lang.String | Trigger id of a pending savepoint operation. |
| triggerTimestamp | java.lang.Long | Trigger timestamp of a pending savepoint operation. |
