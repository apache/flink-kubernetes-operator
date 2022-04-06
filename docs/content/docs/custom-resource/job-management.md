---
title: "Job Management"
weight: 2
type: docs
aliases:
- /custom-resource/job-management.html
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

# Job Lifecycle Management

The core responsibility of the Flink operator is to manage the full production lifecycle of Flink jobs.

What is covered:
 - Running, suspending and deleting applications
 - Stateful and stateless application upgrades
 - Triggering savepoints

The behaviour is always controlled by the respective configuration fields of the `JobSpec` object as introduced in the [FlinkDeployment overview]({{< ref "docs/custom-resource/overview" >}}).

Let's take a look at these operations in detail.

## Running, suspending and deleting applications

By controlling the `state` field of the `JobSpec` users can define the desired state of the application.

Supported application states:
 - `running` : The job is expected to be running and processing data.
 - `suspended` : Data processing should be temporarily suspended, with the intention of continuing later.

**Job State transitions**

There are 4 possible state change scenarios when updating the current FlinkDeployment.

 - `running` -> `running` : Job upgrade operation. In practice, a suspend followed by a restore operation.
 - `running` -> `suspended` : Suspend operation. Stops the job while maintaining state information for stateful applications.
 - `suspended` -> `running` : Restore operation. Start the application from current state using the latest spec.
 - `suspended` -> `suspended` : Update spec, job is not started.

The way state is handled for suspend and restore operations is described in detail in the next section.

**Cancelling/Deleting applications**

As you can see there is no cancelled or deleted among the possible desired states. When users no longer wish to process data with a given FlinkDeployment they can simply delete the deployment object using the Kubernetes api:

```
kubectl delete flinkdeployment my-deployment
```

{{< hint danger >}}
Deleting a deployment will remove all checkpoint and status information. Future deployments will from an empty state unless manually overriden by the user.
{{< /hint >}}

## Stateful and stateless application upgrades

When the spec changes for a FlinkDeployment the running Application or Session cluster must be upgraded.
In order to do this the operator will stop the currently running job (unless already suspended) and redeploy it using the latest spec and state carried over from the previous run for stateful applications.

Users have full control on how state should be managed when stopping and restoring stateful applications using the `upgradeMode` setting of the JobSpec.

Supported values:`stateless`, `savepoint`, `last-state`

The `upgradeMode` setting controls both the stop and restore mechanisms as detailed in the following table:

| | Stateless | Last State | Savepoint |
| ---- | ---------- | ---- | ---- |
| Config Requirement | None | Checkpointing & Kubernetes HA Enabled | Savepoint directory defined |
| Job Status Requirement | None* | None* | Job Running |
| Suspend Mechanism | Cancel / Delete | Delete Flink deployment (keep HA metadata) | Cancel with savepoint |
| Restore Mechanism | Deploy from empty state | Recover last state using HA metadata | Restore From savepoint |

*\*In general no update can be executed while a savepoint operation is in progress*

The three different upgrade modes are intended to support different use-cases:
 - stateless: Stateless applications, prototyping
 - last-state: Suitable for most stateful production applications. Quick upgrades in any application state (even for failing jobs), does not require a healthy job. Requires Flink Kubernetes HA configuration (see example below).
 - savepoint: Suitable for forking, migrating applications. Requires a healthy running job as it requires a savepoint operation before shutdown.

Full example using the `last-state` strategy:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: default
  name: basic-checkpoint-ha-example
spec:
  image: flink:1.14.3
  flinkVersion: v1_14
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.savepoints.dir: file:///flink-data/savepoints
    high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
    high-availability.storageDir: file:///flink-data/ha
  jobManager:
    replicas: 1
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  podTemplate:
    spec:
      serviceAccount: flink
      containers:
        - name: flink-main-container
          volumeMounts:
          - mountPath: /flink-data
            name: flink-volume
      volumes:
      - name: flink-volume
        hostPath:
          # directory location on host
          path: /tmp/flink
          # this field is optional
          type: Directory
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: last-state
    state: running
```

## Savepoint management

Savepoints can be triggered manually by defining a random (nonce) value to the variable `savepointTriggerNonce` in the job specification:

```yaml
 job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: savepoint
    state: running
    savepointTriggerNonce: 123
```

Changing the nonce value will trigger a new savepoint. Information about pending and last savepoint is stored in the FlinkDeployment status.
