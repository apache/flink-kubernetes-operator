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

The core responsibility of the Flink operator is to manage the full production lifecycle of Flink applications.

What is covered:
 - Running, suspending and deleting applications
 - Stateful and stateless application upgrades
 - Triggering and managing savepoints
 - Handling errors, rolling-back broken upgrades

The behaviour is always controlled by the respective configuration fields of the `JobSpec` object as introduced in the [FlinkDeployment/FlinkSessionJob overview]({{< ref "docs/custom-resource/overview" >}}).

Let's take a look at these operations in detail.

{{< hint info >}}
The management features detailed in this section apply (in most part) to both `FlinkDeployment` (application clusters) and `FlinkSessionJob` (session job) deployments.
{{< /hint >}}

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

As you can see there is no cancelled or deleted among the possible desired states. When users no longer wish to process data with a given FlinkDeployment they can delete the deployment object using the Kubernetes api:

```
kubectl delete flinkdeployment my-deployment
```

{{< hint danger >}}
Deleting a deployment will remove all checkpoint and status information. Future deployments will from an empty state unless manually overridden by the user.
{{< /hint >}}

## Stateful and stateless application upgrades

When the spec changes for FlinkDeployment and FlinkSessionJob resources, the running application must be upgraded.
In order to do this the operator will stop the currently running job (unless already suspended) and redeploy it using the latest spec and state carried over from the previous run for stateful applications.

Users have full control on how state should be managed when stopping and restoring stateful applications using the `upgradeMode` setting of the JobSpec.

Supported values: `stateless`, `savepoint`, `last-state`

The `upgradeMode` setting controls both the stop and restore mechanisms as detailed in the following table:

|                        | Stateless               | Last State                                 | Savepoint                              |
|------------------------|-------------------------|--------------------------------------------|----------------------------------------|
| Config Requirement     | None                    | Checkpointing & Kubernetes HA Enabled      | Checkpoint/Savepoint directory defined |
| Job Status Requirement | None                    | HA metadata available                      | Job Running*                           |
| Suspend Mechanism      | Cancel / Delete         | Delete Flink deployment (keep HA metadata) | Cancel with savepoint                  |
| Restore Mechanism      | Deploy from empty state | Recover last state using HA metadata       | Restore From savepoint                 |
| Production Use         | Not recommended         | Recommended                                | Recommended                            |


*\* When Kubernetes HA is enabled the `savepoint` upgrade mode may fall back to the `last-state` behaviour in cases where the job is in an unhealthy state.*

The three upgrade modes are intended to support different scenarios:

 1. **stateless**: Stateless application upgrades from empty state
 2. **last-state**: Quick upgrades in any application state (even for failing jobs), does not require a healthy job as it always uses the latest checkpoint information. Manual recovery may be necessary if HA metadata is lost.
 3. **savepoint**: Use savepoint for upgrade, providing maximal safety and possibility to serve as backup/fork point. The savepoint will be created during the upgrade process. Note that the Flink job needs to be running to allow the savepoint to get created. If the job is in an unhealthy state, the last checkpoint will be used (unless `kubernetes.operator.job.upgrade.last-state-fallback.enabled` is set to `false`). If the last checkpoint is not available, the job upgrade will fail.

During stateful upgrades there are always cases which might require user intervention to preserve the consistency of the application. Please see the [manual Recovery section](#manual-recovery) for details.

Full example using the `last-state` strategy:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: basic-checkpoint-ha-example
spec:
  image: flink:1.16
  flinkVersion: v1_16
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.savepoints.dir: file:///flink-data/savepoints
    state.checkpoints.dir: file:///flink-data/checkpoints
    high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
    high-availability.storageDir: file:///flink-data/ha
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  podTemplate:
    spec:
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

{{< hint warning >}}
Last state upgrade mode is currently only supported for `FlinkDeployments`.
{{< /hint >}}

### Application restarts without spec change

There are cases when users would like to restart the Flink deployments to deal with some transient problem.

For this purpose you can use the `restartNonce` top level field in the spec. Set a different value to this field to trigger a restart.

```yaml
 spec:
    ...
    restartNonce: 123
```

Restarts work exactly the same way as other application upgrades and follow the semantics detailed in the previous section.

## Savepoint management

Savepoints are triggered automatically by the system during the upgrade process as we have seen in the previous sections.

For backup, job forking and other purposes savepoints can be triggered manually or periodically by the operator, however generally speaking these will not be used during upgrades and are not required for the correct operation.

### Manual Savepoint Triggering

Users can trigger savepoints manually by defining a new (different/random) value to the variable `savepointTriggerNonce` in the job specification:

```yaml
 job:
    ...
    savepointTriggerNonce: 123
```

Changing the nonce value will trigger a new savepoint. Information about pending and last savepoint is stored in the resource status.

### Periodic Savepoint Triggering

The operator also supports periodic savepoint triggering through the following config option which can be configured on a per job level:

```yaml
 flinkConfiguration:
    ...
    kubernetes.operator.periodic.savepoint.interval: 6h
```

There is no guarantee on the timely execution of the periodic savepoints as they might be delayed by unhealthy job status or other interfering user operation.

### Savepoint History

The operator automatically keeps track of the savepoint history triggered by upgrade or manual savepoint operations.
This is necessary so cleanup can be performed by the operator for old savepoints.

Users can control the cleanup behaviour by specifying a maximum age and maximum count for the savepoints in the history.

```
kubernetes.operator.savepoint.history.max.age: 24 h
kubernetes.operator.savepoint.history.max.count: 5
```

{{< hint info >}}
Savepoint cleanup happens lazily and only when the application is running.
It is therefore very likely that savepoints live beyond the max age configuration.  
{{< /hint >}}

## Recovery of missing job deployments

When Kubernetes HA is enabled, the operator can recover the Flink cluster deployments in cases when it was accidentally deleted
by the user or some external process. Deployment recovery can be turned off in the configuration by setting `kubernetes.operator.jm-deployment-recovery.enabled` to `false`, however it is recommended to keep this setting on the default `true` value.

This is not something that would usually happen during normal operation and can also indicate a deeper problem,
therefore an Error event is also triggered by the system when it detects a missing deployment.

One scenario which could lead to a loss of jobmanager deployment in Flink versions before 1.15 is the job entering a terminal state:
 - Fatal job error
 - Job Finished
 - Loss of operator process after triggering savepoint shutdown but before recording the status

In Flink version before 1.15 a terminal job state leads to a deployment shutdown, therefore it's impossible for the operator to know what happened.
In these cases no recovery will be performed to avoid dataloss and an error will be thrown.

Please check the [manual Recovery section](#manual-recovery) to understand how to recover from these situations.

## Restart of unhealthy job deployments

When Kubernetes HA is enabled, the operator can restart the Flink cluster deployments in cases when it was considered
unhealthy. Unhealthy deployment restart can be turned on in the configuration by setting `kubernetes.operator.cluster.health-check.enabled` to `true` (default: `false`).  
In order this feature to work one must enable [recovery of missing job deployments](#recovery-of-missing-job-deployments).

At the moment deployment is considered unhealthy when Flink's restarts count reaches `kubernetes.operator.cluster.health-check.restarts.threshold` (default: `64`)
within time window of `kubernetes.operator.cluster.health-check.restarts.window` (default: 2 minutes).

## Restart failed job deployments

The operator can restart a failed Flink job. This could be useful in cases when the job main task is
able to reconfigure the job to handle these failures.

When `kubernetes.operator.job.restart.failed` is set to `true` then at the moment when the job status is
set to `FAILED` the kubernetes operator will delete the current job and redeploy the job using the
latest successful checkpoint.

## Application upgrade rollbacks (Experimental)

The operator supports upgrade rollbacks as an experimental feature.
The rollback feature works based on the concept of stable deployments specs.

When an application is upgraded, the new spec is initially considered unstable. Once the operator successfully observes the new job in a healthy running state, the spec is marked as stable.
If a new upgrade is not marked stable within a certain configurable time period (`kubernetes.operator.deployment.readiness.timeout`) then a rollback operation will be performed, rolling back to the last stable spec.

To enable rollbacks you need to set:
```
kubernetes.operator.deployment.rollback.enabled: true
```

Kubernetes HA is currently required for the rollback functionality.

Applications are never rolled back to a previous running state if they were suspended before the upgrade.
In these cases no rollback will be performed.

### Stability condition

Currently, a new job is marked stable as soon as the operator could observe it running. This allows us to detect obvious errors, but it's not always enough to detect more complex issues.
In the future we expect to introduce more sophisticated conditions.

{{< hint warning >}}
Rollback is currently only supported for `FlinkDeployments`.
{{< /hint >}}

## Manual Recovery

There are cases when manual intervention is required from the user to recover a Flink application deployment.

In most of these situations the main reason for this is that the deployment got into a state where the operator cannot determine the health of the application or the latest checkpoint information to be used for recovery.
While these cases are not common, we need to be prepared to handle them.

Fortunately almost any issue can be recovered by the user manually by using the following steps:

 1. Locate the latest checkpoint/savepoint metafile in your configured checkpoint/savepoint directory.
 2. Delete the `FlinkDeployment` resource for your application
 3. Check that you have the current savepoint, and that your `FlinkDeployment` is deleted completely
 4. Modify your `FlinkDeployment` JobSpec and set the `initialSavepointPath` to your last checkpoint location
 5. Recreate the deployment

These steps ensure that the operator will start completely fresh from the user defined savepoint path and can hopefully fully recover.
Keep an eye on your job to see what could have cause the problem in the first place.

{{< hint info >}}
The main idea behind the recovery process is that the user needs to manually override the target checkpoint/savepoint location because it is not known to the operator.
The only way to do this is to delete the previous deployment resource fully, and recreate it with `initialSavepointPath` set.

The `initialSavepointPath` setting only takes effect on the first deployment and the operator takes over checkpoint management after that.
{{< /hint >}}
