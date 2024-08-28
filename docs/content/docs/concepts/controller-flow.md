---
title: "Controller Flow"
weight: 3
type: docs
aliases:
- /concepts/controller-flow.html
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

# Flink Operator Controller Flow

The goal of this page is to provide a deep introduction to the Flink operator logic and provide enough details about the control flow design so that new developers can get started.

We will assume a good level of Flink Kubernetes and general operational experience for different cluster and job types. For Flink related concepts please refer to https://flink.apache.org/.

We will also assume a deep user-level understanding of the Flink Kubernetes Operator itself.

This document is mostly concerned about the ***Why?*** and ***How?*** of the operator mechanism, the user facing ***What?*** is already documented elsewhere.

The core operator controller flow (as implemented in `FlinkDeploymentController` and `FlinkSessionJobController`) contains the following logical phases:

1. Observe the status of the currently deployed resource
2. Validate the new resource spec
3. Reconcile any required changes based on the new spec and the observed status
4. Rinse and repeat

It is very important to note that all these steps are executed every time. Observe and reconciliation is always executed regardless of the output of the validation, but validation might affect the target of reconciliation as we will see in the next sections.

## Observation phase

In the observation phase the Observer module is responsible for observing the current (point-in-time) status of any deployed Flink resources (already submitted clusters, jobs) and updated the CR Status fields based on that.

The observer will always work using the previously deployed configuration to ensure that it can access the Flink clusters through the rest api. User configuration can affect the rest clients (port configs etc.) so we must always use the config that is running on the cluster. This is the main reason why the `FlinkConfigManager` and the operator in general distinguishes observe & deploy configuration throughout the implementation.

The observer should never take actions to change or submit new resources, these actions are the responsibility of the Reconciler module as we will see later. The main reason for this separation is that the required actions not only depend on the current cluster state but also on any new spec changes the user might have submitted in the meantime.

**Observer class hierarchy:**

{{< img src="/img/concepts/observer_classes.svg" alt="Observer Class Hierarchy" >}}

### Observer Steps

1. If no resource is deployed we skip observing
2. If we are in `ReconciliationState.UPGRADING` state, we check whether the upgrade already went through.
    Normally the reconciler upgrades the state to `DEPLOYED` after a successful submission, but a transient error could prevent this status update. Therefore we use resource specific logic to check if the resource on the cluster already matches the target upgrade spec (annotations, deterministic job ids).
3. For FlinkDeployments we next observe the status of the cluster Kubernetes resources, JM Deployment and Pods. We do this once after an upgrade or any time we cannot access the Job Rest endpoints. The status is recorded in the `jobManagerDeploymentStatus` field.

    It’s important to note here that trying to observe Jobs or anything through the Flink rest API only makes sense if the JobManager Kubernetes resources are seemingly healthy. Any errors with the JM deployment automatically translate into an error state and should clear out any previously observed running JobStatus. A Job cannot be in running state without a healthy jobmanager.
4. If the cluster is seemingly healthy, we proceed to observing the `JobStatus` (for Application and SessionJob resources). In this phase we query the information using the Jobmanager rest api about the current job state and pending savepoint progress.

    The current state of the job determines what can be observed. For terminal job states (`FAILED, FINISHED`) we also record the last available savepoint information to be used during last-state upgrades. We can only do this in terminal states because otherwise there is no guarantee that by the time of the upgrade a new checkpoint won’t be created.

    If the Job cannot be accessed, we have to check the status of the cluster again (see step 3), or if we cannot find the job on the cluster, the next step will depend on the resource type and config and will either trigger an error or we simply need to wait. When the job’s status cannot be determined we use the `RECONCILING` state.
5. Last step in the observe flow is some administration based on the observed status. If everything is healthy and running, we clear previously recorded errors on the resource status. If the job is not running anymore we clear previous savepoint trigger information so that it may be retriggered in the reconcile phase later.
6. At the end of the observer phase the operator sends the updated resource status to Kubernetes. This is a very important step to avoid losing critical information during the later phases. One example of such state loss: You have a failed/completed job and the observer records the latest savepoint info. The reconciler might decide to delete this cluster for an upgrade, but if at this point the operator should fail, the observer would not be able to record the last savepoint again as it cannot be observed on the deleted cluster anymore. Recording the status before any cluster actions are taken is a critical part of the logic.

### Observing the SavepointInfo

Savepoint information is part of the JobStatus and tracks both pending (either manual or periodic savepoint trigger info) and the savepoint history according to the config. Savepoint info is only updated for running and terminal jobs as seen in the observe steps. If a job is failing, restarting etc that means that the savepoint failed and needs to be retriggered.

We observe pending savepoints using the triggerId and if they were completed we record them to the history. If the history reaches the configured size limit we dispose savepoint through the running job rest api, allowing us to do so without any user storage credentials. If the job is not running or unhealthy, we clear pending savepoint trigger info which essentially aborts the savepoint from the operator’s perspective.

## Validation phase

After the resource was successfully observed and the status updated, we next validate the incoming resource spec field.

If the new spec validation fails, we trigger an error event to the user and we reset the last successfully submitted spec in the resource (we don’t update it in Kubernetes, only locally for the purpose of reconciliation).

This step is very important to ensure that reconciliation runs even if the user submits an incorrect specification, thus allowing the operator to stabilize the previously desired state if any errors happened to the deployed resources in the meantime.

## Reconciliation phase

Last step is the reconciliation phase which will execute any required cluster actions to bring the resource to the last desired (valid) spec. In some cases the desired spec is reached in a single reconcile loop, in others we might need multiple passes as we will see.

It’s very important to understand that the Observer phase records a point-in-time view of the cluster and resources into the status. In most cases this can change at any future time (a running job can fail at any time), in some rare cases it is stable (a terminally failed or completed job will stay that way). Therefore the reconciler logic must always take into account the possibility that the cluster status has already drifted from what is in the status (most of the complications arise from this need).

{{< img src="/img/concepts/reconciler_classes.svg" alt="Reconciler Class Hierarchy" >}}

### Base reconciler steps

The `AbstractFlinkResourceReconciler` encapsulates the core reconciliation flow for all Flink resource types. Let’s take a look at the high level flow before we go into specifics for session, application and session job resources.

1. Check if the resource is ready for reconciliation or if there are any pending operations that should not be interrupted (manual savepoints for example)
2. If this is the first deployment attempt for the resource, we simply deploy it. It’s important to note here that this is the only deploy operation where we use the `initialSavepointPath` provided in the spec.
3. Next we determine if the desired spec changed and the type of change: `IGNORE, SCALE, UPGRADE`. Only for scale and upgrade type changes do we need to execute further reconciliation logic.
4. If we have upgrade/scale spec changes we execute the upgrade logic specific for the resource type
5. If we did not receive any spec change we still have to ensure that the currently deployed resources are fully reconciled:
    1. We check if any previously deployed resources have failed / not stabilized and we have to perform a rollback operation.
    2. We apply any further reconciliation steps specific to the individual resources (trigger savepoints, recover deployments, restart unhealthy clusters etc.)

### A note on deploy operations

We have to take special care around deployment operations as they start clusters and jobs which might immediately start producing data and checkpoints. Therefore it’s critical to be able to recognize when a deployment has succeeded / failed. At the same time, the operator process can fail at any point in time making it difficult to always record the correct status.

To ensure we can always recover the deployment status and what is running on the cluster, the to-be-deployed spec is always written to the status with the `UPGRADING` state before we attempt the deployment. Furthermore an annotation is added to the deployed Kubernetes Deployment resources to be able to distinguish the exact deployment attempt (we use the CR generation for this). For session jobs as we cannot add annotations we use a special way of generating the jobid to contain the same information.

### Base job reconciler

The AbstractJobReconciler is responsible for executing the shared logic for Flink resources that also manage jobs (Application and SessionJob clusters). Here the core part of the logic deals with managing job state and executing stateful job updates in a safe manner.

Depending on the type of Spec change SCALE/UPGRADE the job reconciler has slightly different codepaths. For scale operations, if standalone mode and reactive scaling is enabled, we only need to rescale the taskmanagers. In the future we might also add more efficient rescaling here for other cluster types (such as using the rescale API once implemented in upstream Flink)

If an UPGRADE type change is detected in the spec we execute the job upgrade flow:

1. If the job is currently running, suspend according to the desired (available upgrade mode), more on this later
2. Mark upgrading state in status, and trigger a new reconciliation loop (this allows us to pick up new spec changes mid upgrade as the suspend can take a while)
3. Restore job according to the new spec from either HA metadata or savepoint info recorded in status, or empty state (depending on upgrade mode setting)

#### UpgradeMode and suspend/cancel behaviour

The operator must always respect the upgrade mode setting when it comes to stateful upgrades to avoid data loss. There is however some flexibility in the mechanism to account for unhealthy jobs and to provide extra safeguards during version upgrades. The **getAvailableUpgradeMode** method is an important corner stone in the upgrade logic, and it is used to decide what actualy upgrade mode should be used given the request from the user and current cluster state.

In normal healthy cases, the available upgrade mode will be the same as what the user has in the spec. However, there are some cases where we have to change between savepoint and last-state upgrade mode. Savepoint upgrade mode can only be used if the job is healthy and running, for failing, restarting or otherwise unhealthy deployments, we are allowed to use last-state upgrade mode as long as HA metadata is available (and not explicitly configured otherwise). This allows us to have a robust upgrade flow even if a job failed, while keeping state consistency.

Last-state upgrade mode refers to upgrading using the checkpoint information stored in the HA metadata. HA metadata format may not be compatible when upgrading between Flink minor versions therefore a version change must force savepoint upgrade mode and require a healthy running job.

It is very important to note that we NEVER change from last-state/savepoint upgrade mode to stateless as that would compromise the upgrade logic and lead to state loss.

### Application reconciler

Application clusters have a few more extra config steps compared to session jobs that we need to take care of during deployment/upgrade/cancel operations. Here are some important things that the operator does to ensure robust behaviour:

**setRandomJobResultStorePath**: In order to avoid terminated applications being restarted on JM failover, we have to disable Job result cleanup. This forces us to create a random job result store path for each application deployment as described here: https://issues.apache.org/jira/browse/FLINK-27569 . Users need to manually clean up the jobresultstore later.

**setJobIdIfNecessary**: Flink by default generates deterministic jobids based on the clusterId (which is the CR name in our case). This causes checkpoint path conflicts if the job is ever restarted from an empty state (stateless upgrade). We therefore generate a random jobid to avoid this.

**cleanupTerminalJmAfterTtl**: Flink 1.15 and above we do not automatically terminate jobmanager processes after shutdown/failure to get a robust observer behaviour. However once the terminal state is observed we can clean up the JM process/deployment.

## Custom Flink Resource Status update mechanism

The JOSDK provides built in ways to update resource spec and status for the reconciler implementations however the Flink Operator does not use this and has a custom status update mechanism due to the following reasons.

When using the JOSDK, the status of a CR can only be updated at the end of the reconciliation method. In our case we often need to trigger status updates in the middle of the reconciliation flow to provide maximum robustness. One example of such case is recording the deployment information in the status before executing the deploy action.

Another way to look at it is that the Flink Operator uses the resource status as a write ahead log for many actions to guarantee robustness in case of operator failures.

The status update mechanism is implemented in the StatusRecorder class, which serves both as a cache for the latest status and the updater logic. We need to always update our CR status from the cache in the beginning of the controller flow as we are bypassing the JOSDK update-mechanism/caches which can cause old status instances to be returned. For the actual status update we use a modified optimistic locking mechanism which only updates the status if the status has not been externally modified in the meantime.

Under normal circumstances this assumption holds as the operator is the sole owner/updater of the status. Exceptions here might indicate that the user tampered with the status or another operator instance might be running at the same time managing the same resources, which can lead to serious issues.

## JOSDK vs Operator interface naming conflicts

In JOSDK world the Reconciler interface represents the whole control/reconciliation flow. In our case we call these classes controllers and reserve our own Reconciler interface to a specific part of this controller flow.

Historically the JOSDK also called Reconciler interface Controller.
