---
title: "Blue/Green Controller Flow"
weight: 4
type: docs
aliases:
- /concepts/bluegreen-controller-flow.html
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

# Flink Blue/Green Deployment Controller Flow

## Overview

The goal of this page is to provide a deep introduction to the Flink Blue/Green Deployment Controller, explaining the design and control flow for managing stateful Flink applications with zero-downtime deployments.

We will assume familiarity with Flink, Kubernetes, and general Flink Kubernetes Operator concepts.

This document focuses on the ***Why?*** and ***How?*** of the Blue/Green deployment mechanism and its state-based orchestration approach.

### What is Blue/Green Deployment?

Blue/Green deployment is a software release strategy designed to minimize application downtime during updates and provide a reliable rollback mechanism. The core principle involves maintaining two identical production environments, conventionally named "Blue" and "Green," where only one environment serves live traffic at any given time.

In the context of Flink pipelines, this strategy is particularly valuable for stateful streaming applications that require:
- **Zero-downtime upgrades**: Continuous data processing without interruption
- **State preservation**: Maintaining application state across deployments through savepoints
- **Safe rollback capability**: Quick reversion to the previous version if issues arise
- **Validation before traffic switch**: Ensuring the new deployment is running stable before it becomes active

The typical Blue/Green flow works as follows:

1. The **Blue environment** is currently active, serving production traffic
2. A new version is deployed to the **Green environment** while Blue continues running
3. The Green deployment is validated for correctness and health
4. Traffic is switched from Blue to Green
5. Once the transition is successful and stable, the Blue environment is torn down
6. For the next deployment, the roles reverse: Green becomes active and Blue becomes the target for the new deployment

This approach doubles resource requirements temporarily during transitions but provides significant operational benefits in terms of safety and reliability for critical streaming applications.

## Quick Start: Migrating to Blue/Green Deployments

Converting an existing FlinkDeployment to use Blue/Green deployments requires minimal changes to your resource definition. The following diagram illustrates the three key modifications needed:

{{< img src="/img/concepts/BlueGreenConfigurationQuickstart.png" alt="Blue/Green Configuration Quick Start" >}}

### Migration Steps

1. **Change the resource kind**: Update `kind: FlinkDeployment` to `kind: FlinkBlueGreenDeployment`

2. **Add Blue/Green configuration** (optional): At the top level of the spec, add a `configuration` section with Blue/Green specific settings:
   ```yaml
   configuration:
     kubernetes.operator.bluegreen.abort.grace-period: "10 min"
     kubernetes.operator.bluegreen.reconciliation.reschedule-interval: "15s"
     kubernetes.operator.bluegreen.deployment-deletion.delay: "0ms"
   ```
   These settings control the abort grace period for failed deployments (default: 10 minutes), the reconciliation rescheduling interval (default: 15 seconds), and the delay before deleting old deployments after successful transitions (default: 0ms).

3. **Wrap the FlinkDeployment spec in a template**: Move your existing FlinkDeployment `spec` under a `template.spec` structure:
   ```yaml
   template:
     spec:
       # Your original FlinkDeployment spec goes here
       image: docker.com/my_project/my_sample_image:1
       flinkVersion: v1_20
       flinkConfiguration:
         # ... existing configuration
   ```

That's it! The FlinkBlueGreenDeployment will now manage your Flink application using the Blue/Green deployment pattern, automatically handling zero-downtime upgrades through the state machine described below.

## Blue/Green State Machine

The FlinkBlueGreenDeploymentController uses a state machine to orchestrate zero-downtime deployments. The state machine continuously works to reconcile the deployment to one of two terminal/stable states: **ACTIVE_BLUE** or **ACTIVE_GREEN**. These represent the steady states where a single environment is running and serving traffic.

When a deployment change is detected, the state machine transitions through intermediate states to deploy the new version in the inactive environment, validate it, switch traffic, and clean up the old environment.

{{< img src="/img/concepts/Flink_Blue_Green_Deployment_State_Machine.png" alt="Blue/Green State Machine" >}}

### Deployment States

#### Terminal States

These are the stable states where the application remains until a new deployment is requested:

##### ACTIVE_BLUE
Blue deployment serving production traffic. Monitors for spec changes: PATCH_CHILD diffs update Blue in-place, TRANSITION diffs trigger savepointing (if needed) or move directly to TRANSITIONING_TO_GREEN. Can transition to FAILING if deployment becomes unhealthy.

##### ACTIVE_GREEN
Green deployment serving production traffic. Monitors for spec changes: PATCH_CHILD diffs update Green in-place, TRANSITION diffs trigger savepointing (if needed) or move directly to TRANSITIONING_TO_BLUE. Can transition to FAILING if deployment becomes unhealthy.

#### Intermediate States

These states represent transient phases during a deployment transition:

##### INITIALIZING_BLUE
First-time deployment only. Creates initial Blue deployment and transitions to ACTIVE_BLUE on success. Retries in this state if deployment is not ready. Never revisited after successful initialization.

##### SAVEPOINTING_BLUE
Takes savepoint from Blue before transitioning to Green. Triggered when UpgradeMode requires state preservation. Returns to ACTIVE_BLUE once savepoint completes, then proceeds to TRANSITIONING_TO_GREEN. Can fail at trigger or fetch stage.

##### SAVEPOINTING_GREEN
Takes savepoint from Green before transitioning to Blue. Triggered when UpgradeMode requires state preservation. Returns to ACTIVE_GREEN once savepoint completes, then proceeds to TRANSITIONING_TO_BLUE. Can fail at trigger or fetch stage.

##### TRANSITIONING_TO_GREEN
Blue→Green transition in progress. Creates/updates Green deployment (restoring from savepoint if applicable) while Blue continues serving traffic. Monitors Green readiness within grace period. Spec changes during this state apply to Green deployment. Aborts back to ACTIVE_BLUE if Green fails within grace period. Transitions to ACTIVE_GREEN once Green is ready and deletion delay passes.

##### TRANSITIONING_TO_BLUE
Green→Blue transition in progress. Creates/updates Blue deployment (restoring from savepoint if applicable) while Green continues serving traffic. Monitors Blue readiness within grace period. Spec changes during this state apply to Blue deployment. Aborts back to ACTIVE_GREEN if Blue fails within grace period. Transitions to ACTIVE_BLUE once Blue is ready and deletion delay passes.

### State Transition Logic

The state machine follows these transition patterns:

**Initial Deployment:**
```
INITIALIZING_BLUE → ACTIVE_BLUE
```

**Deployment from Blue to Green:**
```
ACTIVE_BLUE → [SAVEPOINTING_BLUE] → TRANSITIONING_TO_GREEN → ACTIVE_GREEN
```

**Deployment from Green to Blue:**
```
ACTIVE_GREEN → [SAVEPOINTING_GREEN] → TRANSITIONING_TO_BLUE → ACTIVE_BLUE
```

The savepointing states (shown in brackets) are conditional and only entered when:
- The deployment change requires state preservation
- A savepoint is explicitly configured or required by the upgrade strategy

During each reconciliation loop, the state machine evaluates the current state and attempts to progress toward the next terminal state (ACTIVE_BLUE or ACTIVE_GREEN) based on the observed conditions and any new deployment requests.

## Controller Architecture

### Core Components

The FlinkBlueGreenDeploymentController is built on the following key components:

#### FlinkBlueGreenDeploymentController
The main controller class that implements the reconciliation entry point for FlinkBlueGreenDeployment resources. It manages event source registration for watching secondary resources (FlinkDeployments and Ingress), coordinates with the StatusRecorder for persistent status updates, and delegates state-specific logic to the appropriate handlers via the BlueGreenStateHandlerRegistry.

#### BlueGreenStateHandlerRegistry
A registry that maps each FlinkBlueGreenDeploymentState to its corresponding state handler implementation. The registry instantiates all state handlers at construction time and provides lookup functionality to retrieve the appropriate handler for the current deployment state.

#### BlueGreenContext
A simplified context object that encapsulates all necessary state and dependencies for Blue/Green deployment state transitions. It provides access to the FlinkBlueGreenDeployment resource, current status, JOSDK context, secondary FlinkDeployment resources (Blue and Green), and the FlinkResourceContextFactory for creating resource-specific contexts.

#### BlueGreenDeploymentService
The consolidated service that orchestrates all Blue/Green deployment operations. It handles deployment initiation and monitoring, savepoint triggering and management, state transitions, and provides utility methods for patching status updates with proper reconciliation scheduling.

#### BlueGreenKubernetesService
A utility service providing Kubernetes resource management operations specific to Blue/Green deployments. It handles FlinkDeployment CRUD operations (create, update, suspend, delete), owner reference management for dependent resources, deployment readiness verification, and FlinkBlueGreenDeployment resource updates.

### Component Interaction Diagram

<!-- TODO: Add component interaction diagram -->
<!-- {{< img src="/img/concepts/bluegreen_components.svg" alt="Blue/Green Component Interaction" >}} -->

## Controller Reconciliation Flow

The core Blue/Green controller flow contains the following logical phases:

1. Initialize or retrieve deployment status
2. Create BlueGreenContext with current state
3. Delegate to appropriate state handler
4. Execute state-specific logic
5. Update status and schedule next reconciliation

### High-Level Reconciliation Steps

<!-- TODO: Describe the reconcile() method flow in detail -->

```java
// Reference: FlinkBlueGreenDeploymentController.reconcile()
```

## State Handler Architecture

### Handler Hierarchy

<!-- TODO: Add handler class hierarchy diagram -->
<!-- {{< img src="/img/concepts/bluegreen_handler_classes.svg" alt="State Handler Class Hierarchy" >}} -->

### BlueGreenStateHandler Interface

The BlueGreenStateHandler interface defines the contract for all state-specific handlers. Each handler implements a handle() method that processes the state-specific logic and returns an UpdateControl to schedule the next reconciliation. Handlers declare which state they support via the getSupportedState() method, enabling the registry to perform lookups.

### AbstractBlueGreenStateHandler

The abstract base class that provides common functionality shared across all state handlers. It maintains a reference to the BlueGreenDeploymentService for performing deployment operations, stores the supported state for each handler, and provides a shared logger for state-specific logging.

### State-Specific Handlers

#### InitializingBlueStateHandler
Handles the initial deployment of a FlinkBlueGreenDeployment resource. This handler creates the first Blue deployment, configures the initial ingress pointing to Blue, and transitions to ACTIVE_BLUE once the deployment is ready. Retries in this state if the initial deployment fails or is not ready.

#### ActiveStateHandler
Manages both ACTIVE_BLUE and ACTIVE_GREEN states through a unified implementation. This handler continuously monitors for spec changes by comparing the current spec with the last reconciled spec. For PATCH_CHILD diffs, it updates the active deployment in-place. For TRANSITION diffs, it determines whether savepointing is needed based on the upgrade mode and transitions to either the savepointing state or directly to the transitioning state.

#### SavepointingStateHandler
Handles both SAVEPOINTING_BLUE and SAVEPOINTING_GREEN states. This handler triggers a savepoint on the active deployment if one hasn't been started, monitors the savepoint progress, and transitions back to the active state once the savepoint completes successfully. Handles savepoint failures at both the trigger and fetch stages, transitioning to FAILING state on errors.

#### TransitioningStateHandler
Manages both TRANSITIONING_TO_GREEN and TRANSITIONING_TO_BLUE states. This handler initiates the new deployment to the inactive environment (restoring from savepoint if applicable), monitors deployment readiness within a configurable grace period, applies any spec changes that occur during the transition to the new deployment, switches traffic by updating the active ingress, and cleans up the old deployment once the deletion delay passes. Aborts back to the previous active state if the new deployment fails within the grace period.

## Kubernetes Resource Management

### Ingress Management

The Blue/Green controller manages a single "active" ingress resource that routes traffic to whichever environment (Blue or Green) is currently serving production traffic.

#### Ingress Configuration
The active ingress is configured based on the FlinkBlueGreenDeploymentSpec.ingress settings and automatically points to the REST service of the currently active FlinkDeployment. The ingress resource is owned by the FlinkBlueGreenDeployment through Kubernetes owner references, ensuring proper lifecycle management and garbage collection.

#### Traffic Switching
Traffic switching is accomplished by updating the active ingress to point to the new deployment's service endpoint. During transitions, the TransitioningStateHandler calls reconcileBlueGreenIngress() to update the ingress backend to the newly deployed environment's REST service (e.g., from blue-deployment-rest to green-deployment-rest). This provides zero-downtime traffic switching as the ingress controller gradually shifts connections to the new target service. If ingress management is disabled via operator configuration, the controller skips ingress updates and expects external traffic management.

### Secondary Resource Tracking

The FlinkBlueGreenDeploymentController watches secondary resources that are owned by the FlinkBlueGreenDeployment to detect changes and trigger reconciliation. Two types of secondary resources are tracked: FlinkDeployment resources (Blue and Green) are always watched using an InformerEventSource configured with owner reference mapping, enabling the controller to reconcile whenever the Blue or Green deployment's status changes. Ingress resources are conditionally watched only when operator ingress management is enabled, allowing the controller to respond to external ingress modifications or ensure ingress state consistency.

## Spec Change Detection and Handling

The Blue/Green deployment controller doesn't proceed on just any spec change. Instead, it uses FlinkBlueGreenDeploymentSpecDiff to analyze the difference between the current and desired specs, returning a BlueGreenDiffType that determines how the controller should respond.

### Spec Difference Types

The BlueGreenDiffType enum defines the following diff types:

#### IGNORE
Returned when the nested FlinkDeploymentSpecs are completely identical. The controller takes no action and remains in the current active state.

#### SUSPEND
Triggered when job.state changes from RUNNING (or null, which defaults to RUNNING) to SUSPENDED. This performs an in-place suspension of the currently active deployment without creating a new Blue/Green deployment.

#### RESUME
Triggered when job.state changes from SUSPENDED back to RUNNING. This resumes the suspended deployment, potentially applying any spec updates that occurred during suspension.

#### SAVEPOINT_REDEPLOY
Triggered when the savepointRedeployNonce field changes. This initiates a full Blue/Green redeploy using the user-specified initialSavepointPath from the spec instead of taking a new savepoint from the running job.

#### PATCH_CHILD
Returned when the FlinkDeploymentSpecs differ but the ReflectiveDiffBuilder returns DiffType.IGNORE. This indicates changes that only affect fields that can be patched in-place on the active FlinkDeployment child resource without requiring a full Blue/Green transition (e.g., parallelism adjustments, resource tuning).

#### TRANSITION
Returned when the ReflectiveDiffBuilder detects significant changes requiring a full Blue/Green transition. Examples include image version changes, major Flink configuration changes, or any modifications that cannot be safely applied in-place. This triggers the full savepointing and transitioning flow.

### Spec Comparison Logic

The FlinkBlueGreenDeploymentSpecDiff.compare() method follows a precedence-based evaluation:

1. **Suspend/Resume Detection (Highest Priority)**: Checks for job.state changes between RUNNING and SUSPENDED, returning SUSPEND or RESUME immediately if detected.

2. **Savepoint Redeploy Detection**: Delegates to ReflectiveDiffBuilder and checks if savepointRedeployNonce changed, returning SAVEPOINT_REDEPLOY if so.

3. **Identity Check**: Compares the nested FlinkDeploymentSpecs for complete equality, returning IGNORE if identical.

4. **ReflectiveDiffBuilder Mapping**: Uses the result from ReflectiveDiffBuilder to determine if changes are in-place patchable (PATCH_CHILD) or require full transition (TRANSITION).

### Last Reconciled Spec Management

The controller tracks the last successfully reconciled spec in the FlinkBlueGreenDeploymentStatus to enable accurate diff detection. This spec is updated after each successful state transition and compared against the desired spec during each reconciliation loop to determine if action is needed.

## Health Monitoring

### Deployment Success Criteria

A deployment is considered successful when it meets both of the following conditions in Kubernetes:
- **JobStatus** is RUNNING
- **ResourceLifecycleState** is STABLE

The controller continuously monitors these conditions during transitions to determine when to complete the Blue/Green switch or trigger abort mechanisms.

## Error Handling and Recovery

### Abort Grace Period

The controller provides an abort mechanism to handle deployment failures during transitions. The abort grace period is configured via the `abort.grace-period` setting (specified in minutes) in the FlinkConfiguration. This defines how long the controller will wait and continuously reconcile for a new deployment to reach the success criteria (RUNNING job status and STABLE lifecycle state).

If the grace period expires without the deployment becoming successful, the controller aborts the transition with the following behavior:
- The unsuccessful new deployment is left running as-is for analysis and debugging
- The previous deployment is preserved and remains as the active deployment
- Traffic continues to be served by the previous deployment
- The state machine transitions back to the previous active state (ACTIVE_BLUE or ACTIVE_GREEN)

This abort mechanism ensures that failed deployments don't disrupt production traffic while allowing operators to investigate what went wrong with the attempted deployment.

## Configuration and Settings

### Blue/Green Specific Configuration

<!-- TODO: Document configuration options -->
- Transition timeouts
- Savepoint settings
- Ingress configuration
- Abort thresholds

### Operator Configuration Integration

<!-- TODO: Describe integration with operator config -->
- Reconciliation intervals
- Resource limits
- Namespace management
