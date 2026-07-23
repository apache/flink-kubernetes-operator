---
title: "Blue/Green Deployments"
weight: 3
type: docs
aliases:
- /concepts/bluegreen-controller-flow.html
- /docs/concepts/bluegreen-controller-flow/
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

# Blue/Green Deployments

{{< hint warning >}}
Blue/green deployments are an experimental feature. The API and the behavior described on this page may still change between releases.
{{< /hint >}}

A `FlinkBlueGreenDeployment` runs a Flink application as two alternating deployments, blue and green, and upgrades it without stopping the pipeline, as described under [Zero-Downtime Upgrades]({{< ref "docs/concepts/zero-downtime-upgrades" >}}). Beyond the uninterrupted processing itself it provides:

- **State preservation**: the application state is carried across versions through savepoints.
- **Safe rollback capability**: the previous version keeps running until the new one proves itself, and a failed upgrade is aborted without touching the serving deployment.
- **Validation before the switch**: a new deployment becomes active only after it has been observed running and stable.
## Requirements

- **Application Mode**: blue/green deployments manage application clusters, so the template must define a `spec.job`, as described under [Application Mode]({{< ref "docs/deployment/overview#application-mode" >}}).
- **State handoff**: for every upgrade mode except `stateless` the transition hands the state over through a savepoint, so checkpointing and a savepoint directory (`state.savepoints.dir`) must be configured just like for regular [stateful upgrades]({{< ref "docs/managing/job-management#upgrades" >}}). This currently applies to `last-state` as well.

## Creating a Blue/Green Deployment

Converting an existing `FlinkDeployment` requires three changes to the resource definition:

{{< img src="/img/managing/BlueGreenConfigurationQuickstart.png" alt="Blue/Green migration quick start" >}}

1. **Change the resource kind** from `FlinkDeployment` to `FlinkBlueGreenDeployment`.
2. **Optionally add a `configuration` block** at the top level of the spec with the blue/green specific settings, listed further down under Configuration.
3. **Wrap the existing spec in a template**: everything that used to live under `spec` moves under `spec.template.spec`, unchanged.

A minimal resource looks like this:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  name: basic-bg-example
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: "10 min"
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: "15s"
    kubernetes.operator.bluegreen.deployment-deletion.delay: "0ms"
  template:
    spec:
      image: flink:1.20
      flinkVersion: v1_20
      flinkConfiguration:
        taskmanager.numberOfTaskSlots: "2"
        execution.checkpointing.interval: "10 s"
        state.checkpoints.dir: s3://flink-data/checkpoints
        state.savepoints.dir: s3://flink-data/savepoints
      serviceAccount: flink
      jobManager:
        resources:
          requests:
            memory: "2048Mi"
            cpu: "1"
      taskManager:
        resources:
          requests:
            memory: "2048Mi"
            cpu: "1"
      job:
        jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
        parallelism: 2
        upgradeMode: savepoint
```

From here on the resource behaves as described on the concepts page: the first deployment comes up as blue, and every later spec change transitions the application to the other color.

## Deployment States

The current phase of a blue/green resource is reported in `status.blueGreenState`:

| State                    | Meaning                                                                    |
|--------------------------|----------------------------------------------------------------------------|
| `INITIALIZING_BLUE`      | First deployment of the resource, the blue deployment is being created     |
| `ACTIVE_BLUE`            | Steady state, the blue deployment is running and serving                   |
| `SAVEPOINTING_BLUE`      | A savepoint is being taken from blue to seed the upcoming green deployment |
| `TRANSITIONING_TO_GREEN` | The green deployment is starting while blue keeps serving                  |
| `ACTIVE_GREEN`           | Steady state, the green deployment is running and serving                  |
| `SAVEPOINTING_GREEN`     | A savepoint is being taken from green to seed the upcoming blue deployment |
| `TRANSITIONING_TO_BLUE`  | The blue deployment is starting while green keeps serving                  |

The savepointing states appear only for stateful upgrade modes, and the resource always settles in one of the two active states:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/managing/bluegreen-deployment-states.svg" alt="Blue/green deployment states and transitions" >}}

A transition is easiest to follow by watching the resource and its children side by side:

```shell
# The blue/green resource and the state of the currently active job
kubectl get flinkbgdep basic-bg-example
kubectl get flinkbgdep basic-bg-example -o jsonpath='{.status.blueGreenState}'

# The child deployments appearing and disappearing during a transition
kubectl get flinkdep
```

How the controller drives these states internally, including the abort and error paths, is documented under [Blue/Green Controller]({{< ref "docs/internals/controllers#blue-green-controller" >}}).

## Spec Change Behavior

Not every spec change causes a transition. The controller compares the desired template against the last reconciled one and reacts proportionally:

| Spec change                                                                                                                                                                                   | Behavior                                                                                                         |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| No effective change to the `spec.template`                                                                                                                                                    | Nothing, the active deployment keeps running                                                                     |
| `job.state` changed between `running` and `suspended`                                                                                                                                         | The active deployment is suspended or resumed in place, no transition                                            |
| `flinkConfiguration` keys under `job.autoscaler.*`, `parallelism.default` or `kubernetes.operator.*`, and job fields such as `upgradeMode`, `initialSavepointPath` or `allowNonRestoredState` | Patched onto the active deployment in place, no transition                                                       |
| `savepointRedeployNonce` changed                                                                                                                                                              | A full transition that restores the new deployment from `initialSavepointPath` instead of taking a new savepoint |
| Any other template change                                                                                                                                                                     | A full blue/green transition                                                                                     |

Changes that arrive while a transition is already running are applied to the incoming deployment, so the transition completes directly with the newest spec. The one exception is suspending: a `job.state: suspended` request during a transition is deferred until the transition completes, and the then-active deployment is suspended afterwards.

## Kubernetes Resources

The resource materializes as up to two child `FlinkDeployment` resources named `<name>-blue` and `<name>-green`, each a complete Flink cluster with the standard object tree shown in the [Deployment Overview]({{< ref "docs/deployment/overview" >}}). In steady state only the active child exists, during transitions both do. The children are owned by the blue/green resource, so deleting it removes both together with everything below them. They are managed entirely by the controller and should not be edited directly, manual changes are overwritten when the controller next reconciles the template.

For reaching the Flink Web UI and REST API there are two ingress layers, both created only while the operator manages ingress resources (`kubernetes.operator.ingress.manage`, enabled by default):

- An ingress defined at the top level of the spec (`spec.ingress`) is owned by the blue/green resource and always routes to the REST service of the currently active deployment.
- An ingress defined inside the template (`spec.template.spec.ingress`) is created per child, with the color prefixed to its ingress template so that the blue and green endpoints do not collide.

Traffic switching happens through the active ingress: when a transition completes, its backend is updated from the old to the new REST service, for example from `basic-bg-example-blue-rest` to `basic-bg-example-green-rest`, and the ingress controller shifts connections to the new target service without downtime. When ingress management is disabled, no ingress is created or updated and traffic management is expected to be handled externally.

## Health Monitoring and Recovery

A new deployment is considered successful when its job reports `RUNNING` and the child resource reaches the `STABLE` lifecycle state ([Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}})). During a transition the controller re-checks these conditions on the rescheduling interval, and the new deployment has the length of the abort grace period to meet them.

If the grace period expires first, the transition is aborted:

- The incoming deployment is suspended and kept for inspection, it is not deleted.
- The previous deployment simply keeps serving, it was never stopped during the transition.
- `status.blueGreenState` returns to the previous active state and the resource status reports the failure.

The abort mechanism ensures that a failed deployment does not disrupt production traffic, while everything needed to investigate what went wrong with the attempted deployment stays in place.

Recovery is then an ordinary next upgrade: inspect the suspended child for the root cause, correct the template, and apply it. The next spec change starts a fresh transition that redeploys the same color from the corrected spec.

## Configuration

Blue/green behavior is configured on the resource itself, in the `configuration` map at the top level of the spec. These options are not read from the operator configuration, and the defaults apply when they are unset:

| Key                                                                | Default | Description                                                                                |
|--------------------------------------------------------------------|---------|--------------------------------------------------------------------------------------------|
| `kubernetes.operator.bluegreen.abort.grace-period`                 | 10 min  | Maximum time the new deployment is given to become stable before the transition is aborted |
| `kubernetes.operator.bluegreen.reconciliation.reschedule-interval` | 15 s    | How often the controller re-checks progress during savepointing and transitions            |
| `kubernetes.operator.bluegreen.deployment-deletion.delay`          | 0 ms    | Extra time the old deployment is kept running after the new one becomes stable             |

The same options are listed with their formal defaults under [Blue/Green Deployment Configuration]({{< ref "docs/deployment/configuration#bluegreen-configuration" >}}).

## State

Everything that drives a transition is kept in the status subresource: the transition state, the last reconciled spec, and the timestamps and savepoint trigger id that gate teardown and abort. The full field list is documented under [FlinkBlueGreenDeployment]({{< ref "docs/custom-resource/overview#flinkbluegreendeployment" >}}), and the two child deployments report their own status like any other resource, as documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

## Metrics

The operator exports blue/green metrics alongside its other resource metrics: how many resources sit in each transition state and job status per namespace, and counters for failed transitions. They are covered under [Metrics]({{< ref "docs/operations/metrics#flinkbluegreendeployment-lifecycle-metrics" >}}).

## Events

The blue/green resource itself does not emit Kubernetes events, failures surface through its status and metrics instead. The two child deployments emit the standard operator events, submissions, upgrades, and errors, catalogued under [Events]({{< ref "docs/operations/events#operator-events" >}}).

## Limitations

- Only application clusters are supported: the template must define a `spec.job`. Session Mode is not supported.
- During the overlap both jobs process and emit the same records, so delivery across the transition is at-least-once. The exactly-once guarantee is not enforced at this point.
- The state handoff is currently always savepoint-based: `last-state` transitions also take a savepoint instead of reusing the latest checkpoint information.
- Suspend requests are deferred while a transition is in progress.
