---
title: "Architecture"
weight: 2
type: docs
aliases:
- /concepts/architecture.html
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

# Architecture

The operator runs inside a Kubernetes cluster and is built from a small set of cooperating components. Everything revolves around the Flink custom resources: users declare them, the control plane admits and stores them, and the operator turns them into running Flink clusters.

Following one resource through the system gives the complete picture. A user applies a Flink custom resource with standard Kubernetes tooling. The Kubernetes API server receives the request and, before storing anything, hands the resource to the admission webhook to be mutated and validated. Once admitted, the resource is persisted and the operator is notified of the change. Its reconciliation loop then deploys and manages the Flink cluster the resource describes, reporting progress back into the resource status. The diagram below shows this end to end:

<!-- Editable draw.io source: docs/static/img/concepts/architecture-overview-v2.drawio (the exported SVG below also embeds a copy of the diagram). -->
{{< img src="/img/concepts/architecture-overview.svg" alt="Flink Kubernetes Operator high-level architecture" >}}

At the center of the diagram sit the custom resources, the meeting point of the whole system: users write the desired state into their spec, and the operator answers with the observed state in their status, so neither ever talks to the other directly. Beyond declaring resources, the only other lever users have is configuration, which tunes how the operator and the webhook themselves behave. Every other relationship in the diagram is just as sharply cut: the admission webhook guards the write path and talks only to the API server, and the Flink clusters are managed exclusively by the operator, each running cluster being the realization of a resource stored on the API server.

For everything related to the custom resources themselves, from their overall structure down to every configurable field, see [Custom Resource → Overview]({{< ref "docs/custom-resource/overview" >}}).

For everything related to deploying the operator and the webhook, from the overall deployment architecture down to the concrete setup, see [Deployment → Overview]({{< ref "docs/deployment/overview" >}}).

## Admission Control

In addition to Kubernetes' compiled-in admission plugins, the operator ships its own admission webhook, following the Kubernetes [dynamic admission control](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/) pattern. The webhook intercepts creates and updates of Flink custom resources before they are persisted, so a broken spec is stopped at the door instead of reaching the operator.

<!-- Editable draw.io source: docs/static/img/concepts/admission-control.drawio (the exported SVG below also embeds a copy of the diagram). -->
{{< img src="/img/concepts/admission-control.svg" alt="Admission Control" >}}

Every admission request is delivered by the API server to the webhook's TLS endpoint. The certificate behind the endpoint comes from a keystore file mounted from a Kubernetes Secret, which the webhook watches and hot-reloads whenever it changes, so certificates rotate without a restart. Once received, the request passes through two stages:

1. **Mutate**: the resource may first be adjusted before it is stored, and every adjustment made here flows back to the API server as a patch. The built-in mutation is deliberately minimal, it stamps each session job with a label naming its target session cluster, so all jobs of a cluster can be found efficiently. The stage is extensible with custom mutators.
2. **Validate**: the spec is then checked against the operator's validation rules, from unsupported Flink versions and forbidden configuration keys to inconsistent job settings and disallowed spec transitions. The rule set is extensible with custom validators. A valid spec is admitted, an invalid one is turned away.

A rejected spec is never stored and never reaches the operator: the failed `kubectl` operation reports the reason immediately, and the running deployment is unaffected. Admission is the first line of defense, the operator validates the spec again on every reconciliation pass, so validation holds even when the webhook is disabled.

The webhook is deployed by default with the Helm installation and can be disabled entirely. Its deployment is covered under [Deployment → Overview]({{< ref "docs/deployment/overview" >}}).

## Reconciliation Loop

The operator is built on the [Java Operator SDK](https://javaoperatorsdk.io/) (JOSDK), whose reconciliation loop implements the Kubernetes [control loop](https://kubernetes.io/docs/concepts/architecture/controller/) pattern. JOSDK provides the machinery of the loop: it watches the Flink custom resources and triggers a reconciliation pass for a resource whenever it is created or updated, after failures, and periodically in between, independently for every managed resource. What happens inside each pass is the operator's own logic, and it always moves through the same phases:

<!-- Editable draw.io source: docs/static/img/concepts/control-loop.drawio (the exported SVG below also embeds a copy of the diagram). -->
{{< img src="/img/concepts/reconciliation-loop.svg" alt="Reconciliation Loop" >}}

1. **Observe**: every pass starts by facing reality. The operator queries the running deployment through the Flink REST API and the Kubernetes API and refreshes the resource status with what it finds, the job state, the health of the JobManager, and the progress of pending operations such as savepoints. All later decisions in the pass are made against these fresh facts, and the same facts are what anyone inspecting the resource status sees.
2. **Validate**: before acting on the desired state, the operator re-checks the spec against the same validation rules the admission webhook applies. An invalid spec is reported on the resource and set aside, the pass continues against the last valid one, and the running deployment stays untouched.
3. **Reconcile**: only now does the operator act. The desired spec is diffed against the last successfully reconciled one, and the difference dictates the action: nothing at all when they match, a scale-only adjustment when just the parallelism changed, a full upgrade cycle when the job itself changed, or a rollback when a previous upgrade never became stable. Whatever the action, its outcome is written back into the resource status, which is exactly what the next pass will observe.

The loop then starts again from observation. A resource can be created or re-applied at any time, and the operator keeps adjusting the running deployment until the current state matches the desired state.

For launching Flink clusters and submitting jobs the operator builds on Flink's [Native Kubernetes Integration](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/) in the default [operator deployment mode]({{< ref "docs/deployment/overview#operator-deployment-modes" >}}). Both JOSDK and the native integration use the [Fabric8 Kubernetes Client](https://github.com/fabric8io/kubernetes-client) to talk to the Kubernetes API server.

The loop is the mechanism, and lifecycle management is what it delivers: deployments, upgrades, rollbacks, snapshots, and autoscaling are all outcomes of this one repeating cycle. How they come together over the life of a Flink resource is covered under [Lifecycle Management]({{< ref "docs/concepts/lifecycle-management" >}}).

