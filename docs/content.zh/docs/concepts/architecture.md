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

Flink Kubernetes Operator (Operator) acts as a control plane to manage the complete deployment lifecycle of Apache Flink applications. The Operator can be installed on a Kubernetes cluster using [Helm](https://helm.sh). In most production environments it is typically deployed in a designated namespace and controls Flink deployments in one or more managed namespaces. The custom resource definition (CRD) that describes the schema of a `FlinkDeployment` is a cluster wide resource. For a CRD, the declaration must be registered before any resources of that CRDs kind(s) can be used, and the registration process sometimes takes a few seconds.

{{< img src="/img/concepts/architecture.svg" alt="Flink Kubernetes Operator Architecture" >}}
> Note: There is no support at this time for [upgrading or deleting CRDs using Helm](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).

## Control Loop
The Operator follow the Kubernetes principles, notably the [control loop](https://kubernetes.io/docs/concepts/architecture/controller/):

{{< img src="/img/concepts/control_loop.svg" alt="Control Loop" >}}

Users can interact with the operator using the Kubernetes command-line tool, [kubectl](https://kubernetes.io/docs/tasks/tools/). The Operator continuously tracks cluster events relating to the `FlinkDeployment` and `FlinkSessionJob` custom resources. When the operator receives a new resource update, it will take action to adjust the Kubernetes cluster to the desired state as part of its reconciliation loop. The initial loop consists of the following high-level steps:

1. User submits a `FlinkDeployment`/`FlinkSessionJob` custom resource(CR) using `kubectl`
2. Operator observes the current status of the Flink resource (if previously deployed)
3. Operator validates the submitted resource change
4. Operator reconciles any required changes and executes upgrades

The CR can be (re)applied on the cluster any time. The Operator makes continuous adjustments to imitate the desired state until the current state becomes the desired state. All lifecycle management operations are realized using this very simple principle in the Operator.

The Operator is built with the [Java Operator SDK](https://github.com/java-operator-sdk/java-operator-sdk) and uses the [Native Kubernetes Integration](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/) for launching Flink deployments and submitting jobs under the hood. The Java Operator SDK is a higher level framework and related tooling to support writing Kubernetes Operators in Java. Both the Java Operator SDK and Flink's native kubernetes integration itself is using the [Fabric8 Kubernetes Client](https://github.com/fabric8io/kubernetes-client) to interact with the Kubernetes API Server.

## Flink Resource Lifecycle

The Operator manages the lifecycle of Flink resources. The following chart illustrates the different possible states and transitions:

{{< img src="/img/concepts/resource_lifecycle.svg" alt="Flink Resource Lifecycle" >}}

**We can distinguish the following states:**

  - CREATED : The resource was created in Kubernetes but not yet handled by the operator
  - SUSPENDED : The (job) resource has been suspended
  - UPGRADING : The resource is suspended before upgrading to a new spec
  - DEPLOYED : The resource is deployed/submitted to Kubernetes, but it's not yet considered to be stable and might be rolled back in the future
  - STABLE : The resource deployment is considered to be stable and won't be rolled back
  - ROLLING_BACK : The resource is being rolled back to the last stable spec
  - ROLLED_BACK : The resource is deployed with the last stable spec
  - FAILED : The job terminally failed

## Admission Control

In addition to compiled-in admission plugins, a custom admission plugin named Flink Kubernetes Operator Webhook (Webhook)
can be started as extension and run as webhook.

The Webhook follow the Kubernetes principles, notably the [dynamic admission control](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/).

It's deployed by default when the Operator is installed on a Kubernetes cluster using [Helm](https://helm.sh).
Please see further details how to deploy the Operator/Webhook [here](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/#deploying-the-operator).

The Webhook is using TLS protocol for communication by default. It automatically loads/re-loads keystore file when the file
has changed and provides the following endpoints:

{{< img src="/img/concepts/webhook.svg" alt="Webhook" >}}
