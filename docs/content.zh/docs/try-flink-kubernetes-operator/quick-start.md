---
title: "Quick Start"
weight: 1
type: docs
aliases:
- /try-flink-kubernetes-operator/quick-start.html
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

# Quick Start

This page walks through deploying the operator and an example Flink job on a local Kubernetes installation.

## Prerequisites

The quick start requires local installations of the following, so that the `kubectl` and `helm` commands are available:

1. [Docker](https://docs.docker.com/)
2. [Kubernetes](https://kubernetes.io/)
3. [Helm](https://helm.sh/docs/intro/quickstart/)

For Docker, [Docker Desktop](https://www.docker.com/products/docker-desktop) configured with at least 8GB of RAM is recommended. For Kubernetes, [minikube](https://minikube.sigs.k8s.io/docs/start/) is a good choice, pinned below to v1.28.0, the version the end-to-end tests run against. Start a cluster with:

```shell
minikube start --kubernetes-version=v1.28.0
```

[k9s](https://k9scli.io/) is a convenient terminal UI for Kubernetes, optional for this guide.

## Deploying the Operator

Install [cert-manager](https://cert-manager.io/) to enable the webhook component (needed once per Kubernetes cluster):

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```

{{< hint info >}}
If the cert-manager installation fails, the webhook can be disabled with `--set webhook.create=false` on the helm install command below. The certificate setup and the disable options are covered in detail under [Cert Manager]({{< ref "docs/deployment/helm/cert-manager" >}}).
{{< /hint >}}

Deploy the latest stable operator version using the included Helm chart:

```shell
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-{{< stable >}}{{< version >}}{{< /stable >}}{{< unstable >}}&lt;OPERATOR-VERSION&gt;{{< /unstable >}}/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```

The list of stable versions is available on the Flink [downloads page](https://flink.apache.org/downloads.html).

{{< hint info >}}
The Helm chart points to the `ghcr.io/apache/flink-kubernetes-operator` image repository by default. In case of connectivity issues, or to use DockerHub instead, set `--set image.repository=apache/flink-kubernetes-operator` during installation.
{{< /hint >}}

Verify the installation via `kubectl` and `helm`:

```shell
kubectl get pods
NAME                                        READY   STATUS    RESTARTS   AGE
flink-kubernetes-operator-fb5d46f94-ghd8b   2/2     Running   0          4m21s

helm list
NAME                        NAMESPACE   REVISION   UPDATED                                STATUS     CHART                                       APP VERSION
flink-kubernetes-operator   default     1          2022-03-09 17:39:55.461359 +0100 CET   deployed   flink-kubernetes-operator-{{< version >}}   {{< version >}}
```

## Submitting a Flink Job

With the operator running, submit the example Flink job:

```shell
kubectl create -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/{{< stable_branch >}}/examples/basic.yaml
```

Follow the job logs after startup, which can take about a minute in a fresh environment, seconds afterwards:

```shell
kubectl logs -f deploy/basic-example

INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 206 (type=CHECKPOINT) @ 1647035164458 for job a12c04ac7f5d8418d8ab27931bf517b7.
INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Completed checkpoint 206 for job a12c04ac7f5d8418d8ab27931bf517b7 (28509 bytes, checkpointDuration=7 ms, finalizationTime=0 ms).
```

To expose the Flink Dashboard, add a port-forward rule or see the [Ingress]({{< ref "docs/custom-resource/ingress" >}}) configuration options:

```shell
kubectl port-forward svc/basic-example-rest 8081
```

The Flink Dashboard is then accessible at [localhost:8081](http://localhost:8081/).

To stop the job and delete the `FlinkDeployment`:

```shell
kubectl delete flinkdeployment/basic-example
```

## Built-in Examples

The operator project ships a wide variety of built-in examples showing how to use its functionality. They are maintained as part of the operator repo, in the [examples directory](https://github.com/apache/flink-kubernetes-operator/tree/main/examples), and cover, among others:

- Application, session, and session job submission
- Checkpointing and HA configuration
- Java, SQL, and Python Flink jobs
- Ingress, logging, and metrics configuration
- Advanced operator deployment techniques using Kustomize
