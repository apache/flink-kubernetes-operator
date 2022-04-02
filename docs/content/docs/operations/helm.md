---
title: "Helm"
weight: 1
type: docs
aliases:
- /operations/helm.html
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

# Helm installation

The operator installation is managed by a helm chart. To install run:

```
helm install flink-kubernetes-operator helm/flink-kubernetes-operator
```

Alternatively to install the operator (and also the helm chart) to a specific namespace:

```
helm install flink-kubernetes-operator helm/flink-kubernetes-operator --namespace flink --create-namespace
```

Note that in this case you will need to update the namespace in the examples accordingly or the `default`
namespace to the [watched namespaces](#watching-only-specific-namespaces).

## Overriding configuration parameters during Helm install

Helm provides different ways to override the default installation parameters (contained in `values.yaml`) for the Helm chart.

To override single parameters you can use `--set`, for example:
```
helm install --set image.repository=apache/flink-kubernetes-operator --set image.tag=1.0.1 flink-kubernetes-operator helm/flink-kubernetes-operator
```

You can also provide your custom values file by using the `-f` flag:
```
helm install -f myvalues.yaml flink-kubernetes-operator helm/flink-kubernetes-operator
```

For more information check the [Helm documentation](https://helm.sh/docs/helm/helm_install/).

## Validating webhook

In order to use the webhook for FlinkDeployment validation, you must install the cert-manager on the Kubernetes cluster:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.7.1/cert-manager.yaml
```

The webhook can be disabled during helm install by passing the `--set webhook.create=false` parameter or editing the `values.yaml` directly.

## Watching only specific namespaces

The operator supports watching a specific list of namespaces for FlinkDeployment resources. You can enable it by setting the `--set watchNamespaces={flink-test}` parameter.
When this is enabled role-based access control is only created specifically for these namespaces for the operator and the jobmanagers, otherwise it defaults to cluster scope.

## Working with Argo CD

If you are using [Argo CD](https://argoproj.github.io) to manage the operator, you will encounter the issue which complains the CRDs too long. Same with [this issue](https://github.com/prometheus-operator/prometheus-operator/issues/4439).
The recommended solution is to split the operator into two Argo apps, such as:

* The first app just for installing the CRDs with `Replace=true` directly, snippet:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flink-kubernetes-operator-crds
spec:
  source:
    repoURL: https://github.com/apache/flink-kubernetes-operator
    targetRevision: main
    path: helm/flink-kubernetes-operator/crds
    syncOptions:
    - Replace=true
...
```

* The second app that installs the Helm chart with `skipCrds=true` (new feature in Argo CD 2.3.0), snippet:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flink-kubernetes-operator-skip-crds
spec:
  source:
    repoURL: https://github.com/apache/flink-kubernetes-operator
    targetRevision: main
    path: helm/flink-kubernetes-operator
    helm:
      skipCrds: true
...
```
