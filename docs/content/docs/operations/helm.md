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
helm install flink-operator helm/flink-operator
```

Alternatively to install the operator (and also the helm chart) to a specific namespace:

```
helm install flink-operator helm/flink-operator --namespace flink-operator --create-namespace
```

Note that in this case you will need to update the namespace in the examples accordingly or the `default`
namespace to the [watched namespaces](#watching-only-specific-namespaces).

## Validating webhook

In order to use the webhook for FlinkDeployment validation, you must install the cert-manager on the Kubernetes cluster:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.7.1/cert-manager.yaml
```

The webhook can be disabled during helm install by passing the `--set webhook.create=false` parameter or editing the `values.yaml` directly.

## Watching only specific namespaces

The operator supports watching a specific list of namespaces for FlinkDeployment resources. You can enable it by setting the `--set watchNamespaces={flink-test}` parameter.
When this is enabled role-based access control is only created specifically for these namespaces for the operator and the jobmanagers, otherwise it defaults to cluster scope.
