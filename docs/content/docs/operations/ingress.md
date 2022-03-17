---
title: "Ingress"
weight: 3
type: docs
aliases:
- /operations/ingress.html
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

# Accessing Flink’s Web UI

The Flink Kubernetes Operator, by default, does not change the way the native kubernetes integration [exposes](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui) the Flink Web UI.

# Ingress
The Operator also supports creating an optional [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) entry for Web UI access. Ingress generation can be turned on by setting the `ingressDomain` field in the FlinkDeployment:

```yaml
apiVersion: flink.apache.org/v1alpha1
kind: FlinkDeployment
metadata:
  namespace: default
  name: basic-ingress
spec:
  image: flink:1.14.3
  flinkVersion: v1_14
  ingressDomain: flink.k8s.io
  ...
```

The Operator takes the `name`, `namespace` and `ingressDomain` values from the CR and creates an Ingress entry using the template `{{name}}.{{namespace}}.{{ingressDomain}}` as the host. This requires that anything `*.flink.k8s.io` should be routed to the Ingress Controller on the Kubernetes cluster.

```shell
kubectl get ingress
NAME             CLASS   HOSTS                                 ADDRESS        PORTS   AGE
basic-ingress    nginx   basic-ingress.default.flink.k8s.io    192.168.49.2   80      30m
basic-ingress2   nginx   basic-ingress2.default.flink.k8s.io   192.168.49.2   80      3m39s
```

The examples above were created on a minikube cluster. Check the [description](https://kubernetes.io/docs/tasks/access-application-cluster/ingress-minikube/) for easily enabling the NGINX Ingress Controller on minikube.

