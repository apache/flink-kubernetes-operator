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

## Ingress
Beyond the native options, the Operator also supports creating [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) entries for external UI access. 
Ingress generation can be turned on by defining the `ingress` field in the `FlinkDeployment`:
```yaml
metadata:
  namespace: default
  name: advanced-ingress
spec:
  image: flink:1.17
  flinkVersion: v1_17
  ingress:
    template: "flink.k8s.io/{{namespace}}/{{name}}(/|$)(.*)"
    className: "nginx"
    annotations:
      nginx.ingress.kubernetes.io/rewrite-target: "/$2"
```
The `ingress` specification defines a mandatory `template` field and two optional fields `className` and `annotations`. 
When the CR is submitted, the Operator substitutes the template variables from metadata and creates an Ingress entry on the Kubernetes cluster.  
Given the example above the Flink UI could be accessed at https://flink.k8s.io/default/advanced-ingress/ and the generated Ingress entry would be:
```yaml
apiVersion: networking.k8s.io/v1
  kind: Ingress
  metadata:
    annotations:
      nginx.ingress.kubernetes.io/rewrite-target: /$2
    name: advanced-ingress
    namespace: default
  spec:
    ingressClassName: nginx
    rules:
    - host: flink.k8s.io
    - http:
        paths:
        - backend:
            service:
              name: advanced-ingress-rest
              port:
                number: 8081
          path: /default/advanced-ingress(/|$)(.*)
          pathType: ImplementationSpecific
```

>Note: Flink Web UI is built with the popular [Angular](https://angular.io) framework and uses relative path to load static resources, hence the endpoint URL must end with trailing a `/` when accessing it from browsers through a proxy otherwise the main page appears as blank. 
> In order to support accessing base URLs without a trailing `/` the URLs can be [redirected](https://github.com/kubernetes/ingress-nginx/issues/4266). When using NGINX as ingress-controller this can be achieved by adding an extra annotation to the Ingress definition:
```yaml
nginx.ingress.kubernetes.io/configuration-snippet: |
if ($uri = "/default/advanced-ingress") {rewrite .* $1/default/advanced-ingress/ permanent;}
```
Beyond the example above the Operator understands other template formats too:

**Simple domain based routing:**
```yaml
ingress:
  template: "{{name}}.{{namespace}}.flink.k8s.io/"
```
This example requires that anything `*.flink.k8s.io` must be routed to the Ingress Controller with a wildcard DNS entry:
```shell
kubectl get ingress -A
NAMESPACE   NAME             CLASS   HOSTS                                 ADDRESS        PORTS   AGE
default     sample-job       nginx   sample-job.default.flink.k8s.io       192.168.49.2   80      30m
```
The Flink Web UI can be accessed at https://sample-job.default.flink.k8s.io

**Simple path based routing:**
```yaml
ingress:
  template: "/{{namespace}}/{{name}}(/|$)(.*)"
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: "/$2"
```
This example requires no DNS entries. 

```shell
kubectl get ingress -A
NAMESPACE   NAME               CLASS   HOSTS          ADDRESS     PORTS   AGE
default     sample-job         nginx   *              localhost   80      54m
```
The Flink Web UI can be accessed at https://localhost/default/sample-job/
>Note: All the examples were created on a minikube cluster. Check the [description](https://kubernetes.io/docs/tasks/access-application-cluster/ingress-minikube/) for enabling the NGINX Ingress Controller on minikube.

