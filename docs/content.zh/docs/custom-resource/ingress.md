---
title: "Ingress"
weight: 4
type: docs
aliases:
- /docs/operations/ingress/
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

# Ingress

By default the operator does not change how Flink's native Kubernetes integration exposes the Flink Web UI, all the native [service expose types](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui) remain available. Beyond those, the operator can also create [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) entries for external UI access, generated from the `ingress` field of a `FlinkDeployment`:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: default
  name: advanced-ingress
spec:
  image: flink:1.20
  flinkVersion: v1_20
  ingress:
    template: "flink.k8s.io/{{namespace}}/{{name}}(/|$)(.*)"
    className: "nginx"
    annotations:
      nginx.ingress.kubernetes.io/rewrite-target: "/$2"
```

The `ingress` specification has a mandatory `template` field and four optional ones:

| Field         | Type   | Description                                                                                                                        |
|---------------|--------|------------------------------------------------------------------------------------------------------------------------------------|
| `template`    | String | The URL template of the entry, with `{{name}}` and `{{namespace}}` substituted from the resource metadata                          |
| `className`   | String | The `ingressClassName` of the generated entry, selecting the Ingress controller                                                    |
| `annotations` | Map    | Annotations set on the generated Ingress metadata                                                                                  |
| `labels`      | Map    | Labels merged into the generated Ingress metadata                                                                                  |
| `tls`         | List   | Standard Kubernetes [Ingress TLS](https://kubernetes.io/docs/concepts/services-networking/ingress/#tls) entries, hosts plus a secret name |

The operator creates the Ingress entry on the Kubernetes cluster when the resource is deployed.

Given the example above, the Flink Web UI is accessible at `https://flink.k8s.io/default/advanced-ingress/` and the generated Ingress entry is:

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
    http:
      paths:
      - backend:
          service:
            name: advanced-ingress-rest
            port:
              number: 8081
        path: /default/advanced-ingress(/|$)(.*)
        pathType: ImplementationSpecific
```

{{< hint info >}}
The Flink Web UI is built with the [Angular](https://angular.io) framework and loads its static resources through relative paths: when accessed through a proxy, the URL must end with a trailing `/`, otherwise the main page renders blank. Base URLs without the trailing slash can be [redirected](https://github.com/kubernetes/ingress-nginx/issues/4266) with an extra annotation on the Ingress definition, shown below for NGINX as the Ingress controller.
{{< /hint >}}

```yaml
nginx.ingress.kubernetes.io/configuration-snippet: |
  if ($uri = "/default/advanced-ingress") {rewrite .* $1/default/advanced-ingress/ permanent;}
```

Beyond the example above, the operator supports other template formats described below.

## Simple Domain-Based Routing

```yaml
ingress:
  template: "{{name}}.{{namespace}}.flink.k8s.io/"
```

This template requires that anything under `*.flink.k8s.io` is routed to the Ingress controller through a wildcard DNS entry:

```shell
kubectl get ingress -A
NAMESPACE   NAME             CLASS   HOSTS                                 ADDRESS        PORTS   AGE
default     sample-job       nginx   sample-job.default.flink.k8s.io       192.168.49.2   80      30m
```

The Flink Web UI is accessible at `https://sample-job.default.flink.k8s.io`.

## Simple Path-Based Routing

```yaml
ingress:
  template: "/{{namespace}}/{{name}}(/|$)(.*)"
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: "/$2"
```

This template requires no DNS entries:

```shell
kubectl get ingress -A
NAMESPACE   NAME               CLASS   HOSTS          ADDRESS     PORTS   AGE
default     sample-job         nginx   *              localhost   80      54m
```

The Flink Web UI is accessible at `https://localhost/default/sample-job/`.

{{< hint info >}}
All examples on this page were created on a minikube cluster, see [Set up Ingress on Minikube with the NGINX Ingress Controller](https://kubernetes.io/docs/tasks/access-application-cluster/ingress-minikube/) for enabling the controller there.
{{< /hint >}}
