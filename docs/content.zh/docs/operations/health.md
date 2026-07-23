---
title: "Monitoring the Operator"
weight: 6
type: docs
aliases:
- /operations/health.html
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

# Monitoring the Operator

This page covers monitoring the health of the operator itself: the health endpoint wired into the Kubernetes probes, canary resources that assert reconciliation is actually working, and scraping the operator with Prometheus. The metrics data behind that last part is documented under [Metrics]({{< ref "docs/operations/metrics" >}}).

## Health Probe

The Flink Kubernetes Operator provides a built-in health endpoint that serves as the information source for Kubernetes liveness and startup probes.

The liveness and startup probes are enabled by default in the Helm chart:

```yaml
operatorHealth:
  port: 8085
  livenessProbe:
    periodSeconds: 10
    initialDelaySeconds: 30
  startupProbe:
    failureThreshold: 30
    periodSeconds: 10
```

The health endpoint catches startup and informer errors that are exposed by the JOSDK framework. By default, if one of the watched namespaces becomes inaccessible the health endpoint will report an error and the operator will restart.

In some cases it is desirable to keep the operator running even if some namespaces are inaccessible. To allow the operator to start even if some namespaces cannot be watched, the `kubernetes.operator.startup.stop-on-informer-error` flag can be disabled.

## Canary Resources

The canary resource feature allows deploying special dummy resources (canaries) into selected namespaces. The operator health probe will then monitor that these resources are reconciled in a timely manner. This allows the probe to catch any slowdowns, and other general reconciliation issues not covered otherwise.

Canary resources are identified by a special label: `"flink.apache.org/canary": "true"`. These resources do not need to define a spec, start no pods, consume no other cluster resources, and exist purely to assert the operator reconciliation functionality.

Canary FlinkDeployment:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: canary
  labels:
    "flink.apache.org/canary": "true"
```

The default timeout for reconciling the canary resources is 1 minute and it is controlled by `kubernetes.operator.health.canary.resource.timeout`. If the operator cannot reconcile the canaries within this time limit the operator is marked unhealthy and will be automatically restarted.

Canaries can be deployed into multiple namespaces, and a `FlinkSessionJob` canary works the same way: the same label, no spec required.

## Monitoring with Prometheus

This assumes the operator exposes its metrics through the Prometheus reporter, as configured under [Metrics]({{< ref "docs/operations/metrics#prometheus" >}}).

The Prometheus Operator, among other options, provides an elegant, declarative way to specify how groups of pods should be monitored using custom resources.

To install the Prometheus operator via Helm run:

```shell
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack
```
The Grafana dashboard can be accessed through port-forwarding:
```shell
kubectl port-forward deployment/prometheus-grafana 3000
```
The credentials for the dashboard can be retrieved by:
```shell
kubectl get secret prometheus-grafana -o jsonpath="{.data.admin-user}" | base64 --decode ; echo
kubectl get secret prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```

To enable the operator metrics in Prometheus create a `pod-monitor.yaml` file with the following content:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: flink-kubernetes-operator
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: flink-kubernetes-operator
  podMetricsEndpoints:
      - port: metrics
```
and apply it to the Kubernetes environment:
```shell
kubectl create -f pod-monitor.yaml
```
Once the custom resource is created in the Kubernetes environment the operator metrics are ready to explore at [http://localhost:3000/explore](http://localhost:3000/explore).
