---
title: "Metrics and Logging"
weight: 4
type: docs
aliases:
- /operations/metrics-logging.html
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

# Metrics

The Flink Kubernetes Operator (Operator) extends the [Flink Metric System](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/) that allows gathering and exposing metrics to centralized monitoring solutions. 


## Deployment Metrics
The Operator gathers aggregates metrics about managed resources.

| Scope     | Metrics                        | Description                                                                                                                                                 | Type  |
|-----------|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
| Namespace | FlinkDeployment.Count          | Number of managed FlinkDeployment instances per namespace                                                                                                   | Gauge |
| Namespace | FlinkDeployment.<Status>.Count | Number of managed FlinkDeployment resources per <Status> per namespace. <Status> can take values from: READY, DEPLOYED_NOT_READY, DEPLOYING, MISSING, ERROR | Gauge |
| Namespace | FlinkSessionJob.Count          | Number of managed FlinkSessionJob instances per namespace                                                                                                   | Gauge |

## System Metrics
The Operator gathers metrics about the JVM process and exposes it similarly to core Flink [System metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/#system-metrics). The list of metrics are not repeated in this document. 

## Metric Reporters

The well known [Metric Reporters](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/metric_reporters) are shipped in the operator image and are ready to use.

In order to specify metrics configuration for the operator, simply prefix them with `kubernetes.operator.`. This logic ensures that we can easily separate Flink job and operator metrics configuration.

Let's look at a few examples.

## Slf4j
The default metrics reporter in the operator is Slf4j. It does not require any external monitoring systems, and it is enabled in the `values.yaml` file by default, mainly for demonstrating purposes.
```yaml
defaultConfiguration:
  create: true
  append: true
  flink-conf.yaml: |+
    kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory
    kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE
```
To use a more robust production grade monitoring solution the configuration needs to be changed.

## How to Enable Prometheus (Example)
The following example shows how to enable the Prometheus metric reporter:
```yaml
defaultConfiguration:
  create: true
  append: true
  flink-conf.yaml: |+
    kubernetes.operator.metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
    kubernetes.operator.metrics.reporter.prom.port: 9999
```
Some metric reporters, including the Prometheus, needs a port to be exposed on the container. This can be achieved be defining a value for the otherwise empty `metrics.port` variable.
Either in the `values.yaml` file:
```yaml
metrics:
  port: 9999
```
or using the option `--set metrics.port=9999` in the command line.

### Set up Prometheus locally
The Prometheus Operator among other options provides an elegant, declarative way to specify how group of pods should be monitored using custom resources.

To install the Prometheus operator via Helm run:

```shell
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack
```
The Grafana dashboard can be accessed through port-forwarding:
```shell
kubectl port-forward deployment/prometheus-grafana 3000
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
and apply it on your Kubernetes environment:
```shell
kubectl create -f pod-monitor.yaml
```
Once the custom resource is created in the Kubernetes environment the operator metrics are ready to explore [http://localhost:3000/explore](http://localhost:3000/explore).

# Logging
The Operator controls the logging behaviour for Flink applications and the Operator itself using configuration files mounted externally via ConfigMaps. [Configuration files](https://github.com/apache/flink-kubernetes-operator/tree/main/helm/flink-kubernetes-operator/conf) with default values are shipped in the Helm chart. It is recommended to review and adjust them if needed in the `values.yaml` file before deploying the Operator in production environments.

To append/override the default log configuration properties for the operator and Flink deployments define the `log4j-operator.properties` and `log4j-console.properties` keys respectively:

```yaml
defaultConfiguration:
  create: true
  append: true
  log4j-operator.properties: |+
    # Flink Operator Logging Overrides
    # rootLogger.level = DEBUG
  log4j-console.properties: |+
    # Flink Deployment Logging Overrides
    # rootLogger.level = DEBUG
```

{{< hint info >}}
Logging in the operator is intentionally succinct and does not include contextual information such as namespace or name of the FlinkDeployment objects.
We rely on the MDC provided by the operator-sdk to access this information and use it directly in the log layout.

See the [Java Operator SDK docs](https://javaoperatorsdk.io/docs/features#contextual-info-for-logging-with-mdc) for more detail.
{{< /hint >}}

To learn more about accessing the job logs or changing the log level dynamically check the corresponding [section](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#logging) of the core documentation.
