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

# Metrics and Logging

## Metrics

The Flink Kubernetes Operator (Operator) extends the [Flink Metric System](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/) that allows gathering and exposing metrics to centralized monitoring solutions.

Different operator metrics can be turned on/off individually using the configuration. For details check the [metrics config reference]({{< ref "docs/operations/configuration#system-metrics-configuration" >}}).

### Flink Resource Metrics
The Operator gathers aggregates metrics about managed resources.

| Scope              | Metrics                                                                                   | Description                                                                                                                                                                                        | Type      |
|--------------------|-------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| Namespace          | FlinkDeployment/FlinkSessionJob.Count                                                     | Number of managed resources per namespace                                                                                                                               | Gauge     |
| Namespace          | FlinkDeployment.ResourceUsage.Cpu/Memory                                            | Total resources used per namespace                                                                                                                                                                | Gauge     |
| Namespace          | FlinkDeployment.JmDeploymentStatus.&lt;Status&gt;.Count                                   | Number of managed FlinkDeployment resources per &lt;Status&gt; per namespace. &lt;Status&gt; can take values from: READY, DEPLOYED_NOT_READY, DEPLOYING, MISSING, ERROR                            | Gauge     |
| Namespace          | FlinkDeployment.FlinkVersion.&lt;FlinkVersion&gt;.Count                               | Number of managed FlinkDeployment resources per &lt;FlinkVersion&gt; per namespace. &lt;FlinkVersion&gt; is retrieved via REST API from Flink JM.                                                  | Gauge     |
| Namespace          | FlinkDeployment/FlinkSessionJob.Lifecycle.State.&lt;State&gt;.Count                       | Number of managed resources currently in state &lt;State&gt; per namespace. &lt;State&gt; can take values from: CREATED, SUSPENDED, UPGRADING, DEPLOYED, STABLE, ROLLING_BACK, ROLLED_BACK, FAILED | Gauge     |
| System/Namespace   | FlinkDeployment/FlinkSessionJob.Lifecycle.State.&lt;State&gt;.TimeSeconds                 | Time spent in state &lt;State$gt for a given resource. &lt;State&gt; can take values from: CREATED, SUSPENDED, UPGRADING, DEPLOYED, STABLE, ROLLING_BACK, ROLLED_BACK, FAILED                      | Histogram |
| System/Namespace   | FlinkDeployment/FlinkSessionJob.Lifecycle.Transition.&lt;Transition&gt;.TimeSeconds       | Time statistics for selected lifecycle state transitions. &lt;Transition&gt; can take values from: Resume, Upgrade, Suspend, Stabilization, Rollback, Submission                                   | Histogram |

#### Lifecycle metrics

Based on the resource status the operator monitors [resource lifecycle states]({{< ref "docs/concepts/architecture#flink-resource-lifecycle" >}}).

The number of resources and time spend in each of these states at any given time is tracked by the `Lifecycle.<STATE>.Count` and `Lifecycle.<STATE>.TimeSeconds` metrics.

In addition to the simple counts we further track a few selected state transitions:

 - Upgrade : End-to-end resource upgrade time from stable to stable
 - Resume : Time from suspended to stable
 - Suspend : Time for any suspend operation
 - Stabilization : Time from deployed to stable state
 - Rollback : Time from deployed to rolled_back state if the resource was rolled back
 - Submission: Flink resource submission time

### Kubernetes Client Metrics

The Operator gathers various metrics related to Kubernetes API server access.

| Scope  | Metrics                                                   | Description                                                                                                                                                  | Type      |
|--------|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| System | KubeClient.HttpRequest.Count                              | Number of HTTP request sent to the Kubernetes API Server                                                                                                     | Counter   |
| System | KubeClient.HttpRequest.&lt;RequestMethod&gt;.Count        | Number of HTTP request sent to the Kubernetes API Server per request method. &lt;RequestMethod&gt; can take values from: GET, POST, PUT, PATCH, DELETE, etc. | Counter   |
| System | KubeClient.HttpRequest.Failed.Count                       | Number of failed HTTP requests that has no response from the Kubernetes API Server                                                                           | Counter   |
| System | KubeClient.HttpResponse.Count                             | Number of HTTP responses received from the Kubernetes API Server                                                                                             | Counter   |
| System | KubeClient.HttpResponse.&lt;ResponseCode&gt;.Count        | Number of HTTP responses received from the Kubernetes API Server per response code. &lt;ResponseCode&gt; can take values from: 200, 404, 503, etc.           | Counter   |
| System | KubeClient.HttpResponse.&lt;ResponseCode&gt;.NumPerSecond | Number of HTTP responses received from the Kubernetes API Server per response code per second. &lt;ResponseCode&gt; can take values from: 200, 404, 503, etc.| Meter     |
| System | KubeClient.HttpRequest.NumPerSecond                       | Number of HTTP requests sent to the Kubernetes API Server per second                                                                                         | Meter     |
| System | KubeClient.HttpRequest.Failed.NumPerSecond                | Number of failed HTTP requests sent to the Kubernetes API Server per second                                                                                  | Meter     |
| System | KubeClient.HttpResponse.NumPerSecond                      | Number of HTTP responses received from the Kubernetes API Server per second                                                                                  | Meter     |
| System | KubeClient.HttpResponse.TimeNanos                         | Latency statistics obtained from the HTTP responses received from the Kubernetes API Server                                                                  | Histogram |

#### Kubernetes client metrics by Http Response Code

It's possible to publish additional metrics by Http response code received from API server by setting `kubernetes.operator.kubernetes.client.metrics.http.response.code.groups.enabled` to `true` .

| Scope  | Metrics                                                   | Description                                                                                                                                                  | Type      |
|--------|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| System | KubeClient.HttpResponse.1xx.Count                         | Number of HTTP Code 1xx responses (informational) received from the Kubernetes API Server per response code.                                                 | Counter   |
| System | KubeClient.HttpResponse.2xx.Count                         | Number of HTTP Code 2xx responses (success) received from the Kubernetes API Server per response code.                                                       | Counter   |
| System | KubeClient.HttpResponse.3xx.Count                         | Number of HTTP Code 3xx responses (redirection) received from the Kubernetes API Server per response code.                                                   | Counter   |
| System | KubeClient.HttpResponse.4xx.Count                         | Number of HTTP Code 4xx responses (client error) received from the Kubernetes API Server per response code.                                                  | Counter   |
| System | KubeClient.HttpResponse.5xx.Count                         | Number of HTTP Code 5xx responses (server error) received from the Kubernetes API Server per response code.                                                  | Counter   |
| System | KubeClient.HttpResponse.1xx.NumPerSecond                  | Number of HTTP Code 1xx responses (informational) received from the Kubernetes API Server per response code per second.                                      | Meter     |
| System | KubeClient.HttpResponse.2xx.NumPerSecond                  | Number of HTTP Code 2xx responses (success) received from the Kubernetes API Server per response code per second.                                            | Meter     |
| System | KubeClient.HttpResponse.3xx.NumPerSecond                  | Number of HTTP Code 3xx responses (redirection) received from the Kubernetes API Server per response code per second.                                        | Meter     |
| System | KubeClient.HttpResponse.4xx.NumPerSecond                  | Number of HTTP Code 4xx responses (client error) received from the Kubernetes API Server per response code per second.                                       | Meter     |
| System | KubeClient.HttpResponse.5xx.NumPerSecond                  | Number of HTTP Code 5xx responses (server error) received from the Kubernetes API Server per response code per second.                                       | Meter     |

### JVM Metrics

The Operator gathers metrics about the JVM process and exposes it similarly to core Flink [System metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/#system-metrics). The list of metrics are not repeated in this document.

### JOSDK Metrics

The Flink operator also forwards metrics created by the Java Operator SDK framework itself under the `JOSDK` metric name prefix.
Some of these metrics are on system, namespace and resource level.

## Metric Reporters

The well known [Metric Reporters](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/metric_reporters) are shipped in the operator image and are ready to use.

In order to specify metrics configuration for the operator, prefix them with `kubernetes.operator.`. This logic ensures that we can separate Flink job and operator metrics configuration.

Let's look at a few examples.

### Slf4j
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

### How to Enable Prometheus (Example)
The following example shows how to enable the Prometheus metric reporter:
```yaml
defaultConfiguration:
  create: true
  append: true
  flink-conf.yaml: |+
    kubernetes.operator.metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
    kubernetes.operator.metrics.reporter.prom.port: 9999
```
Some metric reporters, including the Prometheus, need a port to be exposed on the container. This can be achieved be defining a value for the otherwise empty `metrics.port` variable.
Either in the `values.yaml` file:
```yaml
metrics:
  port: 9999
```
or using the option `--set metrics.port=9999` in the command line.

#### Set up Prometheus locally
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
The credentials for the dashboard can be retrieved by:
```bash
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
and apply it on your Kubernetes environment:
```shell
kubectl create -f pod-monitor.yaml
```
Once the custom resource is created in the Kubernetes environment the operator metrics are ready to explore [http://localhost:3000/explore](http://localhost:3000/explore).

## Logging
The Operator controls the logging behaviour for Flink applications and the Operator itself using configuration files mounted externally via ConfigMaps. [Configuration files](https://github.com/apache/flink-kubernetes-operator/tree/main/helm/flink-kubernetes-operator/conf) with default values are shipped in the Helm chart. It is recommended to review and adjust them if needed in the `values.yaml` file before deploying the Operator in production environments.

To append/override the default log configuration properties for the operator and Flink deployments define the `log4j-operator.properties` and `log4j-console.properties` keys respectively:

```yaml
defaultConfiguration:
  create: true
  append: true
  log4j-operator.properties: |+
    # Flink Operator Logging Overrides
    # rootLogger.level = DEBUG
    # The monitor interval in seconds to enable log4j automatic reconfiguration
    # monitorInterval = 30
  log4j-console.properties: |+
    # Flink Deployment Logging Overrides
    # rootLogger.level = DEBUG
    # The monitor interval in seconds to enable log4j automatic reconfiguration
    # monitorInterval = 30
```

{{< hint info >}}
Logging in the operator is intentionally succinct and does not include contextual information such as namespace or name of the FlinkDeployment objects.
We rely on the MDC provided by the operator-sdk to access this information and use it directly in the log layout.

See the [Java Operator SDK docs](https://javaoperatorsdk.io/docs/features#contextual-info-for-logging-with-mdc) for more detail.
{{< /hint >}}

To learn more about accessing the job logs or changing the log level dynamically check the corresponding [section](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#logging) of the core documentation.

### FlinkDeployment Logging Configuration

Users have the freedom to override the default `log4j-console.properties` settings on a per-deployment level by putting the entire log configuration into `spec.logConfiguration`:

```yaml
spec:
  ...
  logConfiguration:
    log4j-console.properties: |+
      rootLogger.level = DEBUG
      rootLogger.appenderRef.file.ref = LogFile
      ...
```

### FlinkDeployment Prometheus Configuration

The following example shows how to enable the Prometheus metric reporter for the FlinkDeployment:

```yaml
spec:
  ...
  flinkConfiguration:
    metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
    metrics.reporter.prom.port: 9249-9250
```
