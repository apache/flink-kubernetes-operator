---
title: "Configuration"
weight: 2
type: docs
aliases:
- /operations/configuration.html
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

# Configuration

## Specifying Operator Configuration

The operator allows users to specify default configuration that will be shared by the Flink operator itself and the Flink deployments.

These configuration files are mounted externally via ConfigMaps. The [Configuration files](https://github.com/apache/flink-kubernetes-operator/tree/main/helm/flink-kubernetes-operator/conf) with default values are shipped in the Helm chart. It is recommended to review and adjust them if needed in the `values.yaml` file before deploying the Operator in production environments.

To append to the default configuration, simply define the `flink-conf.yaml` key in the `defaultConfiguration` section of the Helm `values.yaml` file:

```
defaultConfiguration:
  create: true
  # Set append to false to replace configuration files
  append: true
  flink-conf.yaml: |+
    # Flink Config Overrides
    kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory
    kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE

    kubernetes.operator.reconciler.reschedule.interval: 15 s
    kubernetes.operator.observer.progress-check.interval: 5 s
```

To learn more about metrics and logging configuration please refer to the dedicated [docs page]({{< ref "docs/operations/metrics-logging" >}}).

## Operator Configuration Reference

| Key  | Default | Type | Description |
| ---- | ------- | ---- | ----------- |
| kubernetes.operator.reconciler.reschedule.interval     |    60s     |  Duration    | The interval for the controller to reschedule the reconcile process.            |
| kubernetes.operator.observer.rest-ready.delay    |  10s       |     Duration |     Final delay before deployment is marked ready after port becomes accessible.        |
| kubernetes.operator.reconciler.max.parallelism     |     5    |  Integer    |    The maximum number of threads running the reconciliation loop. Use -1 for infinite.         |
| kubernetes.operator.observer.progress-check.interval     |  10s       |  Duration    |     The interval for observing status for in-progress operations such as deployment and savepoints.        |
| kubernetes.operator.observer.savepoint.trigger.grace-period     |     10s    |   Duration   |   The interval before a savepoint trigger attempt is marked as unsuccessful.          |
| kubernetes.operator.observer.flink.client.timeout     |     10s    |  Duration    | The timeout for the observer to wait the flink rest client to return.            |
| kubernetes.operator.reconciler.flink.cancel.job.timeout     |     1min    |  Duration    | The timeout for the reconciler to wait for flink to cancel job.            |
| kubernetes.operator.reconciler.flink.cluster.shutdown.timeout     |     60s    |  Duration    | The timeout for the reconciler to wait for flink to shutdown cluster.           |
