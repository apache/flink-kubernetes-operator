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

## Dynamic Operator Configuration

The Kubernetes operator supports dynamic config changes through the operator ConfigMaps. Dynamic operator configuration is enabled by default, and can be disabled by setting `kubernetes.operator.dynamic.config.enabled`  to false. Time interval for checking dynamic config changes is specified by `kubernetes.operator.dynamic.config.check.interval` of which default value is 5 minutes. 

Verify whether dynamic operator configuration updates is enabled via the `deploy/flink-kubernetes-operator` log has:

```
2022-05-28 13:08:29,222 o.a.f.k.o.c.FlinkConfigManager [INFO ] Enabled dynamic config updates, checking config changes every PT5M
```

To change config values dynamically the ConfigMap can be directly edited via `kubectl patch` or `kubectl edit` command. For example to change the reschedule interval you can override `kubernetes.operator.reconciler.reschedule.interval`.

Verify whether the config value of `kubernetes.operator.reconciler.reschedule.interval` is updated to 30 seconds via the `deploy/flink-kubernetes-operator` log has:

```text
2022-05-28 13:08:30,115 o.a.f.k.o.c.FlinkConfigManager [INFO ] Updating default configuration to {kubernetes.operator.reconciler.reschedule.interval=PT30S}
```

## Operator Configuration Reference

{{< generated/kubernetes_operator_config_configuration >}}

## Job Specific Configuration Reference

Job specific configuration can be configured under `spec.flinkConfiguration` and it will override flink configurations defined in `flink-conf.yaml`.

- For application clusters, `spec.flinkConfiguration` will be located in `FlinkDeployment` CustomResource.
- For session clusters, configuring `spec.flinkConfiguration` in parent `FlinkDeployment` will be applied to all session jobs within the session cluster.
  - You can configure some additional job specific supplemental configuration through `spec.flinkConfiguration` in `FlinkSessionJob` CustomResource. 
  Those session job level configurations will override the parent session cluster's Flink configuration. Please note only the following configurations are considered to be valid configurations.
    - `kubernetes.operator.user.artifacts.http.header`
