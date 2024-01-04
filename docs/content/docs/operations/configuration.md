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

To append to the default configuration, define the `flink-conf.yaml` key in the `defaultConfiguration` section of the Helm `values.yaml` file:

```
defaultConfiguration:
  create: true
  # Set append to false to replace configuration files
  append: true
  flink-conf.yaml: |+
    # Flink Config Overrides
    kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory
    kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE

    kubernetes.operator.reconcile.interval: 15 s
    kubernetes.operator.observer.progress-check.interval: 5 s
```

To learn more about metrics and logging configuration please refer to the dedicated [docs page]({{< ref "docs/operations/metrics-logging" >}}).

### Flink Version and Namespace specific defaults

The operator also supports default configuration overrides for selected Flink versions and namespaces. This can be important if some behaviour changed across Flink versions or we want to treat certain namespaces differently (such as reconcile it more or less frequently etc).

```
# Flink Version specific defaults 
kubernetes.operator.default-configuration.flink-version.v1_17.k1: v1
kubernetes.operator.default-configuration.flink-version.v1_17.k2: v2
kubernetes.operator.default-configuration.flink-version.v1_17.k3: v3

# Namespace specific defaults
kubernetes.operator.default-configuration.namespace.ns1.k1: v1
kubernetes.operator.default-configuration.namespace.ns1.k2: v2
kubernetes.operator.default-configuration.namespace.ns2.k1: v1
```

Flink version specific defaults will have a higher precedence so namespace defaults would be overridden by the same key.

## Dynamic Operator Configuration

The Kubernetes operator supports dynamic config changes through the operator ConfigMaps. Dynamic operator configuration is enabled by default, and can be disabled by setting `kubernetes.operator.dynamic.config.enabled` to false. Time interval for checking dynamic config changes is specified by `kubernetes.operator.dynamic.config.check.interval` of which default value is 5 minutes.

Verify whether dynamic operator configuration updates is enabled via the `deploy/flink-kubernetes-operator` log has:

```
2022-05-28 13:08:29,222 o.a.f.k.o.c.FlinkConfigManager [INFO ] Enabled dynamic config updates, checking config changes every PT5M
```

To change config values dynamically the ConfigMap can be directly edited via `kubectl patch` or `kubectl edit` command. For example to change the reschedule interval you can override `kubernetes.operator.reconcile.interval`.

Verify whether the config value of `kubernetes.operator.reconcile.interval` is updated to 30 seconds via the `deploy/flink-kubernetes-operator` log has:

```text
2022-05-28 13:08:30,115 o.a.f.k.o.c.FlinkConfigManager [INFO ] Updating default configuration to {kubernetes.operator.reconcile.interval=PT30S}
```

## Leader Election and High Availability

The operator supports high availability through leader election and standby operator instances. To enable leader election you need to add the following two mandatory operator configuration parameters.

```yaml
kubernetes.operator.leader-election.enabled: true
kubernetes.operator.leader-election.lease-name: flink-operator-lease
```

Lease name must be unique in the current lease namespace. For other more advanced config parameters please refer to the configuration reference.

Once you enabled leader election you can increase the `replicas` for the operator Deployment using the Helm chart to enable high availability.

If `replicas` value is greater than 1, you can define [topologySpreadConstraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/)
via `operatorPod.topologySpreadConstraints`.


## Environment variables
The operator exposes several environment variables which can be used for custom plugins.

| Name     | Description                           | FieldRef      |
|----------|---------------------------------------|---------------|
| HOST_IP  | The host which the pod is deployed on | status.hostIP |
| POD_IP   | Pod IP                                | status.podIP  |
| POD_NAME | Pod Name                              | metadata.name |

## Operator Configuration Reference

### System Configuration

General operator system configuration. Cannot be overridden on a per-resource basis.

{{< generated/system_section >}}

### Resource/User Configuration

These options can be configured on both an operator and a per-resource level. When set under `spec.flinkConfiguration` for the Flink resources it will override the default value provided in the operator default configuration (`flink-conf.yaml`).

{{< generated/dynamic_section >}}

### Autoscaler Configuration

Like other resource options these can be configured on both an operator and a per-resource level. When set under `spec.flinkConfiguration` for the Flink resources it will override the default value provided in the operator default configuration (`flink-conf.yaml`).

> Note: The option prefix `kubernetes.operator.` was removed in FLIP-334, because the autoscaler module was decoupled from flink-kubernetes-operator.

{{< generated/auto_scaler_configuration >}}

### Autoscaler Standalone Configuration

Unlike other resource options, these options only work with autoscaler standalone process.

{{< generated/autoscaler_standalone_configuration >}}

### System Metrics Configuration

Operator system metrics configuration. Cannot be overridden on a per-resource basis.

{{< generated/kubernetes_operator_metric_configuration >}}

### Advanced System Configuration

Advanced operator system configuration. Cannot be overridden on a per-resource basis.

{{< generated/system_advanced_section >}}

### IPV6 Configuration

If you run Flink Operator in IPV6 environment, the [host name verification error](https://issues.apache.org/jira/browse/FLINK-32777) will be triggered
due to a known bug in Okhttp client. As a workaround before new Okhttp 5.0.0 release, the environment variable below needs to be set 
for both Flink Operator and Flink Deployment Configuration.

KUBERNETES_DISABLE_HOSTNAME_VERIFICATION=true