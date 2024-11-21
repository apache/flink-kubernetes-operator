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

The operator also supports default configuration overrides for selected Flink versions and namespaces. This can be important if some behaviour changed across Flink versions, or we want to treat certain namespaces differently (such as reconcile it more or less frequently etc.):

```
# Namespace specific defaults
kubernetes.operator.default-configuration.namespace.ns1.k1: v1
kubernetes.operator.default-configuration.namespace.ns1.k2: v2
kubernetes.operator.default-configuration.namespace.ns2.k1: v1
```

Flink version specific defaults have a higher precedence, so namespace defaults will be overridden by version defaults with the same key.

Flink version defaults can also be suffixed by a `+` character after the version string. This indicates that the default applies to this Flink version and any higher version.

For example, taking the configuration below:
```
# Flink Version specific defaults 
kubernetes.operator.default-configuration.flink-version.v1_16+.k4: v4
kubernetes.operator.default-configuration.flink-version.v1_16+.k5: v5
kubernetes.operator.default-configuration.flink-version.v1_17.k1: v1
kubernetes.operator.default-configuration.flink-version.v1_17.k2: v2
kubernetes.operator.default-configuration.flink-version.v1_17.k3: v3
kubernetes.operator.default-configuration.flink-version.v1_17.k5: v5.1
```
This would result in the defaults for Flink 1.17 being:
```
k1: v1
k2: v2
k3: v3
k4: v4
k5: v5.1
```

**Note**: The configuration above sets `k5: v5` for all versions >= 1.16. 
However, this is overridden for Flink 1.17 to `v5.1`. 
But if you ran a Flink 1.18 deployment with this configuration, then the value of `k5` would be `v5` not `v5.1`. The `k5` override only applies to Flink 1.17. 
Adding a `+` to the Flink 1.17 `k5` default would apply the new value to all future versions.


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

## Java options for Java 17 based Job/Task Manager images

### Summary

From Java 17 onwards, Flink requires certain Java options to be supplied in order to function. These are set by default in the operator's `helm` configuration file. 
However, users should note:

- **Flink 1.19+**: If users want to change the default opts for all Flink deployments, they should **add to** rather than replace these default options (`kubernetes.operator.default-configuration.flink-version.<flink-version>.env.java.default-opts.all`) in the `helm` configuration. They should also avoid overriding `env.java.default-opts.all` in their `FlinkDeployment` configuration and use `env.java.opts.all` for any Java options needed by their own code.
- **Flink 1.18**: Setting default Java opts is not available in Flink 1.18, so the operator sets `env.java.opts.all` to allow Java 17 to work. If users wish to use Flink 1.18 with Java 17 based images, they should avoid overriding `env.java.default.all` in their `FlinkDeployment` configuration. If custom Java opts are required, then they should include the [upstream Java options](https://github.com/apache/flink/blob/release-1.18.1/flink-dist/src/main/resources/flink-conf.yaml).

### Details

The default Job/Task Manager images use JRE 11 (Eclipse Temurin).
However, there are official images published which use JRE 17 (e.g. `flink:1.20.0-scala_2.12-java17`) and users can customize the image used in their `FlinkDeployment` which can use a custom JRE.

As Java 17 now requires the use of the module system ([Project Jigsaw](https://openjdk.org/projects/jigsaw/)), the user will need to supply the appropriate `add-exports` and `add-opens` Java options in order for the pre-module system code in Flink to function correctly with Java 17+.
These options are defined upstream in the default configuration yaml (e.g. for [Flink 1.20](https://github.com/apache/flink/blob/release-1.20.0/flink-dist/src/main/resources/config.yaml)).

Users wanting to run images based on Java 17 could supply a copy of these options by setting `env.java.opts.all`, in their `FlinkDeployment.spec.flinkconfiguration` to the upstream opts list and adding any options their code required.
However, this has to be done for every deployment.
To avoid this, the default configuration file used for `helm` sets the [`env.java.default-opts.all`](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/#env-java-default-opts-all) config to the upstream Flink Java options for the given Flink version.
The default opts are appended to any user supplied options via `env.java.opts.all`.
Therefore, users can continue to set Java options for their own code in their `FlinkDeployment`s and those required for Flink's core operations will be appended automatically.

The exception to the above is Flink 1.18, as the `env.java.default-opts.all` option is not available in that version.
For 1.18 the `helm` default config sets the `env.java.opts.all` directly.
This will allow Java 17 based images to work correctly.
However, if a user sets their own `env.java.opts.all` in their `FlinkDeployment`, then they will need to copy the [upstream Java options](https://github.com/apache/flink/blob/release-1.18.1/flink-dist/src/main/resources/flink-conf.yaml) into their list.
