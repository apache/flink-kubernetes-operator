---
title: "Configuration"
weight: 4
type: docs
aliases:
- /docs/operations/configuration/
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

The operator supports specifying a default configuration that is shared by the operator itself and the Flink deployments it manages.

These configuration files are mounted externally via ConfigMaps. The [configuration files](https://github.com/apache/flink-kubernetes-operator/tree/main/helm/flink-kubernetes-operator/conf) with default values are shipped in the Helm chart. It is recommended to review and adjust them if needed in the `values.yaml` file before deploying the operator in production environments.

To append to the default configuration, define the `flink-conf.yaml` key in the `defaultConfiguration` section of the Helm `values.yaml` file:

```yaml
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

### YAML Configuration File Format

Operator configuration is delivered through the `defaultConfiguration.flink-conf.yaml` block in the Helm `values.yaml`. The operator's runtime loads that file with Flink's legacy configuration parser, which accepts only flat key-value pairs written with dot notation and does not support nested YAML maps.

{{< hint info >}}
Flink 2.0 onward also defines a `config.yaml` format with YAML 1.2 syntax that supports nested keys (see the [Flink Configuration File](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/#flink-configuration-file) documentation). The Helm chart's `values.yaml` shows a `config.yaml` example block, but with the current chart it is not effectively loadable: the ConfigMap template unconditionally renders both `flink-conf.yaml` and `config.yaml` keys, and Flink's `GlobalConfiguration` prefers `flink-conf.yaml` when both are present.
{{< /hint >}}

### Flink Version and Namespace Specific Defaults

The operator also supports default configuration overrides for selected Flink versions and namespaces. This can be important if some behavior changed across Flink versions, or when certain namespaces should be treated differently, such as reconciling them more or less frequently:

```yaml
kubernetes.operator.default-configuration.namespace.ns1.k1: v1
kubernetes.operator.default-configuration.namespace.ns1.k2: v2
kubernetes.operator.default-configuration.namespace.ns2.k1: v1
```

Flink version specific defaults have a higher precedence, so namespace defaults are overridden by version defaults with the same key.

Flink version defaults can also be suffixed by a `+` character after the version string. This indicates that the default applies to this Flink version and any higher version.

For example, taking the configuration below:
```yaml
kubernetes.operator.default-configuration.flink-version.v1_16+.k4: v4
kubernetes.operator.default-configuration.flink-version.v1_16+.k5: v5
kubernetes.operator.default-configuration.flink-version.v1_17.k1: v1
kubernetes.operator.default-configuration.flink-version.v1_17.k2: v2
kubernetes.operator.default-configuration.flink-version.v1_17.k3: v3
kubernetes.operator.default-configuration.flink-version.v1_17.k5: v5.1
```
This would result in the defaults for Flink 1.17 being:
```yaml
k1: v1
k2: v2
k3: v3
k4: v4
k5: v5.1
```

The configuration above sets `k5: v5` for all versions >= 1.16. However, this is overridden for Flink 1.17 to `v5.1`. A Flink 1.18 deployment with this configuration would get `k5: v5`, not `v5.1`. The `k5` override only applies to Flink 1.17. Adding a `+` to the Flink 1.17 `k5` default would apply the new value to all future versions.

## Dynamic Operator Configuration

The Kubernetes operator supports dynamic config changes through the operator ConfigMaps. Dynamic operator configuration is enabled by default, and can be disabled by setting `kubernetes.operator.dynamic.config.enabled` to false. The check interval is specified by `kubernetes.operator.dynamic.config.check.interval` (default 5 minutes).

Whether dynamic configuration updates are enabled can be verified in the `deploy/flink-kubernetes-operator` log:

```
o.a.f.k.o.c.FlinkConfigManager [INFO ] Enabled dynamic config updates, checking config changes every PT5M
```

To change config values dynamically, the ConfigMap can be edited directly with `kubectl patch` or `kubectl edit`. For example, the reconcile interval is changed by overriding `kubernetes.operator.reconcile.interval`.

The update, here to 30 seconds, is confirmed in the operator log:

```
o.a.f.k.o.c.FlinkConfigManager [INFO ] Updating default configuration to {kubernetes.operator.reconcile.interval=PT30S}
```

The operator ConfigMap watcher always picks up edits and rebuilds the persisted default configuration. Whether the change reaches the running operator depends on which section the property lives in. Properties read only at startup keep their original value until the operator is restarted.

- Reconciliation-related properties are picked up on the next reconcile cycle. See:
  - [System Configuration → Reconciliation](#system-reconcile)
  - [Advanced System Configuration → Reconciliation](#advanced-reconcile)
  - [Resource and User Configuration](#resource-and-user-configuration)
  - [Blue/Green Deployment Configuration](#bluegreen-configuration)
  - [Autoscaler Configuration](#autoscaler-configuration)
  - [Autoscaler Standalone Configuration](#autoscaler-standalone-configuration)

- Startup-related properties are read once at operator startup. See:
  - [System Configuration → Startup](#system-startup)
  - [System Metrics Configuration](#system-metrics-configuration)
  - [Advanced System Configuration → Startup](#advanced-startup)

## Operator Configuration Reference

### System Configuration

General operator system configuration. Cannot be overridden on a per-resource basis. These are the options most installations tune.

#### Reconciliation {#system-reconcile}

Configuration properties consumed by the operator on every reconcile cycle. When dynamic ConfigMap reload is enabled, changes take effect on the next reconciliation cycle.

{{< generated/system_reconcile_section >}}

#### Startup {#system-startup}

Configuration properties read once at operator startup. Editing them changes the persisted default, but the running operator continues to use the value it read at startup until restarted.

{{< generated/system_startup_section >}}

### System Metrics Configuration

Operator system metrics configuration. Cannot be overridden on a per-resource basis. Editing them changes the persisted default, but the running operator continues to use the value it read at startup until restarted.

{{< generated/kubernetes_operator_metric_configuration >}}

### Advanced System Configuration

Operator-scope configuration properties that are rarely tuned. Cannot be overridden on a per-resource basis.

#### Reconciliation {#advanced-reconcile}

Advanced configuration properties that influence behavior inside the reconcile loop. When dynamic ConfigMap reload is enabled, changes take effect on the next reconciliation cycle.

{{< generated/system_advanced_reconcile_section >}}

#### Startup {#advanced-startup}

Advanced configuration properties that govern operator subsystems read at startup. Editing them changes the persisted default, but the running operator continues to use the value it read at startup until restarted.

{{< generated/system_advanced_startup_section >}}

### Resource and User Configuration

These configuration properties can be configured on both operator and per-resource level. When set under `spec.flinkConfiguration` on the Flink resources, they override the default value provided in the operator default configuration (`flink-conf.yaml`). They are also auto-reloadable through the [Dynamic Operator Configuration](#dynamic-operator-configuration) mechanism, taking effect on the next reconcile cycle.

{{< generated/dynamic_section >}}

### Blue/Green Deployment Configuration {#bluegreen-configuration}

Configuration properties of [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}), read from the `configuration` map at the top level of the `FlinkBlueGreenDeployment` spec rather than from the operator configuration.

{{< generated/bluegreen_section >}}

### Autoscaler Configuration

Like other resource configuration properties these can be configured on both operator and per-resource level. When set under `spec.flinkConfiguration` on the Flink resources, they override the default value provided in the operator default configuration (`flink-conf.yaml`). When dynamic ConfigMap reload is enabled, changes take effect on the next reconciliation cycle.

{{< hint info >}}
The option prefix `kubernetes.operator.` was removed in [FLIP-334](https://cwiki.apache.org/confluence/display/FLINK/FLIP-334+%3A+Decoupling+autoscaler+and+kubernetes+and+support+the+Standalone+Autoscaler), because the autoscaler module was decoupled from flink-kubernetes-operator.
{{< /hint >}}

{{< generated/auto_scaler_configuration >}}

### Autoscaler Standalone Configuration

Unlike other resource options, these options only work with the autoscaler standalone process. When dynamic ConfigMap reload is enabled, changes take effect on the next reconciliation cycle.

{{< hint info >}}
The standalone autoscaler also reads all properties listed in [Autoscaler Configuration](#autoscaler-configuration) above. The properties below are specific to the standalone process and complement that set.
{{< /hint >}}

{{< generated/autoscaler_standalone_configuration >}}

## Environment Variables

The operator uses several environment variables, falling into five groups based on how they are set and who reads them.

### Pod Metadata

These environment variables are exposed via Kubernetes fieldRef and populated from the downward API. They are available to custom plugins and sidecars that need to know the pod's own identity.

| Name               | Description                              | FieldRef             |
|--------------------|------------------------------------------|----------------------|
| HOST_IP            | The host on which the pod is scheduled   | status.hostIP        |
| OPERATOR_NAMESPACE | Namespace the operator is running in     | metadata.namespace   |
| POD_IP             | Pod IP                                   | status.podIP         |
| POD_NAME           | Pod name                                 | metadata.name        |

### Bootstrap and Runtime

These environment variables are consumed by the operator during startup and runtime. Most are set by the Helm chart and should not be overridden manually, though `WATCH_NAMESPACES` and `CONF_OVERRIDE_DIR` are intended for user tuning when running outside the standard Helm install.

| Name                    | Purpose                                                                                                 |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| CONF_OVERRIDE_DIR       | Additional directory whose `flink-conf.yaml` is layered on top of the default configuration at startup. |
| FLINK_CONF_DIR          | Mount path of the Flink config (`/opt/flink/conf`). Set by the Helm chart.                              |
| FLINK_PLUGINS_DIR       | Mount path of the Flink plugins directory (`/opt/flink/plugins`). Set by the Helm chart.                |
| HOSTNAME                | Pod hostname, used as the lease holder identity for leader election.                                    |
| JVM_ARGS                | Extra JVM arguments for the operator process. Set by the Helm chart from `jvmArgs.operator`.            |
| KUBERNETES_SERVICE_HOST | Auto-injected by Kubernetes. Presence triggers cluster mode, absence triggers local-development mode.   |
| LOG_CONFIG              | Path to the logging configuration file passed to the JVM. Set by the Helm chart.                        |
| LOGGING_FRAMEWORK       | `log4j2` or `logback`. Selects the logging implementation. Set by the Helm chart.                       |
| OPERATOR_NAME           | Operator identity string, set by the Helm chart.                                                        |
| WATCH_NAMESPACES        | Comma-separated namespace list. Overrides `kubernetes.operator.watched.namespaces` when set.            |

### TLS

These environment variables are set on the operator container when `tls.create: true` in the Helm values.

| Name                        | Purpose                                                  |
|-----------------------------|----------------------------------------------------------|
| OPERATOR_KEYSTORE_PASSWORD  | Keystore password, sourced from a Kubernetes secret.     |
| OPERATOR_KEYSTORE_PATH      | Path to the JKS keystore mounted on the operator pod.    |
| OPERATOR_TRUSTSTORE_PATH    | Path to the JKS truststore mounted on the operator pod.  |

### Webhook

These environment variables are set only on the webhook container when `webhook.create: true` in the Helm values.

| Name                       | Purpose                                                                |
|----------------------------|------------------------------------------------------------------------|
| WEBHOOK_KEYSTORE_FILE      | Path to the PKCS12 keystore (`/certs/keystore.p12`).                   |
| WEBHOOK_KEYSTORE_PASSWORD  | Keystore password, sourced from a Kubernetes secret.                   |
| WEBHOOK_KEYSTORE_TYPE      | Keystore type, defaults to `pkcs12`.                                   |
| WEBHOOK_SERVER_PORT        | Port the webhook listens on, defaults to `9443`.                       |

### IPv6 Configuration

In IPv6 environments, a host name verification error is triggered by a known bug in the OkHttp client. As a workaround until the OkHttp 5.0.0 release, the environment variable below needs to be set for both the operator and the Flink deployment configuration.

```
KUBERNETES_DISABLE_HOSTNAME_VERIFICATION=true
```

## Java Options for Java 17 Based Flink Images

From Java 17 onwards, Flink requires certain Java options to be supplied in order to function. These are set by default in the Helm chart's default configuration.
However, note:

- **Flink 1.19+**: To change the default opts for all Flink deployments, add to, rather than replace, these default options (`kubernetes.operator.default-configuration.flink-version.<flink-version>.env.java.default-opts.all`) in the Helm chart configuration. Avoid overriding `env.java.default-opts.all` in the `FlinkDeployment` configuration and use `env.java.opts.all` for any Java options needed by the job code.
- **Flink 1.18**: Setting default Java opts is not available in Flink 1.18, so the operator sets `env.java.opts.all` to allow Java 17 to work. For Flink 1.18 with Java 17 based images, avoid overriding `env.java.opts.all` in the `FlinkDeployment` configuration. If custom Java opts are required, include the [upstream Java options](https://github.com/apache/flink/blob/release-1.18.1/flink-dist/src/main/resources/flink-conf.yaml).

The default Flink images use JRE 11 (Eclipse Temurin).
However, there are official images published which use JRE 17 (e.g. `flink:1.20.0-scala_2.12-java17`), and the image used in a `FlinkDeployment` can be customized to use a different JRE.

As Java 17 now requires the use of the module system ([Project Jigsaw](https://openjdk.org/projects/jigsaw/)), the appropriate `add-exports` and `add-opens` Java options must be supplied in order for the pre-module system code in Flink to function correctly with Java 17+.
These options are defined upstream in the default configuration yaml (e.g. for [Flink 1.20](https://github.com/apache/flink/blob/release-1.20.0/flink-dist/src/main/resources/config.yaml)).

Java 17 based images could be handled by setting `env.java.opts.all` in `spec.flinkConfiguration` to the upstream opts list plus any options the job code requires, but that would have to be repeated for every deployment.
To avoid this, the Helm chart's default configuration sets the [`env.java.default-opts.all`](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/#env-java-default-opts-all) config to the upstream Flink Java options for the given Flink version.
The default opts are appended to any user supplied options via `env.java.opts.all`.
Job-specific Java options can therefore keep going into the `FlinkDeployment`, and those required for Flink's core operations are appended automatically.

The exception to the above is Flink 1.18, as the `env.java.default-opts.all` option is not available in that version.
For 1.18 the Helm chart's default configuration sets the `env.java.opts.all` directly, which allows Java 17 based images to work correctly.
However, a `FlinkDeployment` that sets its own `env.java.opts.all` needs to copy the upstream Java options into its list.
