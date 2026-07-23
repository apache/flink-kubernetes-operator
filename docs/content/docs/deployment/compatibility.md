---
title: "Compatibility"
weight: 2
type: docs
aliases:
- /docs/operations/compatibility/
- /operations/compatibility.html
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

# Compatibility

This page describes the compatibility commitments the operator makes across its custom resources and plugin SPIs, and the versions of Flink, Kubernetes, Java, and Helm that it works with.

The scope here is the operator's own compatibility. For upgrading the Flink version of a running job, and whether its APIs and state (savepoints and checkpoints) stay compatible, check the [Flink Upgrading Guide](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/upgrading/).

## Custom Resource

### Backward Compatibility

The Flink [custom resources]({{< ref "docs/custom-resource/overview" >}}) are the operator's main user-facing API.

Starting from `v1beta1` (operator version 1.0.0) the operator aims to provide backward compatibility for the already deployed Flink custom resources (`FlinkDeployment`, `FlinkSessionJob`, `FlinkStateSnapshot`, and `FlinkBlueGreenDeployment`).

This means that already deployed Flink resources (and running Flink applications) safely survive an upgrade to newer versions of the operator and CRD.
This ensures a smooth operational experience where the latest fixes and improvements are always available without risking current deployments.

{{< hint warning >}}
There is no guarantee that new resources can always be deployed using old API versions.
For example, when the API moves from `v1beta1` to `v1`, current jobs are guaranteed to keep running, but submitting new resources may require upgrading them to `v1`.
{{< /hint >}}

### Resource Status Contents

Currently, the `FlinkDeployment`, `FlinkSessionJob`, `FlinkStateSnapshot`, and `FlinkBlueGreenDeployment` resources carry very detailed status information, containing everything the operator needs to manage them.
Most of this information should be considered internal to operator logic and may change or disappear in the future.

In general, the operator aims to slim the status down to only the information relevant to users and clients for the next major CRD version (`v1`).

## Java

### Java Runtime

The operator and its webhook target JDK 17. Custom plugins should also be compiled for JDK 17.

The Job and Task Manager pods run separately from the operator and may use a different JRE based on the selected Flink image. See [Configuration → Java Options for Java 17 Based Flink Images]({{< ref "docs/deployment/configuration#java-options-for-java-17-based-flink-images" >}}) for the Java 17 specifics on the Flink side.

### Java API Compatibility

The operator's Java types under `org.apache.flink.kubernetes.operator.*` and `org.apache.flink.autoscaler.*` are `@Experimental` and may change between releases. This applies to both plugin families:

- [Resource Plugins]({{< ref "docs/deployment/plugins#resource-plugins" >}})
- [Autoscaler Plugins]({{< ref "docs/deployment/plugins#autoscaler-plugins" >}})

Custom plugins built against these interfaces are linked at compile time to the operator's published artifacts and to the Flink artifacts described in the [Operator's Bundled Flink Version](#operators-bundled-flink-version) section below. An operator upgrade that changes either contract may require a plugin rebuild. Plugin authors should track operator releases and rebuild plugins against the new artifacts before deploying a new operator image.

The Java types published by the `flink-kubernetes-operator-api` module (package `org.apache.flink.kubernetes.operator.api.*`) are also `@Experimental`. This covers the `FlinkDeployment`, `FlinkSessionJob`, `FlinkStateSnapshot`, and `FlinkBlueGreenDeployment` classes together with their `spec` and `status` types. Code that builds or reads these resources programmatically in Java links against this module and is subject to the same evolution.

{{< hint warning >}}
Using the public API classes from the `flink-kubernetes-operator-api` module carries no source or binary compatibility guarantee. These types may change once the CRD moves from `v1beta1` to `v1`, so Java code compiled against them may need to be updated and rebuilt for that release.
This is separate from the [Backward Compatibility](#backward-compatibility) guarantee, which protects already deployed resources at the Kubernetes level, not the Java model classes compiled against.
{{< /hint >}}

## Flink

### Supported Flink Versions

The Flink versions the operator can manage are enumerated by the `FlinkVersion` Java enum. The value set in `spec.flinkVersion` must be one of these. See the [FlinkVersion]({{< ref "docs/custom-resource/reference#flinkversion" >}}) table in the Custom Resource Reference for the full list and the version constants.

| Flink version                       | Status              | Notes                                                                                                                    |
|-------------------------------------|---------------------|--------------------------------------------------------------------------------------------------------------------------|
| 1.19, 1.20, 2.0, 2.1, 2.2, 2.3, 2.4 | Supported           | Actively maintained. Recommended for new deployments.                                                                    |
| 1.15, 1.16, 1.17, 1.18              | Deprecated          | Still accepted so existing jobs keep running. Plan to migrate off, as these versions may be removed in a future release. |
| 1.13, 1.14                          | No longer supported | Rejected at validation. The operator emits an `UnsupportedFlinkVersion` warning event.                                   |

### Operator's Bundled Flink Version

The operator JVM links against a specific Flink release for its own runtime needs, independent of the Flink version of the clusters it manages. The current bundled version is Flink 1.20.4.

This bundled runtime is used by the operator process for purposes such as:

- Pluggable filesystem access (`org.apache.flink.core.fs.FileSystem`), used to fetch `FlinkSessionJob` artifacts from non-HTTP schemes such as `s3://`, `gs://`, `hdfs://`.
- Configuration parsing (`org.apache.flink.configuration.GlobalConfiguration`) that reads the operator's own `flink-conf.yaml`.
- Plugin discovery (`org.apache.flink.core.plugin.PluginManager`) that loads custom validators, mutators, listeners, and filesystem factories.

## Kubernetes

The operator supports Kubernetes 1.21 and newer. It is built on the Fabric8 Kubernetes client, which is designed to work with any currently supported Kubernetes cluster version, so newer releases work as well and running a recent Kubernetes version is recommended.

The operator's baseline API needs are older than that floor, and all of them are served by far earlier releases:

- Custom resource definitions (`apiextensions.k8s.io/v1`)
- Leases (`coordination.k8s.io/v1`), used for [leader election]({{< ref "docs/deployment/leader-election" >}})
- Admission webhooks (`admissionregistration.k8s.io/v1`)

What sets the 1.21 floor is the webhook's default namespace filtering, which relies on the `kubernetes.io/metadata.name` label that Kubernetes adds automatically since 1.21.1. Clusters older than that can still run the operator if the webhook is disabled, or if the [watched namespaces]({{< ref "docs/deployment/helm/installation#watching-only-specific-namespaces" >}}) are labeled manually.

## Helm Chart

The operator is installed with a Helm chart, and both Helm 3 and Helm 4 are supported. The chart declares `apiVersion: v2` and relies on the `crds/` directory convention, which both CLIs install natively and Helm 2 cannot. One consequence follows from that convention: Helm applies the CRDs on the first install only, upgrades and uninstalls leave them untouched, so CRD upgrades are a separate manual step, as described under [Upgrading the Operator → Upgrading the CRDs]({{< ref "docs/operations/upgrade#2-upgrading-the-crds" >}}).

The Helm chart values schema is not guaranteed to stay stable across operator releases. Review the chart's `values.yaml` and the [Helm chart reference]({{< ref "docs/deployment/helm/installation" >}}) before upgrading, particularly when adopting a new minor operator version. The chart's evolution is tracked alongside operator code and is intentionally allowed to change shape when a clearer or safer default is identified.
