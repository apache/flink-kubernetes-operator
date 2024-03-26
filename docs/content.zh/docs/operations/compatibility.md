---
title: "Compatibility Guarantees"
weight: 4
type: docs
aliases:
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

# Compatibility Guarantees

It is very important for users to clearly understand the compatibility guarantees the operator provides when upgrading to new versions.

## Custom Resource backward compatibility

The main user facing api of the operator are the Flink [custom resources]({{< ref "docs/custom-resource/overview" >}}).

Starting from `v1beta1` (operator version 1.0.0) we aim to provide backward compatibility for the already deployed Flink custom resources (`FlinkDeployment`, `FlinkSessionJob`).

This means that if you have Flink resources deployed (and Flink applications running), you can still safely upgrade to newer versions of the operator and CRD without any problems.
This should ensure a smooth operational experience where you can always benefit from the latest fixes and improvements without risking current deployments.

{{< hint warning >}}
We do not guarantee that you will always be able to deploy new resources using old API versions.
For example when in the future we upgrade from `v1beta1 -> v1`, while it is guaranteed that your current jobs won't fail, you might need to upgrade to `v1` before you can submit new resources.
{{< /hint >}}

## Contents of the resource status

Currently, the `FlinkDeployment` and `FlinkSessionJob` resources contain a very detailed status information that contains everything necessary for the operator to manage the resources.
Most of this information should be considered ***internal*** to operator logic and is subject to change/disappear in the future.

In general, we are aiming to slim down the status to only contain information that is relevant to users/client for the next major CRD version (`v1`).

## Java API compatibility

While it is very important for us to provide [backward compatibility for the custom resources](#custom-resource-backward-compatibility), at this time we cannot provide the same guarantee for Java API.

The operator related java interfaces, such as Validators should be considered experimental/evolving and can possibly break between releases.

## Helm chart compatibility

The Helm chart included in the operator repo provides a convenient way to deploy and manage the operator in complex environments.
We are constantly working on improving the configuration parameters and features to cover all the different scenarios.

At this time we do not provide backward compatibility for the Helm chart configuration / components. Please verify your custom settings before upgrading to a new version.
