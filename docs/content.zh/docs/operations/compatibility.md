---
title: "兼容性保证"
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

# 兼容性保证

用户清楚地了解 Operator 在升级到新版本时提供的兼容性保证非常重要。

## 自定义资源向后兼容性

Operator 面向用户的主要 API 是 Flink [自定义资源]({{< ref "docs/custom-resource/overview" >}})。

从 `v1beta1`（Operator 版本 1.0.0）开始，我们致力于为已部署的 Flink 自定义资源（`FlinkDeployment`、`FlinkSessionJob`）提供向后兼容性。

这意味着，如果您已部署了 Flink 资源（且 Flink 应用程序正在运行），您仍然可以安全地升级到更新版本的 Operator 和 CRD，而不会出现任何问题。
这将确保顺畅的操作体验，让您始终能够受益于最新的修复和改进，而不会危及当前的部署。

{{< hint warning >}}
我们不保证您始终能够使用旧 API 版本部署新资源。
例如，当我们将来从 `v1beta1 -> v1` 升级时，虽然可以保证您当前的作业不会失败，但您可能需要先升级到 `v1` 才能提交新资源。
{{< /hint >}}

## 资源状态的内容

目前，`FlinkDeployment` 和 `FlinkSessionJob` 资源包含非常详细的状态信息，其中包含 Operator 管理资源所需的一切内容。
这些信息大部分应被视为 Operator 逻辑的***内部***信息，将来可能会更改或消失。

总体而言，我们的目标是在下一个主要 CRD 版本（`v1`）中将状态精简为仅包含与用户/客户端相关的信息。

## Java API 兼容性

尽管为自定义资源提供[向后兼容性](#自定义资源向后兼容性)对我们非常重要，但目前我们无法为 Java API 提供同样的保证。

与 Operator 相关的 Java 接口（例如验证器）应被视为实验性/演进中的接口，可能在不同版本之间发生破坏性变更。

## Helm Chart 兼容性

Operator 仓库中包含的 Helm Chart 提供了一种在复杂环境中部署和管理 Operator 的便捷方式。
我们不断致力于改进配置参数和功能，以覆盖所有不同的场景。

目前，我们不为 Helm Chart 配置/组件提供向后兼容性。在升级到新版本之前，请验证您的自定义设置。

## Flink 版本兼容性

从 Flink Kubernetes Operator 1.7 开始，Operator 仅支持基于 Operator 发布日期的最近 4 个 Flink 次要版本。
超出此范围的版本将在 API 中被标记为 `@Deprecated`，并在使用时产生验证警告。

如需了解当前支持和已弃用的 Flink 版本的完整列表，请参阅
[FlinkVersion.java](https://github.com/apache/flink-kubernetes-operator/blob/main/flink-kubernetes-operator-api/src/main/java/org/apache/flink/kubernetes/operator/api/spec/FlinkVersion.java)。

{{< hint warning >}}
强烈建议升级到受支持的 Flink 版本，以确保与 Operator 的持续兼容性。
{{< /hint >}}
