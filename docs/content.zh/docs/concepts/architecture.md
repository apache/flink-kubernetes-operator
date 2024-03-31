---
title: "Architecture"
weight: 2
type: docs
aliases:
- /concepts/architecture.html
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

<a name="architecture"></a>

# 架构

Flink Kubernetes Operator（Operator）充当控制平面，用于管理 Apache Flink 应用程序的完整 deployment 生命周期。可以使用 [Helm](https://helm.sh) 在 Kubernetes 集群上安装 Operator。在大多数生产环境中，它通常部署在指定的命名空间中，并控制一个或多个 Flink deployments 到受托管的 namespaces 。描述 `FlinkDeployment` 结构的自定义资源定义（CRD）是集群范围的资源。对于 CRD，必须在使用该 CRD 类型的任何资源之前注册声明，注册过程有时需要几秒钟。

{{< img src="/img/concepts/architecture.svg" alt="Flink Kubernetes Operator 架构" >}}
> Note: 目前不支持[使用 Helm 升级或删除 CRD](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).

<a name="control-loop"></a>

## 控制平面
Operator 遵循 Kubernetes 原则，特别是 [控制平面](https://kubernetes.io/docs/concepts/architecture/controller/)：

{{< img src="/img/concepts/control_loop.svg" alt="控制循环" >}}

用户可以使用 Kubernetes 命令行工具 [kubectl](https://kubernetes.io/docs/tasks/tools/) 与 Operator 进行交互。Operator 不断跟踪与 `FlinkDeployment` 和 `FlinkSessionJob` 自定义资源相关的集群事件。当 Operator 接收到新的资源更新时，它将调整 Kubernetes 集群以达到所需状态，这个调整将作为其协调循环的一部分。初始循环包括以下高级步骤：

1. 用户使用 `kubectl` 提交 `FlinkDeployment`/`FlinkSessionJob` 自定义资源（CR）
2. Operator 观察 Flink 资源的当前状态（如果先前已部署）
3. Operator 验证提交的资源更改
4. Operator 协调任何必要的更改并执行升级

CR 可以随时在集群上（重新）应用。Operator 通过不断调整来模拟期望的状态，直到当前状态变为期望的状态。Operator 中的所有生命周期管理操作都是使用这个非常简单的原则实现的。

Operator 使用 [Java Operator SDK](https://github.com/java-operator-sdk/java-operator-sdk) 构建，并使用 [Native Kubernetes Integration](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/) 用于启动 Flink deployment 并在后台提交作业。
Java Operator SDK 是一个更高级别的框架和相关工具，用于支持使用 Java 编写 Kubernetes Operator。Java Operator SDK 和 Flink 的原生 kubernetes 集成本身都使用 [Fabric8 Kubernetes 客户端](https://github.com/fabric8io/kubernetes-client) 与 Kubernetes API 服务器交互。

<a name="flink-resource-lifecycle"></a>

## Flink 资源生命周期

Operator 管理 Flink 资源的生命周期。以下图表说明了可能的状态和转换：

{{< img src="/img/concepts/resource_lifecycle.svg" alt="Flink 资源生命周期" >}}


**我们可以区分以下几种状态：**

  - CREATED : 资源在 Kubernetes 中已创建，但尚未由 Operator 处理
  - SUSPENDED : （flink job）资源已被暂停
  - UPGRADING : 资源是在升级到新规格之前暂停的
  - DEPLOYED : 资源已部署/提交到 Kubernetes，但尚未稳定，可能会回滚
  - STABLE : 资源 deployment 被视为稳定，不会回滚
  - ROLLING_BACK : 资源正在回滚到上一个稳定规格
  - ROLLED_BACK : 资源已回滚到上一个稳定规格
  - FAILED : flink job 终止失败

<a name="admission-control"></a>

## 准入控制

除了预编译的准入插件之外，还有名为 Flink Kubernetes Operator Webhook (Webhook) 的自定义准入插件可以作为扩展启动并作为 webhook 运行。

Webhook 遵循 Kubernetes 原则，特别是 [dynamic admission control](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)。

当使用 [Helm](https://helm.sh) 在 Kubernetes 集群上安装 Operator 时，它会默认部署。有关如何部署 Operator/Webhook 的更多详细信息，
请参见[此处](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/#deploying-the-operator)。

Webhook 默认使用 TLS 协议进行通信。当文件发生更改时，它会自动加载/重新加载 keystore 文件，并提供以下端点：

{{< img src="/img/concepts/webhook.svg" alt="Webhook" >}}

