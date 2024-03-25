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

# Architecture

Flink Kubernetes Operator（Operator）作为控制平面，管理Apache Flink应用程序的完整部署生命周期。可以使用[Helm](https://helm.sh/)在Kubernetes集群上安装Operator。在大多数生产环境中，它通常部署在指定的命名空间中，并控制一个或多个托管命名空间中的Flink部署。描述`FlinkDeployment`模式的自定义资源定义（CRD）是一个集群范围的资源。对于CRD，必须先注册声明，然后才能使用该CRD类型得任何资源，注册过程有时需要几秒钟的延迟。

{{< img src="/img/concepts/architecture.svg" alt="Flink Kubernetes Operator Architecture" >}}
> 注意：目前不支持[使用 Helm 升级或删除 CRD](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/)

## Control Loop

Operator遵循Kubernetes原则，特别是[控制循环](https://kubernetes.io/docs/concepts/architecture/controller/)：

{{< img src="/img/concepts/control_loop.svg" alt="Control Loop" >}}

用户可以使用Kubernetes命令行工具[kubectl](https://kubernetes.io/docs/tasks/tools/)与操作员进行交互。Operator持续跟踪与`FlinkDeployment` 和`FlinkSessionJob`自定义资源相关得集群事件。当Operator收到新的资源更新时，它将采取行动将Kubernetes集群调整到所需状态，作为其协调循环的一部分。初始循环由以下高级步骤组成：

1. 用户使用`kubectl`提交`FlinkDeployment`/`FlinkSessionJob`自定义资源（CR）
2. Operator观察Flink资源的当前状态（如果之前已经部署过）
3. Operator验证提交的资源变更
4. Operator协调任何所需的更改并执行升级

CR可以随时（重新）应用于集群。Operator不断调整以模仿期望的状态，直到当前状态成为期望的状态。在Operator中，所有的生命周期管理操作都是通过这个原则实现的。

该Operator使用[Java Operator SDK](https://github.com/java-operator-sdk/java-operator-sdk)构建，并在内部使用[Native Kubernetes Integration](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/)来启动Flink部署并提交作业。Java Operator SDK是一个更高级的框架和相关工具，用于支持使用Java编写Kubernetes Operator。Java Operator SDK和Flink的原生Kubernetes集成都使用[Fabric8 Kubernetes Client](https://github.com/fabric8io/kubernetes-client)与Kubernetes API Server进行交互。

## Flink Resource Lifecycle

Operator管理Flink资源的生命周期。下图说明了不同可能状态和转换：

{{< img src="/img/concepts/resource_lifecycle.svg" alt="Flink Resource Lifecycle" >}}

我们可以区分以下状态：

- CREATED：资源已在Kubernetes中创建，但尚未由Operator处理。
- SUSPENDED：（作业）资源已被挂起。
- UPGRADING：资源在升级到新规范之前被挂起。
- DEPLOYED：资源已部署/提交到Kubernetes，但尚未被视为稳定，可能在将来被回滚。
- STABLE：资源部署被视为稳定，不会回滚。
- ROLLING_BACK：资源正在回滚到最后一个稳定的规范。
- ROLLED_BACK：资源已部署使用最后一个稳定的规范。
- FAILED：作业终止失败。

## Admission Control

除了内置的准入插件之外，还可以作为扩展启动自定义准入插件，名为Flink Kubernetes Operator Webhook (Webhook)，并作为Webhook运行。

Webhook 遵循 Kubernetes 原则，特别是[动态准入控制](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)。

当使用 [Helm](https://helm.sh) 在 Kubernetes 集群上安装 Operator 时，它会默认部署。
请参阅如何部署 Operator/Webhook 的更多详细信息[此处](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/#deploying-the-operator).

该Webhook默认使用TLS协议进行通信。当密钥库文件发生更改时，它会自动加载/重新加载文件，并提供以下端点：

{{< img src="/img/concepts/webhook.svg" alt="Webhook" >}}
