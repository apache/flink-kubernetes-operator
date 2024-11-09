---
title: "Overview"
weight: 1
type: docs
aliases:
- /concepts/overview.html
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

<a name="overview"></a>

# 概述
Flink Kubernetes Operator 扮演控制平面的角色，用于管理 Apache Flink 应用程序的完整部署生命周期。尽管 Flink 的原生 Kubernetes 集成已经允许你直接在运行的 Kubernetes(k8s) 集群上部署 Flink 应用程序，但 [自定义资源](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) 和 [operator 模式](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) 也已成为 Kubernetes 本地部署体验的核心。

Flink Kubernetes Operator 旨在承担人工操作 Flink 部署的职责。 人工操作者对 Flink 部署应该如何运行、如何启动集群、如何部署作业、如何升级作业以及出现问题时如何反应有着深入的了解。Operator 的主要目标是使这些活动自动化，而这无法仅通过 Flink 原生集成来实现。


<a name="features"></a>

## 特征

<a name="core"></a>

### 核心
- 全自动 [Job Lifecycle Management]({{< ref "docs/custom-resource/job-management" >}})
  - 运行、暂停和删除应用程序
  - 有状态和无状态应用程序升级
  - 保存点的触发和管理
  - 处理错误，回滚失败的升级
- 多 Flink 版本支持：v1.16, v1.17, v1.18, v1.19, v1.20
- [Deployment Modes]({{< ref "docs/custom-resource/overview#application-deployments" >}}):
  - Application 集群
  - Session 集群
  - Session job
- Built-in [High Availability](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/)   
- Extensible framework
  - [Custom validators]({{< ref "docs/operations/plugins#custom-flink-resource-validators" >}})
  - [Custom resource listeners]({{< ref "docs/operations/plugins#custom-flink-resource-listeners" >}})  
- Advanced [Configuration]({{< ref "docs/operations/configuration" >}}) management
  - 默认配置与动态更新
  - 作业级别的配置
  - 环境变量
- POD augmentation via [Pod Templates]({{< ref "docs/custom-resource/pod-template" >}})
  - 原生 Kubernetes POD 定义
  - 用于自定义容器和资源
- [Job Autoscaler]({{< ref "docs/custom-resource/autoscaler" >}})
  - 收集延迟和利用率指标
  - 将作业顶点调整到理想的并行度
  - 根据负载的变化进行扩展和缩减

<a name="operations"></a>

### 运营
- Operator [Metrics]({{< ref "docs/operations/metrics-logging#metrics" >}})
  - 使用成熟的 [Flink Metric System](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics)
  - 可插拔的指标报告器
  - 详细的资源和 kubernetes api 访问指标
- Fully-customizable [Logging]({{< ref "docs/operations/metrics-logging#logging" >}})
  - 默认日志配置
  - 作业级别的日志配置
  - 基于 sidecar 的日志转发器
- Flink Web UI 和 REST 端点访问
  - 完整支持 Flink 原生 Kubernetes [服务暴露类型](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui)
  - 通过 [Ingress 模板]({{< ref "docs/operations/ingress" >}}) 动态暴露服务
- [Helm based installation]({{< ref "docs/operations/helm" >}})
  - 自动的 [RBAC 配置]({{< ref "docs/operations/rbac" >}})
  - 高级定制化技术
- 最新的公共存储库
  - GitHub Container Registry [ghcr.io/apache/flink-kubernetes-operator](http://ghcr.io/apache/flink-kubernetes-operator)
  - DockerHub [https://hub.docker.com/r/apache/flink-kubernetes-operator](https://hub.docker.com/r/apache/flink-kubernetes-operator)

<a name="built-in-examples"></a>

## 内置示例

Operator 项目提供了各种内置示例，以展示如何使用 Operator 功能。这些示例维护在 operator 的仓库中，可以在[这里](https://github.com/apache/flink-kubernetes-operator/tree/main/examples) 找到.

**涵盖以下:**
 - Application, Session 以及 SessionJob 的提交
 - 检查点和高可用配置
 - Java, SQL 和 Python Flink 作业
 - Ingress、日志和指标配置
 - 使用 Kustomize 的高级 operator deployment 技术
 - 更多...

<a name="known-issues--limitations"></a>

## 已知问题和限制

<a name="jobManager-high-availability"></a>

### JobManager 高可用性
Operator 为 Flink 作业提供具有高可用性的 [Kubernetes HA 服务](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/)和 [Zookeeper HA 服务](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/zookeeper_ha/)。HA 解决方案可以从使用额外的 [备用副本](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/overview/)中受益，这将可以拥有更快的恢复时间，但是当 Leader JobManager 停机时，Flink 作业仍将重新启动。

<a name="jobResultStore-resource-leak"></a>

### JobResultStore 资源泄漏
为了缓解 [FLINK-27569](https://issues.apache.org/jira/browse/FLINK-27569) 的影响，Operator 在 [FLINK-27573](https://issues.apache.org/jira/browse/FLINK-27573) 中引入了一个解决方法: 设置 `job-result-store.delete-on-commit=false` 以及为每次集群启动设置唯一值的 `job-result-store.storage-path`。但这必须手动清理旧运行的存储路径，并始终保留最新目录：
```shell
ls -lth /tmp/flink/ha/job-result-store/basic-checkpoint-ha-example/
total 0
drwxr-xr-x 2 9999 9999 40 May 12 09:51 119e0203-c3a9-4121-9a60-d58839576f01 <- must be retained
drwxr-xr-x 2 9999 9999 60 May 12 09:46 a6031ec7-ab3e-4b30-ba77-6498e58e6b7f
drwxr-xr-x 2 9999 9999 60 May 11 15:11 b6fb2a9c-d1cd-4e65-a9a1-e825c4b47543
```

<a name="auditUtils-can-log-sensitive-information-present-in-the-custom-resources"></a>

### AuditUtils 可以记录自定义资源中的敏感信息
- 按照 [FLINK-30306](https://issues.apache.org/jira/browse/FLINK-30306) 中的报告，当 Flink 自定义资源发生变化时，Operator 会记录更改，其中可能包含敏感信息。我们建议在运行时将机密信息注入到 Flink 容器中以减轻这种情况。
- 请注意，任何可以访问自定义资源的人员已经可以访问潜在的敏感信息，但是只能访问日志的人员现在也可以看到这些信息。我们计划在后续版本中向 AuditUtils 引入遮蔽规则以改进此问题。
