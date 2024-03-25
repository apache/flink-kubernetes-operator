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

# Overview

Flink Kubernetes Operator充当控制平面，用于管理Apache Flink应用程序的完整部署生命周期。尽管Flink的原生Kubernetes集成已经允许您直接在运行中的Kubernetes（k8s）集群上部署Flink应用程序，但[自定义资源](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)和[Operator模式](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)也已成为Kubernetes本地部署体验的核心组成部分。

Flink Kubernetes Operator旨在承担管理Flink部署的人工操作员的责任。人工操作员对Flink部署的行为方式、如何启动集群、如何部署作业、如何升级以及如何在出现问题时做出反应具有深入的了解。Operator的主要目标是自动化这些活动，这是仅通过Flink本地集成无法实现的。

## Features

### 核心功能

- 完全自动化的[作业生命周期管理]({{< ref "docs/custom-resource/job-management" >}})
  - 运行、暂停和删除应用程序
  - 有状态和无状态应用程序升级
  - 触发和管理保存点
  - 处理错误，回滚中断的升级
- 支持多个 Flink 版本：v1.15、v1.16、v1.17、v1.18
- [部署模式]({{< ref "docs/custom-resource/overview#application-deployments" >}})：
  - 应用程序集群
  - 会话集群
  - 会话作业
- 内置的[高可用性](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/)
- 可扩展的框架
  - [自定义验证器]({{< ref "docs/operations/plugins#custom-flink-resource-validators" >}})
  - [自定义资源监听器]({{< ref "docs/operations/plugins#custom-flink-resource-listeners" >}})
- 高级[配置管理]({{< ref "docs/operations/configuration" >}})
  - 动态更新的默认配置
  - 每个作业的配置
  - 环境变量
- 通过[Pod 模板]({{< ref "docs/custom-resource/pod-template" >}})增强 POD
  - 原生 Kubernetes POD 定义
  - 分层（基础/作业管理器/任务管理器覆盖）
- [作业自动缩放器]({{< ref "docs/custom-resource/autoscaler" >}})
  - 收集滞后和利用率指标
  - 将作业顶点缩放到理想并行度
  - 根据负载的变化进行扩展和缩减

### 运维功能

- 操作员[指标]({{< ref "docs/operations/metrics-logging#metrics" >}})
  - 利用成熟的[Flink指标系统](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics)
  - 可插拔的指标报告器
  - 详细的资源和 Kubernetes API 访问指标
- 完全可定制的[日志记录]({{< ref "docs/operations/metrics-logging#logging" >}})
  - 默认日志配置
  - 每个作业的日志配置
  - 基于 Sidecar 的日志转发器
- Flink Web UI 和 REST 端点访问
  - 完全支持 Flink 原生 Kubernetes 的[服务暴露类型](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui)
  - 动态的[Ingress 模板]({{< ref "docs/operations/ingress" >}})
- 基于 Helm 的安装方式]({{< ref "docs/operations/helm" >}})
  - 自动化的[RBAC 配置]({{< ref "docs/operations/rbac" >}})
  - 高级定制技术
- 最新的公共仓库
  - GitHub 容器注册表 [ghcr.io/apache/flink-kubernetes-operator](http://ghcr.io/apache/flink-kubernetes-operator)
  - DockerHub https://hub.docker.com/r/apache/flink-kubernetes-operator

## Built-in Examples

该Operator项目提供了各种内置示例，以展示如何使用Operator功能。这些示例作为Operator仓库的一部分进行维护，可以在[这里](https://github.com/apache/flink-kubernetes-operator/tree/main/examples)找到。

**所涵盖的内容：**

- 应用程序、会话和会话作业的提交
- 检查点和高可用性配置
- Java、SQL和Python的Flink作业
- 入口、日志和指标配置
- 使用Kustomize进行高级运算符部署技术
- 还有一些其他内容...

## Known Issues & Limitations

### JobManager High-availability
Operator支持使用[Kubernetes HA Services](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/)和[ZooKeeper HA Services](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/zookeeper_ha/)来为Flink作业提供高可用性。高可用性解决方案可以通过使用额外的[备用副本](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/overview/)来提高恢复速度，但是当主JobManager失效时，Flink作业仍然会重新启动。

### JobResultStore Resource Leak

为了减轻[FLINK-27569](https://issues.apache.org/jira/browse/FLINK-27569)的影响，Operator引入了一个解决方法[FLINK-27573](https://issues.apache.org/jira/browse/FLINK-27573)，通过设置`job-result-store.delete-on-commit=false`和每次集群启动时为`job-result-store.storage-path`设置唯一值来解决。旧运行的存储路径必须手动清理，始终保留最新的目录。

```shell
ls -lth /tmp/flink/ha/job-result-store/basic-checkpoint-ha-example/
total 0
drwxr-xr-x 2 9999 9999 40 May 12 09:51 119e0203-c3a9-4121-9a60-d58839576f01 <- must be retained
drwxr-xr-x 2 9999 9999 60 May 12 09:46 a6031ec7-ab3e-4b30-ba77-6498e58e6b7f
drwxr-xr-x 2 9999 9999 60 May 11 15:11 b6fb2a9c-d1cd-4e65-a9a1-e825c4b47543
```

### AuditUtils can log sensitive information present in the custom resources
根据[FLINK-30306](https://issues.apache.org/jira/browse/FLINK-30306)的报告，当Flink自定义资源发生更改时，Operator会记录这些更改，其中可能包含敏感信息。我们建议在运行时将密码注入到Flink容器中，以减轻这个问题。

需要注意的是，任何具有对自定义资源访问权限的人已经可以访问到可能的敏感信息，但是现在只有那些只能访问日志的人也可以看到这些信息。我们计划在以后的版本中引入审计工具（AuditUtils）的数据遮蔽规则以改进这个问题。
