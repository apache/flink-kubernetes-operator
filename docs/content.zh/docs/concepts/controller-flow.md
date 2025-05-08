---
title: "Controller Flow"
weight: 3
type: docs
aliases:
- /concepts/controller-flow.html
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

# Flink Operator Controller 流程

本页面的目的是深入介绍Flink Operator的逻辑，并提供足够的控制流设计细节，以便新开发者能够快速上手。

我们将假设读者具备良好的Flink Kubernetes及不同类型集群和作业的通用操作经验。有关Flink相关的概念，请参阅 https://flink.apache.org/ 。

我们还将假设读者对Flink Kubernetes Operator本身有深入的用户级理解。

本文档主要关注操作机制的 ***为什么？*** 和 ***怎么做？*** ，而用户层面的 ***是什么？*** 已经在其他地方有文档说明。

核心的 operator controller 流程 (在 `FlinkDeploymentController` 和 `FlinkSessionJobController` 中实现) 包含以下逻辑阶段：

1. 观察当前已部署资源的状态
2. 验证新的资源规范
3. 根据新的规范和观察到的状态，协调所需的更改
4. 重复执行上述步骤

需要注意的是，所有这些步骤每次都必须执行。无论验证的结果如何，观察和协调总是会执行，但验证可能会影响协调的目标，具体内容将在接下来的章节中详细说明。

## 观察阶段

在观察阶段，Observer模块负责观察当前（某一时间点）已部署的Flink资源（已提交的集群、作业）的状态，并根据这些信息更新自定义资源（CR）的状态字段。

观察者始终使用先前部署的配置来确保可以通过REST API访问Flink集群。用户配置可能会影响REST客户端（如端口配置等），因此我们必须始终使用集群上正在运行的配置。这就是为什么 `FlinkConfigManager` 和操作符在整个实现中区分观察和部署配置的主要原因。

观察者不应采取任何行动来更改或提交新的资源，我们将在后文看到这些操作是由协调器模块负责。这种分离的主要原因是，所需的操作不仅依赖于当前集群状态，还取决于用户在此期间可能提交的新规格变更。

**Observer 类层次结构：**

{{< img src="/img/concepts/observer_classes.svg" alt="Observer 类层次结构" >}}

### Observer 步骤

1. 如果没有部署资源，则跳过观察。
2. 如果处于 `ReconciliationState.UPGRADING` 状态，检查升级是否已经完成。
   通常，协调器在成功提交后会将状态升级为 `DEPLOYED`，但瞬态错误可能会阻止此状态更新。因此，我们使用资源特定的逻辑来检查集群中的资源是否已经匹配目标升级规范（注解、确定性作业ID）。
3. 对于 FlinkDeployments，接下来观察集群 Kubernetes 资源的状态，包括 JM Deployment 和 Pods。我们在升级后或无法访问 Job Rest 端点时进行此操作。状态记录在 `jobManagerDeploymentStatus` 字段中。

   值得注意的是，只有在 JobManager Kubernetes 资源看似健康的情况下，通过 Flink REST API 观察 Jobs 或其他内容才有意义。任何 JM 部署错误都会自动转换为错误状态，并应清除之前观察到的任何正在运行的 JobStatus。没有健康的 JobManager，作业不可能处于运行状态。
4. 如果集群看似健康，我们将继续观察 `JobStatus`（适用于 Application 和 SessionJob 资源）。在此阶段，我们使用 JobManager REST API 查询当前作业状态和待处理的保存点进度。

   当前作业状态决定了可以观察的内容。对于终端作业状态（`FAILED, FINISHED`），我们还会记录最后可用的保存点信息，以便在最后状态升级时使用。我们只能在终端状态这样做，因为否则无法保证在升级时不会创建新的检查点。

   如果无法访问作业，我们需要再次检查集群状态（参见步骤 3），或者如果在集群中找不到作业，下一步将取决于资源类型和配置，可能会触发错误或我们只需等待。当无法确定作业状态时，我们将使用 `RECONCILING` 状态。
5. 观察流程的最后一步是基于观察到的状态进行一些管理。如果一切正常且正在运行，我们将清除之前记录的资源状态错误。如果作业不再运行，我们将清除之前的保存点触发信息，以便在后续的协调阶段重新触发。
6. 在观察阶段结束时，操作员将更新的资源状态发送到 Kubernetes。这是避免在后续阶段丢失关键信息的关键步骤。一个这样的状态丢失示例：你有一个失败或完成的作业，观察者记录了最新的保存点信息。协调器可能会决定删除此集群以进行升级，但如果此时操作员失败，观察者将无法再次记录最后的保存点，因为它无法在已删除的集群上观察到。在执行任何集群操作之前记录状态是逻辑的关键部分。### Observing the SavepointInfo

### Observing the SavepointInfo

Savepoint 信息是 JobStatus 的一部分，跟踪待处理的保存点（无论是手动还是周期性触发的保存点信息）以及根据配置的保存点历史记录。Savepoint 信息仅在观察步骤中更新运行和终端状态的作业。如果作业正在失败、重启等，这意味着保存点失败并需要重新触发。

我们使用 triggerId 观察待处理的保存点，如果它们已完成，则将其记录到历史记录中。如果历史记录达到配置的大小限制，我们通过正在运行的作业 REST API 处置保存点，这样无需任何用户存储凭据即可完成。如果作业未运行或不健康，我们将清除待处理的保存点触发信息，这实际上从操作员的角度中止了保存点。

## 验证阶段

在资源成功观察并状态更新之后，我们接下来验证传入的资源 spec 字段。

如果新的 spec 验证失败，我们将触发一个错误事件给用户，并重置资源中上次成功提交的 spec（我们不会在 Kubernetes 中更新它，仅在本地用于协调）。

这一步非常重要，以确保即使用户提交了不正确的规范，协调也会运行，从而允许操作员在部署资源期间发生任何错误时恢复到之前所需的状态。

## 协调阶段

最后一步是协调阶段，它将执行任何必要的集群操作，以使资源达到最后一个所需（有效的）spec。在某些情况下，所需的 spec 可能在单次协调循环中达到，在其他情况下，我们可能需要多次传递，如我们将看到的。

非常重要的是要理解，观察阶段会将集群和资源的某个时间点视图记录到状态中。在大多数情况下，这可能会在任何未来时间发生变化（一个正在运行的作业可能会随时失败），在某些罕见情况下它是稳定的（一个终端失败或完成的作业将保持不变）。因此，协调器逻辑必须始终考虑集群状态可能已经与状态中的内容发生偏差的可能性（大多数复杂情况由此产生）。

< img src="/img/concepts/reconciler_classes.svg" alt="Reconciler 类层次结构" >

### 基础协调器步骤

`AbstractFlinkResourceReconciler` 封装了所有 Flink 资源类型的协调核心流程。在深入会话、应用程序和会话作业资源的特定细节之前，我们先来看看高层次流程。

1. 检查资源是否准备好进行协调，或者是否有任何不应中断的待处理操作（例如手动保存点）。
2. 如果这是资源的首次部署尝试，我们直接部署它。需要注意的是，这是唯一一个使用 spec 中提供的 `initialSavepointPath` 的部署操作。
3. 接下来我们确定所需的 spec 是否发生变化以及变化的类型：`IGNORE, SCALE, UPGRADE`。仅在 `SCALE` 和 `UPGRADE` 类型的变化时，才需要执行进一步的协调逻辑。
4. 如果我们有 `UPGRADE`/`SCALE` spec 变化，我们将执行特定于资源类型的升级逻辑。
5. 如果我们没有收到任何 spec 变化，我们仍然需要确保当前部署的资源已完全协调：
   1. 检查任何先前部署的资源是否失败/未稳定，如果需要，执行回滚操作。
   2. 应用任何特定于各个资源的进一步协调步骤（触发保存点、恢复部署、重启不健康的集群等）。

### 关于部署操作的注意事项

我们必须特别小心部署操作，因为它们会启动集群和作业，这些作业可能会立即开始生成数据和检查点。因此，能够识别部署是否成功/失败至关重要。同时，操作员进程可能会在任何时候失败，这使得始终记录正确状态变得困难。

为了确保我们始终能够恢复部署状态以及集群上正在运行的内容，在尝试部署之前，将要部署的 spec 始终以 `UPGRADING` 状态写入状态。此外，向部署的 Kubernetes Deployment 资源添加注解，以便能够区分确切的部署尝试（我们使用 CR 生成版本来实现这一点）。对于会话作业，由于无法添加注解，我们使用一种特殊的方式生成包含相同信息的 jobid。

### 基础作业协调器

`AbstractJobReconciler` 负责执行管理作业的 Flink 资源的共享逻辑（应用程序和会话作业集群）。这里的核心逻辑部分处理管理作业状态以及以安全方式执行有状态作业更新。

根据 spec 变化的类型 `SCALE/UPGRADE`，作业协调器具有略有不同的代码路径。对于缩放操作，如果启用了独立模式和反应式缩放，我们只需要重新缩放 TaskManagers。未来我们可能会在此处为其他集群类型添加更高效的缩放（例如，一旦上游 Flink 实现了缩放 API）。

如果在 spec 中检测到 `UPGRADE` 类型的变化，我们将执行作业升级流程：

1. 如果作业当前正在运行，根据所需的（可用的升级模式）暂停，更多内容稍后介绍。
2. 在状态中标记升级状态，并触发新的协调循环（这允许我们在升级过程中拾取新的 spec 变化，因为暂停可能需要一段时间）。
3. 根据新的 spec 从 HA 元数据或状态中记录的保存点信息，或空状态（根据升级模式设置）恢复作业。

#### UpgradeMode 和暂停/取消行为

操作员必须始终尊重升级模式设置，以避免有状态升级时的数据丢失。然而，机制中有一些灵活性，可以处理不健康的作业，并在版本升级期间提供额外的保护。**getAvailableUpgradeMode** 方法是升级逻辑中的一个重要基石，用于根据用户的请求和当前集群状态决定实际使用的升级模式。

在正常健康的情况下，可用的升级模式将与用户在 spec 中的设置相同。然而，在某些情况下，我们需要在保存点和最后状态升级模式之间进行切换。保存点升级模式仅在作业健康且正在运行时可以使用，对于失败、重启或其他不健康的部署，只要 HA 元数据可用（且未显式配置其他方式），我们可以使用最后状态升级模式。这使我们即使在作业失败的情况下也能拥有一个稳健的升级流程，同时保持状态一致性。

最后状态升级模式指的是使用 HA 元数据中存储的检查点信息进行升级。HA 元数据格式在升级 Flink 次要版本之间可能不兼容，因此版本更改必须强制使用保存点升级模式，并要求一个健康且正在运行的作业。

非常重要的是要注意，我们从不从最后状态/保存点升级模式切换到无状态模式，因为这会破坏升级逻辑并导致状态丢失。

### 应用程序协调器

应用程序集群在部署/升级/取消操作期间需要处理一些比会话作业更多的额外配置步骤。以下是操作员为确保稳健行为所做的重要事情：

**setRandomJobResultStorePath**: 为了防止终止的应用程序在 JM 故障转移时被重新启动，我们必须禁用作业结果清理。这迫使我们为每个应用程序部署创建一个随机的作业结果存储路径，如这里所述：https://issues.apache.org/jira/browse/FLINK-27569 。用户需要稍后手动清理 jobresultstore。

**setJobIdIfNecessary**: Flink 默认基于 clusterId（在我们的情况下是 CR 名称）生成确定性的 jobid。如果作业从空状态（无状态升级）重新启动，这会导致检查点路径冲突。因此，我们生成一个随机的 jobid 以避免这种情况。

**cleanupTerminalJmAfterTtl**: 在 Flink 1.15 及以上版本中，我们不会在关闭/失败后自动终止 JobManager 进程，以获得稳健的观察行为。然而，一旦观察到终端状态，我们可以清理 JM 进程/部署。

## 自定义 Flink 资源状态更新机制

JOSDK 提供了内置的方法来更新协调器实现中的资源 spec 和状态，但 Flink 操作员不使用这些方法，而是使用自定义的状态更新机制，原因如下。

当使用 JOSDK 时，CR 的状态只能在协调方法结束时更新。在我们的情况下，我们经常需要在协调流程中间触发状态更新，以提供最大的稳健性。一个这样的例子是在执行部署操作之前记录部署信息到状态中。

另一种看待方式是，Flink 操作员将资源状态用作许多操作的预写日志，以保证在操作员失败时的稳健性。

状态更新机制在 `StatusRecorder` 类中实现，该类同时作为最新状态的缓存和更新逻辑。我们需要始终从缓存中更新我们的 CR 状态，因为在控制器流程的开头我们绕过了 JOSDK 的更新机制/缓存，这可能导致返回旧的状态实例。对于实际的状态更新，我们使用一种修改后的乐观锁定机制，只有在状态在此期间未被外部修改时才更新状态。

在正常情况下，这种假设成立，因为操作员是状态的唯一所有者/更新者。这里的异常可能表明用户篡改了状态，或者另一个操作员实例正在同时管理相同的资源，这可能导致严重问题。

## JOSDK 与操作员接口命名冲突

在 JOSDK 世界中，Reconciler 接口代表整个控制/协调流程。在我们的情况下，我们将这些类称为控制器，并将我们自己的 Reconciler 接口保留为控制器流程的特定部分。

历史上，JOSDK 也将 Reconciler 接口称为 Controller。