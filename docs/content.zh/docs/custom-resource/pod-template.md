---
title: "Pod Template"
weight: 3
type: docs
aliases:
- /custom-resource/pod-template.html
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

# Pod template

<a name="pod-template"></a>

Operator CRD 被设计为一组直接、简短的 CRD 设置，以表达 deployment 的最基本属性。对于所有其他设置，CRD 提供了 `flinkConfiguration` 和 `podTemplate` 字段。

Pod templates 允许自定义 Flink Job 和 Task Manager 的 pod，例如指定卷挂载、临时存储、sidecar 容器等。

Pod template 可以被分层，如下面的示例所示。
一个通用的 pod template 可以保存适用于作业和 task manager 的设置，比如 `volumeMounts`。作业或 task manager 下的另一个模板可以定义补充或覆盖通用模板中的其他设置，比如一个 task manager sidecar。

Operator 将分别合并作业和 task manager 的通用和特定模板。

下面是一个完整的示例：

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: default
  name: pod-template-example
spec:
  image: flink:1.17
  flinkVersion: v1_17
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
  serviceAccount: flink
  podTemplate:
    spec:
      containers:
        # Do not change the main container name
        - name: flink-main-container
          volumeMounts:
            - mountPath: /opt/flink/log
              name: flink-logs
        # Sample sidecar container
        - name: fluentbit
          image: fluent/fluent-bit:1.8.12-debug
          command: [ 'sh','-c','/fluent-bit/bin/fluent-bit -i tail -p path=/flink-logs/*.log -p multiline.parser=java -o stdout' ]
          volumeMounts:
            - mountPath: /flink-logs
              name: flink-logs
      volumes:
        - name: flink-logs
          emptyDir: { }
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
    podTemplate:
      spec:
        initContainers:
          # Sample sidecar container
          - name: busybox
            image: busybox:1.35.0
            command: [ 'sh','-c','echo hello from task manager' ]
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
```

{{< hint info >}}
当使用与 Flink 原生 Kubernetes 集成的 operator 时，请参考 [pod template 字段优先级](
https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#fields-overwritten-by-flink)。
{{< /hint >}}


## Array Merging Behaviour

<a name="array-meging-behaviour"></a>

当分层 pod templates（例如同时定义顶层和 jobmanager 特定的 pod 模板）时，相应的 yaml 会合并在一起。

Pod 模板机制的默认行为是通过合并相应数组位置的对象来合并数组。
这要求 podTemplates 中的容器以相同的顺序定义，否则结果可能未定义。

默认行为（按位置合并）：

```
arr1: [{name: a, p1: v1}, {name: b, p1: v1}]
arr1: [{name: a, p2: v2}, {name: c, p2: v2}]

merged: [{name: a, p1: v1, p2: v2}, {name: c, p1: v1, p2: v2}]
```

Operator 支持另一种数组合并机制，可以通过 `kubernetes.operator.pod-template.merge-arrays-by-name` 标志启用。
当为 true 时，不会进行默认的位置合并，而是根据名称合并定义了 `name` 属性的对象数组元素，并且生成的数组将是两个输入数组的并集。

通过名称合并：

```
arr1: [{name: a, p1: v1}, {name: b, p1: v1}]
arr1: [{name: a, p2: v2}, {name: c, p2: v2}]

merged: [{name: a, p1: v1, p2: v2}, {name: b, p1: v1}, {name: c, p2: v2}]
```

当合并容器规格或者当基础模板和覆盖模板没有一起定义时，按名称合并可以非常方便。
