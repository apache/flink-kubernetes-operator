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

# Pod Template

The custom resources are designed with a minimal set of direct, shorthand settings for the most basic attributes of a deployment. For all other settings the spec provides the `flinkConfiguration` and `podTemplate` fields.

Pod templates customize the JobManager and TaskManager pods of a `FlinkDeployment`, for example with volume mounts, ephemeral storage, or sidecar containers. They exist only there: a `FlinkSessionJob` carries no pod template, its job runs on the session cluster's pods.

The template type is not an operator invention: `podTemplate` is the standard Kubernetes [`PodTemplateSpec`](https://github.com/fabric8io/kubernetes-client/blob/main/kubernetes-model-generator/kubernetes-model-core/src/generated/java/io/fabric8/kubernetes/api/model/PodTemplateSpec.java) from the [Fabric8 Kubernetes Client](https://github.com/fabric8io/kubernetes-client) model the operator is built on, with the CRD schema generated from the upstream `Pod` type. Anything a Kubernetes pod template can express is valid here.

`spec.jobManager` and `spec.taskManager` are deliberately thin wrappers around the same structure: each holds only `replicas`, the container `resources` (standard Kubernetes resource requirements, with the older `resource` shorthand deprecated), and its own `podTemplate`. Everything else about the pods is expressed through the templates.

Pod templates can be layered, as shown in the example below. A common template under `spec.podTemplate` holds the settings that apply to both JobManager and TaskManager, like `volumeMounts`, while a template under `spec.jobManager` or `spec.taskManager` defines additional settings that supplement or override the common ones, such as a TaskManager sidecar. The operator merges the common and specific templates for each side.

The Flink container is identified by its fixed name: `flink-main-container` must not be changed, it is how the operator and Flink locate the main container among the sidecars when merging and deploying the templates.

A full example:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: default
  name: pod-template-example
spec:
  image: flink:1.20
  flinkVersion: v1_20
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
    resources:
      requests:
        memory: "2048Mi"
        cpu: "1"
  taskManager:
    resources:
      requests:
        memory: "2048Mi"
        cpu: "1"
    podTemplate:
      spec:
        initContainers:
          # Sample init container
          - name: busybox
            image: busybox:1.35.0
            command: [ 'sh','-c','echo hello from task manager' ]
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
```

{{< hint info >}}
In [Native Mode]({{< ref "docs/deployment/overview#native-mode" >}}) some template fields are overwritten by Flink, see [Fields Overwritten by Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#fields-overwritten-by-flink). In [Standalone Mode]({{< ref "docs/deployment/overview#standalone-mode" >}}) the operator merges the templates directly into the Deployments it creates.
{{< /hint >}}

## Array Merging Behavior

When layering pod templates, defining both a top-level and a JobManager-specific template for example, the corresponding YAML objects are merged together.

The default behavior is to merge arrays by position: the objects at the same array index are combined. This requires that containers in the templates are defined in the same order, otherwise the results may be undefined.

Default behavior (merge by position):

```
base:     [{name: a, p1: v1}, {name: b, p1: v1}]
override: [{name: a, p2: v2}, {name: c, p2: v2}]

merged:   [{name: a, p1: v1, p2: v2}, {name: c, p1: v1, p2: v2}]
```

The operator supports an alternative array merging mechanism, enabled with `kubernetes.operator.pod-template.merge-arrays-by-name`. When true, object array elements that define a `name` property are merged by that name instead of their position, and the resulting array is the union of the two inputs.

Merge by name:

```
base:     [{name: a, p1: v1}, {name: b, p1: v1}]
override: [{name: a, p2: v2}, {name: c, p2: v2}]

merged:   [{name: a, p1: v1, p2: v2}, {name: b, p1: v1}, {name: c, p2: v2}]
```

Merging by name is convenient when merging container specs or when the base and override templates are not defined together.
