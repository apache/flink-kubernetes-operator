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

The operator CRD is designed to have a minimal set of direct, short-hand CRD settings to express the most
basic attributes of a deployment. For all other settings the CRD provides the `flinkConfiguration` and
`podTemplate` fields.

Pod templates permit customization of the Flink job and task manager pods, for example to specify
volume mounts, ephemeral storage, sidecar containers etc.

Pod templates can be layered, as shown in the example below.
A common pod template may hold the settings that apply to both job and task manager,
like `volumeMounts`. Another template under job or task manager can define additional settings that supplement or override those
in the common template, such as a task manager sidecar.

The operator is going to merge the common and specific templates for job and task manager respectively.

Here the full example:

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
When using the operator with Flink native Kubernetes integration, please refer to [pod template field precedence](
https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#fields-overwritten-by-flink).
{{< /hint >}}

## Array Merging Behaviour

When layering pod templates (defining both a top level and jobmanager specific podtemplate for example) the corresponding yamls are merged together.

The default behaviour of the pod template mechanism is to merge array arrays by merging the objects in the respective array positions.
This requires that containers in the podTemplates are defined in the same order otherwise the results may be undefined.

Default behaviour (merge by position):

```
arr1: [{name: a, p1: v1}, {name: b, p1: v1}]
arr1: [{name: a, p2: v2}, {name: c, p2: v2}]

merged: [{name: a, p1: v1, p2: v2}, {name: c, p1: v1, p2: v2}]
```

The operator supports an alternative array merging mechanism that can be enabled by the `kubernetes.operator.pod-template.merge-arrays-by-name` flag.
When true, instead of the default positional merging, object array elements that have a `name` property defined will be merged by their name and the resulting array will be a union of the two input arrays.

Merge by name:

```
arr1: [{name: a, p1: v1}, {name: b, p1: v1}]
arr1: [{name: a, p2: v2}, {name: c, p2: v2}]

merged: [{name: a, p1: v1, p2: v2}, {name: b, p1: v1}, {name: c, p2: v2}]
```

Merging by name can be very convenient when merging container specs or when the base and override templates are not defined together.
