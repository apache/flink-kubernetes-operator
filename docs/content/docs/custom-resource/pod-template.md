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
  image: flink:1.15
  flinkVersion: v1_15
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
  podTemplate:
    apiVersion: v1
    kind: Pod
    metadata:
      name: pod-template
    spec:
      serviceAccount: flink
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
      apiVersion: v1
      kind: Pod
      metadata:
        name: task-manager-pod-template
      spec:
        initContainers:
          # Sample sidecar container
          - name: busybox
            image: busybox:latest
            command: [ 'sh','-c','echo hello from task manager' ]
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
```

{{< hint info >}}
When using the operator with Flink native Kubernetes integration, please refer to [pod template field precedence](
https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/resource-providers/native_kubernetes/#fields-overwritten-by-flink).
{{< /hint >}}
