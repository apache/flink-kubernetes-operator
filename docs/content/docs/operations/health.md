---
title: "Operator Health Monitoring"
weight: 3
type: docs
aliases:
- /operations/health.html
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

# Operator Health Monitoring

## Health Probe

The Flink Kubernetes Operator provides a built in health endpoint that serves as the information source for Kubernetes liveness and startup probes.

The liveness and startup probes are enabled by default in the Helm chart:

```
operatorHealth:
  port: 8085
  livenessProbe:
    periodSeconds: 10
    initialDelaySeconds: 30
  startupProbe:
    failureThreshold: 30
    periodSeconds: 10
```

The health endpoint catches startup and informer errors that are exposed by the JOSDK framework. By default if one of the watched namespaces becomes inaccessible the health endpoint will report an error and the operator will restart.

In some cases it is desirable to keep the operator running even if some namespaces are inaccessible. To allow the operator to start even if some namespaces cannot be watched, you can disable the `kubernetes.operator.startup.stop-on-informer-error` flag.

## Canary Resources

The canary resource feature allows users to deploy special dummy resources (canaries) into selected namespaces. The operator health probe will then monitor that these resources are reconciled in a timely manner. This allows the operator health probe to catch any slowdowns, and other general reconciliation issues not covered otherwise.

Canary deployments are identified by a special label: `"flink.apache.org/canary": "true"`. These resources do not need to define a spec and they will not start any pods or consume other cluster resources and are purely there to assert the operator reconciliation functionality.

Canary FlinkDeployment:

```
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: canary
  labels:
    "flink.apache.org/canary": "true"
```

The default timeout for reconciling the canary resources is 1 minute and it is controlled by `kubernetes.operator.health.canary.resource.timeout`. If the operator cannot reconcile the canaries within this time limit the operator is marked unhealthy and will be automatically restarted.

Canaries can be deployed into multiple namespaces.
