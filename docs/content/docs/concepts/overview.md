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
Flink Kubernetes Operator acts as a control plane to manage the complete deployment lifecycle of Apache Flink applications. Although Flink’s native Kubernetes integration already allows you to directly deploy Flink applications on a running Kubernetes(k8s) cluster, [custom resources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) and the [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) have also become central to a Kubernetes native deployment experience.

Flink Kubernetes Operator aims to capture the responsibilities of a human operator who is managing Flink deployments. Human operators have deep knowledge of how Flink deployments ought to behave, how to start clusters, how to deploy jobs, how to upgrade them and how to react if there are problems. The main goal of the operator is the automation of these activities, which cannot be achieved through the Flink native integration alone.

# Features

| Category               | Feature                        | Support | Comment                              |
|------------------------|--------------------------------|---------|--------------------------------------|
| Kubernetes integration | Flink Native                   | full    |                                      |
|                        | Standalone                     | no      |                                      |
| Deployment Mode        | Application Mode               | full    |                                      |
|                        | Session Mode                   | limited | no job management                    |
| Lifecycle Management   | Start Job                      | full    | empty state or from savepoint        |
|                        | Upgrade Job                    | full    | stateless or last-state(chkp/svp)    |
|                        | Delete Job                     | full    |                                      |
|                        | Pause/Resume Job               | full    |                                      |
|                        | Savepoint Management           | limited | manual savepoint triggering only     |
|                        | HA                             | full    | via flink native k8s HA              |
|                        | Validation                     | full    | webhook and operator based           |
| Configuration          | Operator configuration         | full    | defaults and helm values override    |
|                        | Native Flink properties        | full    | defaults and job level override      |
|                        | Environment variables          | full    | via pod templates                    |
|                        | Native Kubernetes POD settings | full    | via pod templates                    |
| Operations             | Installation                   | limited | Helm based, no public repos used     |
|                        | UI Access                      | limited | domain based routing only            |
|                        | Operator Log Aggregation       | full    | k8s native and/or custom appender    |
|                        | Operator Metric Aggregation    | limited | basic process metrics only           |
|                        | Job Logs                       | full    | k8s native and/or custom appender    |
|                        | Job Metrics                    | full    | k8s native and/or custom appender    |
|                        | K8s Events                     | limited | deployment events only               |
|                        | Error Handling and Recovery    | limited | non-configurable exponential backoff |
| Pod Augment            | Pod Template                   | full    |                                      |
|                        | Init containers                | full    |                                      |
|                        | Sidecar containers             | full    |                                      |
|                        | Layering                       | full    | jm/tm level override                 |
| Job Type               | Jar job                        | full    |                                      |
|                        | SQL Job                        | no      |                                      |
|                        | Python Job                     | no      |                                      |
| CI/CD                  | Continuous Integration         | full    | via github actions                   |
|                        | Public Docker repository       | full    | ghcr.io / dockerhub                  |
|                        | Public Helm repository         | full    | apache release repo                  |
