---
title: "RBAC model"
weight: 2
type: docs
aliases:
- /operations/rbac.html
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

# Role-based Access Control Model

To be able to deploy the operator itself and Flink jobs, we define two separate Kubernetes 
[roles](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#role-and-clusterrole). 
The former, called `flink-operator` role is used to manage the `flinkdeployments`, to create and manage the 
[JobManager](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/#jobmanager) deployment
for each Flink job and other resources like [services](https://kubernetes.io/docs/concepts/services-networking/service/).
The latter, called the `flink` role is used by the JobManagers of the jobs to create and manage the 
[TaskManagers](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/#taskmanagers) and
[ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/) for the job.

{{< img src="/img/operations/rbac.svg" alt="Flink Operator RBAC Model" >}}

These service accounts and roles can be created via the operator Helm [chart]({{< ref "docs/operations/helm" >}}).
By default the `flink-operator` role is cluster scoped (created as a `clusterrole`) and thus allowing a single operator
instance to be responsible for all Flink deployments in a Kubernetes cluster regardless of the namespace they are
deployed to. Certain environments are more restrictive and only allow namespaced roles, so we also support this option
via [watchNamespaces]({{< ref "docs/operations/helm" >}}#watching-only-specific-namespaces).

The `flink` role is always namespaced, by default it is created in the namespace of the operator. When 
[watchNamespaces]({{< ref "docs/operations/helm" >}}#watching-only-specific-namespaces) is enabled it is created for all
watched namespaces individually. 