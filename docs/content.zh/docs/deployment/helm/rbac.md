---
title: "RBAC"
weight: 2
type: docs
aliases:
- /docs/operations/rbac/
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

The operator and the Flink clusters it manages act on the Kubernetes API under their own identities: each side runs with a dedicated service account, bound to a Kubernetes [role](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#role-and-clusterrole) carrying only the permissions that side needs. This page describes the two roles, how the Helm chart creates and scopes them, and the extra step for running jobs in additional namespaces.

The `flink-operator` role is used by the operator itself to manage the Flink custom resources and everything materialized from them: the JobManager deployments together with their services, ConfigMaps, secrets, events, ingresses, and the leader-election Lease. The `flink` role is used by the JobManagers of the jobs to create and manage the TaskManagers and ConfigMaps of the job.

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/deployment/rbac.svg" alt="Flink Operator RBAC Model" >}}

The service accounts, roles, and bindings are created by the Helm chart (`rbac.create`, on by default), as part of the [Installation]({{< ref "docs/deployment/helm/installation" >}}). By default the `flink-operator` role is cluster scoped (created as a ClusterRole), allowing a single operator instance to be responsible for all Flink deployments in the cluster regardless of the namespace they are deployed to, with one extra step for additional namespaces, described under [Running Jobs in Other Namespaces](#running-jobs-in-other-namespaces). Certain environments are more restrictive and only allow namespaced roles, which is supported through [watchNamespaces]({{< ref "docs/deployment/helm/installation#watching-only-specific-namespaces" >}}).

The `flink` role is always namespaced. By default it is created in the namespace of the operator, and when `watchNamespaces` is enabled it is created for every watched namespace individually.

A few chart details around this model:

- The service account names default to `flink-operator` and `flink`, configurable through `operatorServiceAccount.name` and `jobServiceAccount.name`.
- With `rbac.nodesRule.create: true` the operator role additionally gains node listing, used for REST services of NodePort type and by the autoscaler's cluster capacity check, as described under [Security]({{< ref "docs/deployment/security#access-control" >}}).
- The `flink` service account and its roles carry the `helm.sh/resource-policy: keep` annotation and survive a Helm uninstall, as described under [Uninstallation]({{< ref "docs/deployment/helm/installation#uninstallation" >}}).

## Running Jobs in Other Namespaces

A cluster-scoped operator watches every namespace, but after installation the `flink` service account its jobs run with exists only in the operator's own namespace. Running Flink jobs in any other namespace requires creating the `flink` service account, role, and role binding there first.

For each additional namespace that runs Flink jobs:
1. Switch to the namespace:
    ```sh
    kubectl config set-context --current --namespace=<namespace>
    ```
2. Create the service account, role, and role binding:
    ```sh
    kubectl apply -f - <<EOF
    apiVersion: v1
    kind: ServiceAccount
    metadata:
      labels:
        app.kubernetes.io/name: flink-kubernetes-operator
      name: flink
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      labels:
        app.kubernetes.io/name: flink-kubernetes-operator
      name: flink
    rules:
    - apiGroups:
      - ""
      resources:
      - pods
      - configmaps
      verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
    - apiGroups:
      - apps
      resources:
      - deployments
      - deployments/finalizers
      verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: RoleBinding
    metadata:
      labels:
        app.kubernetes.io/name: flink-kubernetes-operator
      name: flink-role-binding
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: Role
      name: flink
    subjects:
    - kind: ServiceAccount
      name: flink
    EOF
    ```
3. Optionally deploy an example Flink job in the namespace, from the root of the cloned flink-kubernetes-operator repository:
    ```sh
    kubectl apply -f examples/basic.yaml
    ```
