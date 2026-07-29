---
title: "Upgrading the Operator"
weight: 7
type: docs
aliases:
- /operations/upgrade.html
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

# Upgrading the Operator

This page details the process of upgrading the operator to a new version.

{{< hint warning >}}
Upgrading from the preview `v1alpha1` release to `v1beta1` requires a one-time manual process, described in the [related section](#upgrading-from-v1alpha1---v1beta1) below.
{{< /hint >}}

## Before Upgrading

Every operator release moves the compatibility envelope, so the [Compatibility]({{< ref "docs/deployment/compatibility" >}}) page is the first stop before an upgrade. Three checks matter in practice:

- **Flink versions**: a new operator release can deprecate or drop support for older Flink versions. Verify the `spec.flinkVersion` of every managed pipeline against the target release's [Supported Flink Versions]({{< ref "docs/deployment/compatibility#supported-flink-versions" >}}): resources on a dropped version are rejected at validation after the upgrade, so those pipelines have to be migrated to a supported Flink version first.
- **Kubernetes**: a new release can raise the minimum supported Kubernetes version or start relying on newer API objects. Check the [Kubernetes]({{< ref "docs/deployment/compatibility#kubernetes" >}}) requirements against the cluster's API server version.
- **Helm chart**: the chart's values schema is allowed to change shape between releases. Review any custom values against the new chart's `values.yaml`, as described under [Helm Chart]({{< ref "docs/deployment/compatibility#helm-chart" >}}).

The backward compatibility guarantees for already deployed resources are described under [Custom Resource]({{< ref "docs/deployment/compatibility#custom-resource" >}}).

## Normal Upgrade Process

When upgrading from `kubernetes-operator-1.0.0` or later, follow these three steps:
1. Upgrading the Java Client Library
2. Upgrading the CRDs
3. Upgrading the Helm Deployment

The steps are covered in detail in the next sections.

### 1. Upgrading the Java Client Library

When the Flink Kubernetes Operator Java client library is used, it needs to be updated first, ensuring that responses from
the new operator version can be parsed properly. For minor releases, the new version of the Java library is
backwards-compatible with the previous minor version of the operator.

### 2. Upgrading the CRDs

This step upgrades the CRDs for the `FlinkDeployment`, `FlinkBlueGreenDeployment`, `FlinkSessionJob` and `FlinkStateSnapshot` resources.
It must be completed manually and is not part of the Helm installation logic.

```sh
kubectl replace -f helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml
kubectl replace -f helm/flink-kubernetes-operator/crds/flinkbluegreendeployments.flink.apache.org-v1.yml
kubectl replace -f helm/flink-kubernetes-operator/crds/flinksessionjobs.flink.apache.org-v1.yml
kubectl replace -f helm/flink-kubernetes-operator/crds/flinkstatesnapshots.flink.apache.org-v1.yml
```

{{< hint warning >}}
The `replace` command ensures that running deployments are unaffected.
If the CRD does not exist yet, the command fails, and `kubectl apply` should be used instead.
{{< /hint >}}

### 3. Upgrading the Helm Deployment

{{< hint warning >}}
Before upgrading, diff the newly rendered manifests against the running ones, and keep a copy of the current manifests as a backup for rollback:
{{< /hint >}}


```sh
helm template flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator <custom settings> | kubectl diff -f -
```


With the new CRD versions in place, the Helm deployment can be upgraded:


```sh
helm upgrade flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator <custom settings>
```

or

```sh
# Uninstall running Helm deployment and install new version
helm uninstall flink-kubernetes-operator
helm install flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator <custom settings>
```

The exact installation or upgrade command depends on the current environment and settings. See the Helm [Installation]({{< ref "docs/deployment/helm/installation" >}}) page for details.

## Upgrading from v1alpha1 -> v1beta1

When upgrading from `kubernetes-operator-0.1.0`, refer to the following steps, as the first stable `v1beta1` release introduced some breaking changes on the operator side compared to the preview (`v1alpha1`) release.
These changes require a one-time manual upgrade process for the running jobs.

### 1. Upgrading without Existing FlinkDeployments

In an environment without any `FlinkDeployments`, the operator needs to be uninstalled and the `v1alpha1` CRD deleted.

```sh
# Uninstall helm deployment
helm uninstall flink-kubernetes-operator

# Delete CRD
kubectl delete crd flinkdeployments.flink.apache.org

# Now reinstall the operator with the new v1beta1 version
helm install flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator <custom settings>
```

### 2. Upgrading with Existing FlinkDeployments

The following steps demonstrate the CRD upgrade process from `v1alpha1` to `v1beta1` in an environment with an existing [stateful](https://github.com/apache/flink-kubernetes-operator/blob/main/examples/basic-checkpoint-ha.yaml) job with an old `v1alpha1` apiVersion. After the CRD upgrade, the job will be resumed from the savepoint.
Here is a reference example of upgrading a `basic-checkpoint-ha-example` deployment.
1. Suspend the job and create savepoint:
    ```sh
    kubectl patch flinkdeployment/basic-checkpoint-ha-example --type=merge -p '{"spec": {"job": {"state": "suspended", "upgradeMode": "savepoint"}}}'
    ```
    Verify `deploy/basic-checkpoint-ha-example` has terminated and `flinkdeployment/basic-checkpoint-ha-example` has the Last Savepoint Location similar to `file:/flink-data/savepoints/savepoint-000000-aec3dd08e76d/_metadata`. This file will be used to restore the job. See [stateful and stateless application upgrades]({{< ref "docs/managing/job-management#upgrades" >}}) for more detail.

2. Delete the job:
   ```sh
   kubectl delete flinkdeployment/basic-checkpoint-ha-example
   ```

3. Uninstall flink-kubernetes-operator helm chart and the CRD with the old `v1alpha1` version:
    ```sh
    helm uninstall flink-kubernetes-operator
    kubectl delete crd flinkdeployments.flink.apache.org
    ```
4. Reinstall the flink-kubernetes-operator helm chart with the `v1beta1` CRD
    ```sh
    helm repo update flink-operator-repo
    helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
    ```
    Verify the `deploy/flink-kubernetes-operator` log has:
    ```
    i.j.o.Operator                 [INFO ] Registered reconciler: 'flinkdeploymentcontroller' for resource: 'class org.apache.flink.kubernetes.operator.crd.FlinkDeployment' for namespace(s): [all namespaces]
    i.f.k.c.i.VersionUsageUtils    [WARN ] The client is using resource type 'flinksessionjobs' with unstable version 'v1beta1'
    i.j.o.Operator                 [INFO ] Registered reconciler: 'flinksessionjobcontroller' for resource: 'class org.apache.flink.kubernetes.operator.crd.FlinkSessionJob' for namespace(s): [all namespaces]
    i.j.o.Operator                 [INFO ] Operator SDK 2.1.2 (commit: a3a81ef) built on 2022-03-15T09:59:42.000+0000 starting...
    i.j.o.Operator                 [INFO ] Client version: 5.12.1
    i.f.k.c.i.VersionUsageUtils    [WARN ] The client is using resource type 'flinkdeployments' with unstable version 'v1beta1'
    ```
5. Restore the job:

   Deploy the previously deleted job using this [FlinkDeployment](https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic-checkpoint-ha.yaml) with `v1beta1` and explicitly set the `job.initialSavepointPath` to the savepoint location obtained from the step 1.

    ```
    spec:
      ...
      job:
        initialSavepointPath: /flink-data/savepoints/savepoint-000000-aec3dd08e76d/_metadata
        upgradeMode: savepoint
      ...
    ```
    Alternatively, the manifest can be edited and deployed with:
    ```sh
    wget -qO - https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic-checkpoint-ha.yaml| yq w - "spec.job.initialSavepointPath" "/flink-data/savepoints/savepoint-000000-aec3dd08e76d/_metadata"| kubectl apply -f -
    ```
   Finally, verify that `deploy/basic-checkpoint-ha-example` log has:
    ```
    Starting job 00000000000000000000000000000000 from savepoint /flink-data/savepoints/savepoint-000000-aec3dd08e76d/_metadata
    ```

### 3. Changes of Default Values of FlinkDeployment
There are some changes or improvement of default values in the fields of the FlinkDeployment in `v1beta1`:
1. Default value of `crd.spec.Resource#cpu` is `1.0`.
2. Default value of `crd.spec.JobManagerSpec#replicas` is `1`.
3. No default value of `crd.spec.FlinkDeploymentSpec#serviceAccount`, its value must be specified explicitly.
