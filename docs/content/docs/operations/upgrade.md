---
title: "Operator Upgrades"
weight: 4
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

# Operator Upgrade Process

This page details the process of upgrading the operator to a new version.

Please check the [compatibility page]({{< ref "docs/operations/compatibility" >}}) for the complete overview of the backward compatibility guarantees before upgrading to new versions.

{{< hint danger >}}
Upgrading from the preview/experimental `v1alpha1` release to `v1beta1` requires a one time manual process.
Please check the [related section](#upgrading-from-v1alpha1---v1beta1).
{{< /hint >}}

## Normal Upgrade Process

If you are upgrading from `kubernetes-operator-1.0.0` or later, please refer to the following two steps:
1. Upgrading the CRDs
2. Upgrading the Helm deployment

We will cover these steps in detail in the next sections.

### 1. Upgrading the Java client library

If you use the Flink Kubernetes operator Java client library, you need to update it first to ensure that responses from
the new operator version can be parsed properly. For minor releases, the new version of the Java library is
backwards-compatible with the previous minor version of the operator.

### 2. Upgrading the CRD

The first step of the upgrade process is upgrading the CRDs for `FlinkDeployment` and `FlinkSessionJob` resources.
This step must be completed manually and is not part of the helm installation logic.

```sh
kubectl replace -f helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml
kubectl replace -f helm/flink-kubernetes-operator/crds/flinksessionjobs.flink.apache.org-v1.yml
```

{{< hint danger >}}
Please note that we are using the `replace` command here which ensures that running deployments are unaffected.
{{< /hint >}}

### 3. Upgrading the Helm deployment

{{< hint danger >}}
Before upgrading, please compare the version difference between the currently generated yaml and the running yaml, which will be used for backup and restore.
{{< /hint >}}


```sh
helm template flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator  <custom settings> | kubectl diff -f -
```


Once we have the new CRDs versions we can upgrade the Helm deployment:


```sh
helm upgrade flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator <custom settings>
```

or

```sh
# Uninstall running Helm deployment and install new version
helm uninstall flink-kubernetes-operator
helm install flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator <custom settings>
```

The exact installation/upgrade command depends on your current environment and settings. Please see the [helm page]({{< ref "docs/operations/helm" >}}) for details.

## Upgrading from v1alpha1 -> v1beta1

If you are upgrading from `kubernetes-operator-0.1.0` , please refer to the following steps. Because the first stable `v1beta1` release introduced some breaking changes on the operator side when upgrading from the preview (`v1alpha1`) release.
These changes require a one time manual upgrade process for the running jobs.

### 1. Upgrading without existing FlinkDeployments

In an environment without any `FlinkDeployments` you need to uninstall the operator and delete the `v1alpha1` CRD.

```sh
# Uninstall helm deployment
helm uninstall flink-kubernetes-operator

# Delete CRD
kubectl delete crd flinkdeployments.flink.apache.org

# Now reinstall the operator with the new v1beta1 version
helm install flink-kubernetes-operator <helm-repo>/flink-kubernetes-operator <custom settings>
```

### 2. Upgrading with existing FlinkDeployments

The following steps demonstrate the CRD upgrade process from `v1alpha1` to `v1beta1` in an environment with an existing [stateful](https://github.com/apache/flink-kubernetes-operator/blob/main/examples/basic-checkpoint-ha.yaml) job with an old `v1alpha1` apiVersion. After the CRD upgrade, the job will resumed from the savepoint.
Here is a reference example of upgrading a `basic-checkpoint-ha-example` deployment.
1. Suspend the job and create savepoint:
    ```sh
    kubectl patch flinkdeployment/basic-checkpoint-ha-example --type=merge -p '{"spec": {"job": {"state": "suspended", "upgradeMode": "savepoint"}}}'
    ```
    Verify `deploy/basic-checkpoint-ha-example` has terminated and `flinkdeployment/basic-checkpoint-ha-example` has the Last Savepoint Location similar to `file:/flink-data/savepoints/savepoint-000000-aec3dd08e76d/_metadata`. This file will used to restore the job. See [stateful and stateless application upgrade]({{< ref "docs/custom-resource/job-management" >}})  for more detail.

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
    2022-04-13 06:09:40,761 i.j.o.Operator                 [INFO ] Registered reconciler: 'flinkdeploymentcontroller' for resource: 'class org.apache.flink.kubernetes.operator.crd.FlinkDeployment' for namespace(s): [all namespaces]
    2022-04-13 06:09:40,943 i.f.k.c.i.VersionUsageUtils    [WARN ] The client is using resource type 'flinksessionjobs' with unstable version 'v1beta1'
    2022-04-13 06:09:41,461 i.j.o.Operator                 [INFO ] Registered reconciler: 'flinksessionjobcontroller' for resource: 'class org.apache.flink.kubernetes.operator.crd.FlinkSessionJob' for namespace(s): [all namespaces]
    2022-04-13 06:09:41,464 i.j.o.Operator                 [INFO ] Operator SDK 2.1.2 (commit: a3a81ef) built on 2022-03-15T09:59:42.000+0000 starting...
    2022-04-13 06:09:41,464 i.j.o.Operator                 [INFO ] Client version: 5.12.1
    2022-04-13 06:09:41,499 i.f.k.c.i.VersionUsageUtils    [WARN ] The client is using resource type 'flinkdeployments' with unstable version 'v1beta1'
    ```
5. Restore the job:

   Deploy the previously deleted job using this [FlinkDeployemnt](https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic-checkpoint-ha.yaml) with `v1beta1` and explicitly set the `job.initialSavepointPath` to the savepoint location obtained from the step 1.

    ```
    spec:
      ...
      job:
        initialSavepointPath: /flink-data/savepoints/savepoint-000000-aec3dd08e76d/_metadata
        upgradeMode: savepoint
      ...
    ```
    Alternatively, we may use this command to edit and deploy the manifest:
    ```sh
    wget -qO - https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic-checkpoint-ha.yaml| yq w - "spec.job.initialSavepointPath" "/flink-data/savepoints/savepoint-000000-aec3dd08e76d/_metadata"| kubectl apply -f -
    ```
   Finally, verify that `deploy/basic-checkpoint-ha-example` log has:
    ```
    Starting job 00000000000000000000000000000000 from savepoint /flink-data/savepoints/savepoint-000000-2f40a9c8e4b9/_metadata
    ```

### 3. Changes of default values of FlinkDeployment
There are some changes or improvement of default values in the fields of the FlinkDeployment in `v1beta1`:
1. Default value of `crd.spec.Resource#cpu` is `1.0`.
2. Default value of `crd.spec.JobManagerSpec#replicas` is `1`.
3. No default value of `crd.spec.FlinkDeploymentSpec#serviceAccount` and users must specify its value explicitly.
