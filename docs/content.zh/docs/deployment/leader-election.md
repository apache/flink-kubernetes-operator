---
title: "High Availability"
weight: 5
type: docs
aliases:
- /docs/operations/leader-election/
- /operations/leader-election.html
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

# High Availability

High availability on Kubernetes covers two separate concerns, and this page treats each in turn:

- **Flink Job High Availability** removes the JobManager as a single point of failure, so a running job survives a JobManager crash. It is a property of the Flink cluster and is configured through Flink's own HA services.
- **Operator High Availability** keeps the operator itself available through leader election and standby replicas, so reconciliation continues if the active operator pod is lost.

The two are independent and can be enabled in any combination.

## Flink Job High Availability

A Flink cluster has a single leading JobManager. Without HA that JobManager is a single point of failure: if it crashes, no new jobs can be submitted and running jobs fail. Flink's HA services remove that risk by electing a new leader and persisting the metadata a successor needs to resume (JobGraphs, completed checkpoint pointers, and job status). The operator integrates with these services and builds several of its own features on top of them.

The operator works with both [Kubernetes HA Services](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/) and [ZooKeeper HA Services](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/zookeeper_ha/). Kubernetes HA is the common choice, because it needs no external system.

### Enabling High Availability

HA is not enabled by default. It is turned on through `spec.flinkConfiguration`, the same way as for any Flink cluster:

```yaml
spec:
  flinkConfiguration:
    high-availability.type: kubernetes
    high-availability.storageDir: file:///flink-data/ha
```

`high-availability.type: kubernetes` selects the Kubernetes HA services. `high-availability.storageDir` points to a durable filesystem (S3, GCS, HDFS, or a mounted volume) where JobManager metadata is persisted. The operator also accepts the older factory form, `high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory`.

### Cluster Identity

Flink's [Kubernetes HA setup](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/kubernetes_ha/) normally requires setting `kubernetes.cluster-id` (and, on older versions, `high-availability.cluster-id`) to identify the cluster. The operator sets both of these itself and does not allow configuring them. They are always derived from the custom resource name, and the operator rejects any deployment that sets either key in `spec.flinkConfiguration`:

```
Forbidden Flink config key: kubernetes.cluster-id
```

The same restriction applies to `kubernetes.namespace`.

{{< hint info >}}
Binding the cluster identity to the resource name is what lets the operator locate a cluster's HA metadata across restarts and upgrades, so this is intentional rather than a limitation.
{{< /hint >}}

### Operator Features Built on HA

Beyond surviving a JobManager crash, several operator features depend on HA metadata being present:

- **`last-state` upgrades** restore a job from the latest checkpoint recorded in the HA metadata, without taking a savepoint first. The operator confirms the HA metadata is available before it suspends the job, and a stateful upgrade fails if the JobManager is already gone and no HA metadata remains.
- **Automatic rollback** requires HA. The operator rejects `kubernetes.operator.deployment.rollback.enabled: true` unless HA is active (`HA must be enabled for rollback support.`), because a rollback restores the previous job from its last state.
- **JobManager recovery**. If the JobManager deployment is lost while a job should be running, the operator can redeploy it and let Flink recover from the HA metadata instead of treating the job as failed.

For how the upgrade modes use this, see [Job Management]({{< ref "docs/managing/job-management#upgrades" >}}).

### Standby JobManagers

Flink Job HA works with a single JobManager. Set `spec.jobManager.replicas` greater than 1 to run standby JobManagers as well, the operator maps the field to Flink's [`kubernetes.jobmanager.replicas`](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/#kubernetes-jobmanager-replicas). A standby takes over leadership immediately instead of waiting for a fresh JobManager pod to start, which shortens recovery time.

{{< hint warning >}}
Even with standbys the running job still restarts when the leader JobManager fails. The standby speeds up that restart, it does not make it transparent.
{{< /hint >}}

### JobResultStore Cleanup

To work around a JobResultStore resource leak in Flink ([FLINK-27569](https://issues.apache.org/jira/browse/FLINK-27569)), the operator adjusts two settings whenever an application deployment runs with HA storage configured: `job-result-store.delete-on-commit` is turned off unless set explicitly, and `job-result-store.storage-path` is pointed at a unique directory under the HA storage path on every cluster launch. The directories of earlier launches are never removed automatically, so they accumulate and must be cleaned up manually, always retaining the most recent one:

```shell
ls -lth /tmp/flink/ha/job-result-store/basic-checkpoint-ha-example/
total 0
drwxr-xr-x 2 9999 9999 40 May 12 09:51 119e0203-c3a9-4121-9a60-d58839576f01 <- must be retained
drwxr-xr-x 2 9999 9999 60 May 12 09:46 a6031ec7-ab3e-4b30-ba77-6498e58e6b7f
drwxr-xr-x 2 9999 9999 60 May 11 15:11 b6fb2a9c-d1cd-4e65-a9a1-e825c4b47543
```

### Limitations

The following are known limitations of the operator's Flink Job HA integration:

- **A `stateless` job may recover state after becoming unhealthy.** When HA is enabled and the operator resubmits a job that entered an unhealthy state, the resubmission can fall back to last-state recovery regardless of the configured `upgradeMode`. A job set to `upgradeMode: stateless` may therefore restart from HA metadata instead of from empty state.
- **HA metadata is retained for terminally finished jobs.** When a bounded job reaches a terminal `FINISHED` state and its JobManager deployment is removed, the operator keeps the HA metadata for a possible last-state restore even though it is no longer needed. The resource then stays in the reconcile loop and the leftover HA data has to be removed manually.
- **Automatic recovery uses the latest checkpoint, not periodic savepoints.** The restore path recorded when a job is first submitted is not refreshed as periodic savepoints are taken. HA-driven recovery always resumes from the latest completed checkpoint in the HA metadata, so treat periodic savepoints as manual backup or fork points rather than automatic recovery points.
- **REST mutual TLS with HA on Flink 1.18.** On Flink 1.18, enabling REST SSL mutual authentication (`security.ssl.rest.authentication-enabled: true`) together with Kubernetes HA breaks the operator's REST client with an `Empty server certificate chain` handshake error. Enable only one of the two on that version.
- **HA in the Standalone operator deployment mode.** In [Standalone mode]({{< ref "docs/deployment/overview#operator-deployment-modes" >}}) the operator passes the JobManager pod IP through the `--host` argument, which is known to interfere with HA leader election and failover. Prefer Native mode when Flink Job HA is required.

## Operator High Availability

The operator itself runs as a Kubernetes Deployment. By default it runs a single replica with the `Recreate` update strategy, so during an operator restart or upgrade there is a short window with no operator running. The Flink jobs keep running during that window, but nothing reconciles them until the operator is back.

To remove that gap, run standby operator replicas with leader election. This is built on the Java Operator SDK [Leader Election](https://javaoperatorsdk.io/docs/documentation/operations/leader-election/) feature. Several operator replicas run at once, but only the one holding the lease actively reconciles. If the leader fails or is rescheduled, a standby acquires the lease and takes over.

### Enabling Leader Election

Leader election is off by default. Enable it with two mandatory options in the operator configuration:

```yaml
kubernetes.operator.leader-election.enabled: true
kubernetes.operator.leader-election.lease-name: flink-operator-lease
```

The lease name must be unique among leases in the operator's namespace. The remaining options tune the lease timing and can be left at their defaults:

| Option                                               | Default | Purpose                                                                         |
|------------------------------------------------------|---------|---------------------------------------------------------------------------------|
| `kubernetes.operator.leader-election.lease-duration` | 15s     | How long a lease stays valid before the holder must renew it.                   |
| `kubernetes.operator.leader-election.renew-deadline` | 10s     | Grace period in which the current leader must renew before releasing the lease. |
| `kubernetes.operator.leader-election.retry-period`   | 2s      | How often a standby retries to acquire the lease.                               |

These options are also listed in the [System Configuration → Startup]({{< ref "docs/deployment/configuration#system-startup" >}}) reference table.

### Running Multiple Replicas

With leader election enabled, raise the operator `replicas` and switch the update strategy to `RollingUpdate` in the Helm values:

```yaml
replicas: 2
strategy:
  type: RollingUpdate
```

{{< hint warning >}}
Do not raise `replicas` above 1 without enabling leader election first. Multiple active reconcilers competing for the same resources produce conflicting updates and event spam. The Helm defaults (`replicas: 1`, `strategy.type: Recreate`) enforce the safe single-instance setup.
{{< /hint >}}

### Migrating from a Single Replica

Enabling leader election on an operator that is already running needs care. Leader election is an operator configuration setting that is read only at startup. Raising `replicas` and switching `strategy` change the Deployment spec but not the operator pod template, so Kubernetes does not restart the instance that is already running, it only starts an additional pod. The new pod comes up with leader election enabled and takes the lease, while the original pod keeps reconciling with leader election still disabled. That leaves two active reconcilers at once, the exact conflict leader election is meant to prevent, and it persists until the original pod is restarted.

To migrate safely, bring the running instance down and back up with the new configuration instead of leaving it in place. For example, scale the operator Deployment to zero, apply the change (leader election enabled, the higher `replicas`, and `strategy: RollingUpdate`), then let it scale back up. Every pod then starts leader-election-aware, one acquires the lease, and the standbys stay idle until they are needed. This brief control-plane gap does not affect running Flink jobs. Once leader election is active, later operator upgrades roll one pod at a time with no reconciliation gap.

### Topology Spread Constraints

When `replicas` is greater than 1, configure [topologySpreadConstraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/) through `operatorPod.topologySpreadConstraints` in the Helm values so standby instances land on different nodes (or availability zones, depending on the topology key). Otherwise a single node failure can take down every replica at once.

```yaml
operatorPod:
  topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: kubernetes.io/hostname
      whenUnsatisfiable: DoNotSchedule
      labelSelector:
        matchLabels:
          app.kubernetes.io/name: flink-kubernetes-operator
```

### Verifying Leader Election

At startup every operator pod logs whether leader election is on, printing one of:

```
Operator leader election is enabled.
Operator leader election is disabled.
```

The Java Operator SDK then logs a leadership-acquired event on the pod that wins the lease. To see which pod currently holds it:

```shell
kubectl get lease <lease-name> -n <operator-namespace> -o jsonpath='{.spec.holderIdentity}'
```

The `holderIdentity` matches the `HOSTNAME` of the active operator pod. Only that pod reconciles. The standbys stay idle until they take over.

### Failure Behavior

If the active operator fails to renew its lease within `lease-duration` (default 15s), a standby can acquire it after its own `retry-period` (default 2s). The `renew-deadline` (default 10s) is the window in which the current holder must succeed in renewing before it voluntarily releases the lease. Tightening these values shortens failover time, at the cost of more API traffic and less tolerance for transient API server slowness.
