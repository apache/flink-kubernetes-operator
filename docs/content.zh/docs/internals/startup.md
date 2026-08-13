---
title: "Startup"
weight: 2
type: docs
aliases:
- /internals/startup-flow.html
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

# Startup

This page traces how the operator starts, in two stages. First the Helm chart materializes the CRDs, the RBAC sets, the configuration, and the operator Deployment on the cluster. Then the JVM enters at `FlinkOperator` and wires configuration, SPI plugins, shared services, and controllers with the Java Operator SDK (JOSDK), until the informers sync and reconciliation begins.

## From Helm to JVM

Before the operator's Java code runs, the chart materializes the operator on the cluster:

 - `crds/*` are applied first (before any template renders), registering the four Flink CRDs on the API server.
 - Helm merges `values.yaml` with any user overrides from `custom-values.yaml`, and `_helpers.tpl` produces the names and labels used across templates.
 - `controller/configmap.yaml` creates the `flink-operator-config` ConfigMap. `rbac/*` and `flink/*` create two RBAC sets, one for the operator itself and one for the managed Flink JM/TM pods. `webhook/*` plus `cert-manager/*` create the admission Service, Secret, webhook configurations, Issuer, and Certificate when the webhook is enabled. `controller/deployment.yaml` produces the `flink-kubernetes-operator` Deployment.
 - Kubernetes reconciles the Deployment into a Pod with an operator container (plus a webhook sidecar when enabled). The operator container runs `docker-entrypoint.sh`, which preloads jemalloc and `exec`s the JVM with the assembled classpath.
 - The JVM begins execution at `FlinkOperator.main()`.

<div style="display: flex; justify-content: center;">

```text
helm install -f custom-values.yaml  flink-kubernetes-operator  helm/flink-kubernetes-operator  <helm-params> ─┐
                └─── .Values ────┘  └──── .Release.Name ────┘                                                 │
              ┌───────────────────────────────────────────────────────────────────────────────────────────────┘
              │
              │
   ╔═════════════ Phase 1: crds/* applied first (static YAML, no templating) ═════════════╗
   ║          │                                                                           ║
   ║          ▼                                                                           ║
   ║   crds/*.yml ─► CRDs registered with API server:                                     ║
   ║          │        flinkdeployments,                                                  ║
   ║          │        flinksessionjobs,                                                  ║
   ║          │        flinkstatesnapshots,                                               ║
   ║          │        flinkbluegreendeployments                                          ║
   ╚══════════════════════════════════════════════════════════════════════════════════════╝
              │
              │
   ╔════════════ Phase 2: templates rendered with values and helpers, applied ════════════╗
   ║          │                                                                           ║
   ║          ▼                                                                           ║
   ║   custom-values.yaml                                                                 ║
   ║          │  user overrides                                                           ║
   ║          ▼                                                                           ║
   ║    values.yaml ──────────────────────────────────────────────────────► _helpers.tpl  ║
   ║  (chart defaults)                                                            │       ║
   ║          │                                                                   │       ║
   ║          ▼                                                                   │       ║
   ║   controller/configmap.yaml ─► flink-operator-config ConfigMap is created    │       ║
   ║          │                                                                   │       ║
   ║          ├──► rbac/*         ─► ServiceAccount,                              │       ║
   ║          │                      Role / RoleBinding,                          │       ║
   ║          │                      ClusterRole / ClusterRoleBinding (operator)  │       ║
   ║          │                                                                   │       ║
   ║          ├──► flink/*        ─► ServiceAccount,                              │       ║
   ║          │                      Role / RoleBinding                           │       ║
   ║          │                      (for managed Flink JM/TM)                    │       ║
   ║          │                                                                   │       ║
   ║          ├──► webhook/*      ─► Service,                                     │       ║
   ║          │   (optional)         Secret,                                      │       ║
   ║          │                      ValidatingWebhookConfiguration,              │       ║
   ║          │                      MutatingWebhookConfiguration                 │       ║
   ║          │                                                                   │       ║
   ║          ├──► cert-manager/* ─► Issuer + Certificate                         │       ║
   ║          │     (optional)       (cert-manager generates the                  │       ║
   ║          │                      webhook-server-cert Secret)                  │       ║
   ║          │                                                                   │       ║
   ║          └──────────► controller/deployment.yaml ◄───────────────────────────┘       ║
   ╚══════════════════════════════════════════════════════════════════════════════════════╝
                                  │
                                  │  flink-kubernetes-operator Deployment is created with:
                                  │      ENV: LOG_CONFIG=..., JVM_ARGS=..., OPERATOR_NAMESPACE, ...
                                  │      Volume items: config.yaml,
                                  │                    log4j-operator.properties or logback-operator.xml,
                                  │                    log4j-console.properties or logback-console.xml
                                  ▼
                         Deployment ─► ReplicaSet ─► Pod
                                  │
                                  │  kubelet pulls image, projects ConfigMap volume,
                                  │  starts the operator container (and a webhook sidecar
                                  │  container when webhook is enabled, see the Webhook page)
                                  ▼
                         /docker-entrypoint.sh operator
                                  │  1. cd /flink-kubernetes-operator
                                  │  2. maybe_enable_jemalloc  (sets LD_PRELOAD)
                                  │  3. exec java
                                  ▼
       java -cp <classpath>  $LOG_CONFIG  $JVM_ARGS  FlinkOperator
                                  │
                                  ▼
                    FlinkOperator JVM begins execution
```

</div>

The following notes elaborate on the chart's CRD handling, helper-driven labels, `flink-operator-config` ConfigMap construction, operator `Deployment` passthrough, and the container's environment variables:

 - `crds/` uses Helm's special CRD handling: applied on `helm install` only, untouched by `helm upgrade` and `helm uninstall`. Based on this, Helm CRD upgrades and deletions are out-of-band today, and they need to be manually executed via `kubectl` commands (`kubectl apply -f crds/`, `kubectl delete crd ...`). See [Upgrading the Operator → Upgrading the CRDs]({{< ref "docs/operations/upgrade#2-upgrading-the-crds" >}}) for the supported procedure.
 - `_helpers.tpl` defines a common label set applied to every chart-rendered resource: `app.kubernetes.io/name`, `app.kubernetes.io/version`, `app.kubernetes.io/managed-by`, and `helm.sh/chart`. The same `app.kubernetes.io/name` label is the operator Deployment's `selector.matchLabels`, so a single `kubectl get all -l app.kubernetes.io/name=<release-name>` reaches every chart-created object.
 - The `flink-operator-config` ConfigMap is created only when `defaultConfiguration.create` is set to `true`, and it carries five data keys (`config.yaml`, `log4j-operator.properties`, `log4j-console.properties`, `logback-operator.xml`, `logback-console.xml`) populated with two different strategies:
   - Append: chart's `conf/config.yaml`, `conf/log4j-operator.properties`, and `conf/log4j-console.properties` baselines (when `defaultConfiguration.append=true`), followed by user-supplied `defaultConfiguration."config.yaml"` / `"log4j-operator.properties"` / `"log4j-console.properties"` overrides concatenated on top. The single `config.yaml` entry resolves `defaultConfiguration."config.yaml"` when set, otherwise `defaultConfiguration."flink-conf.yaml"`.
   - Replace: user-supplied `defaultConfiguration."logback-operator.xml"` / `"logback-console.xml"` if set, otherwise the chart's `conf/logback-*.xml` baseline when `defaultConfiguration.append=true`. The two sources are mutually exclusive, not concatenated.
   - In the `config.yaml` entry, `kubernetes.operator.watched.namespaces` and `kubernetes.operator.health.probe.*` are auto-injected when the matching `values.yaml` fields are set.
 - The operator `Deployment` is a standard Pod-spec passthrough configured via `operatorPod.*` and related keys in `values.yaml`:
   - container `name`, `image`, `command`, `ports` (metrics, health-probe), `volumeMounts` (config, artifacts, TLS cert), `env`, `envFrom`, `livenessProbe`, `startupProbe`, `lifecycle.postStart`, and `securityContext`.
   - pod-level scheduling fields `nodeSelector`, `affinity`, `tolerations`, `topologySpreadConstraints`, `priorityClassName`, `serviceAccountName`, `imagePullSecrets`, and optional `initContainers` / `sidecarContainers`.
 - Container env vars come from three sources:
   - chart-fixed: `FLINK_CONF_DIR`, `FLINK_PLUGINS_DIR`, `OPERATOR_NAME`, plus the downward-API ones (`OPERATOR_NAMESPACE`, `HOST_IP`, `POD_IP`, `POD_NAME`).
   - `values.yaml`-driven:
     - `JVM_ARGS` from `jvmArgs.operator`.
     - `LOGGING_FRAMEWORK` from `logging.framework`.
     - `LOG_CONFIG` rendered by the `flink-operator.logConfig` helper based on `logging.framework` (`-Dlog4j.configurationFile=...` or `-Dlogback.configurationFile=...`).
     - When `tls.create=true`: `OPERATOR_KEYSTORE_PATH`, `OPERATOR_TRUSTSTORE_PATH`, `OPERATOR_KEYSTORE_PASSWORD` (password sourced from `tls.secretKeyRef`).
   - user-supplied additions via `operatorPod.env` / `operatorPod.envFrom`.
 - Every value the chart reads is documented on the [Helm → Installation]({{< ref "docs/deployment/helm/installation" >}}) page.

{{< hint info >}}
Only `$LOG_CONFIG` and `$JVM_ARGS` reach the `java` command line. Other container env vars (`OPERATOR_NAMESPACE`, `POD_NAME`, `FLINK_CONF_DIR`, etc.) are read by the operator at runtime via `System.getenv(...)`.
{{< /hint >}}

{{< hint info >}}
For the webhook's runtime flow (TLS issuance via cert-manager, admission request routing through the API server, and the mutator and validator SPI chains), see the [Webhook]({{< ref "docs/internals/webhook" >}}) page.
{{< /hint >}}

## JVM Startup

The operator builds on JOSDK, which builds on the Fabric8 Kubernetes client, and owns only the top layer. Once the JVM enters at `FlinkOperator`, startup is a short, deterministic sequence:

```
 startup
   ├─ load the operator configuration ──► flink-operator-config, defaults plus overrides,
   │                                      re-read at runtime when dynamic config is enabled
   ├─ discover the SPI plugins ─────────► validators and listeners from plugin jars
   ├─ wire the shared services ─────────► the Fabric8 Kubernetes client, metrics, health probe,
   │                                      status and event recorders
   ├─ create the JOSDK operator ────────► configured with the reconciliation pool, the metrics
   │                                      bridge, and the informer error policy
   ├─ register the controllers ─────────► one controller per resource type registered with it,
   │                                      the snapshot controller only when its CRD is installed
   ├─ start the JOSDK operator ─────────► the shutdown hook installed, then every registered
   │                                      controller starts in turn
   ├─ start the informers ──────────────► Fabric8 LIST/WATCH per watched resource, the primary
   │                                      first, secondaries concurrently, gated by cache sync
   └─ serve ────────────────────────────► JOSDK event processing dispatches onto the reconciliation
                                          pool, leader election gating when enabled
```

The process installs a shutdown hook honoring `kubernetes.operator.termination.timeout` (default 10 seconds), and with `kubernetes.operator.leader-election.enabled` only the leader replica runs the controllers, as described under [High Availability → Operator High Availability]({{< ref "docs/deployment/leader-election#operator-high-availability" >}}).

### Configuration

The configuration manager loads the operator's settings at startup and derives every Flink-facing configuration from them. Four configurations matter throughout the operator:

| Config  | Derived From                                                                                                                                                                                                                                                    | Used For                                                                                                     |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| Default | The `flink-operator-config` ConfigMap, refined by per-namespace overrides (`kubernetes.operator.default-configuration.namespace.<namespace>.<key>`) and per-Flink-version overrides (`kubernetes.operator.default-configuration.flink-version.<version>.<key>`) | The operator's own behavior, and the base of everything below                                                |
| Deploy  | The namespace and Flink version defaults, always merged with the current spec's `flinkConfiguration`, plus operator-managed additions such as the resource generation annotation                                                                                | Submitting and upgrading the Flink cluster                                                                   |
| Observe | The same recipe applied to the last reconciled spec and its `flinkConfiguration`, with a session job's own `flinkConfiguration` layered on top of its cluster's, and the runtime configuration layered on top of that                                            | Talking to the running cluster, which must be addressed with the configuration it was actually deployed with |
| Runtime | The running job itself, read through the JobManager configuration, job execution and checkpoint config REST endpoints                                                                                                                                            | Correcting the observed view wherever the job's effective settings differ from the spec                      |

Deploy and observe are the same derivation applied to different specs: deploy reads the spec being rolled out, observe reads the spec that last reached the cluster. The two differ exactly while an upgrade is in flight, and converge again once the new spec lands.

The runtime configuration exists because a spec is a request, not a record of what the job ended up running with. A job's main method can change settings programmatically, and those take precedence over anything the operator submitted. The job status observer therefore reads them back from the cluster and layers them over the observed configuration, so that decisions such as whether checkpointing is enabled are made against the job's real settings. Its lifecycle differs from the derived configurations above:

- It is fetched once per job, and skipped for jobs in a globally terminal state, whose REST endpoints are already gone.
- It is cached per resource and job id, under the same cache size and timeout limits as the derived configurations. A new job id, which every redeploy produces, naturally invalidates it.
- A failed fetch is logged and retried on the next cycle, and the observed configuration stays purely spec-derived until one succeeds.

Three runtime behaviors round out the configuration machinery:

- Derived configurations are cached per resource and spec, at most `kubernetes.operator.config.cache.size` entries (default `1000`), expiring after `kubernetes.operator.config.cache.timeout` (default 10 minutes).
- With `kubernetes.operator.dynamic.config.enabled` (default `true`), the ConfigMap is re-read every `kubernetes.operator.dynamic.config.check.interval` (default 5 minutes), and changed defaults apply without an operator restart.
- The watched namespaces come from the same configuration, and with `kubernetes.operator.dynamic.namespaces.enabled`, namespace changes adjust the controllers' informers at runtime.

{{< hint warning >}}
The runtime configuration covers a mapped subset of the job's settings, mainly the default parallelism, object reuse, the job's global parameters, and the checkpointing, state backend and changelog options, and only for as long as the job is running. Whatever falls outside that subset stays invisible to the operator, including a `config.yaml` baked into the image and environment overrides. The configuration a pipeline actually runs with can therefore still differ from what the operator tracks.
{{< /hint >}}

### Services

The services wired before the controllers start come in two scopes. The Kubernetes client, the configuration manager, the SPI-discovered validator and listener sets, the health probe, the root metric group, and the event recording are process-wide singletons shared by everything. Status recording and the autoscaler exist once per controller, and every reconciliation operates them against the one resource at hand:

| Service                    | Role                                                                                                                                                                                                                                                                                                                                                       |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Validators and listeners   | Discovered once through the SPI: validators enforce the [rules]({{< ref "docs/internals/controllers#rules" >}}) shared with the [webhook]({{< ref "docs/internals/webhook" >}}), listeners receive every event and status change, and both plug in as described under [Plugins → Resource Plugins]({{< ref "docs/deployment/plugins#resource-plugins" >}}) |
| Health probe               | Serves `/health` when `kubernetes.operator.health.probe.enabled` is set (default `true`, port `8085`), fed by the controllers' health and the canary resources, see [Monitoring the Operator]({{< ref "docs/operations/health" >}})                                                                                                                        |
| Metrics                    | The operator's metric group, bridged into the JOSDK and optionally the Kubernetes client, see [Metrics]({{< ref "docs/operations/metrics" >}})                                                                                                                                                                                                             |
| Status and event recording | The per-controller status cache and the Kubernetes event emission, described under [Controllers → Status Updates]({{< ref "docs/internals/controllers#status-updates" >}}) and [Events]({{< ref "docs/operations/events" >}})                                                                                                                              |
| Autoscaler                 | Wired into the deployment and session job controllers, running inside their reconcile passes, see [Internals → Autoscaler]({{< ref "docs/internals/autoscaler" >}})                                                                                                                                                                                        |

### Informers

An informer maintains a live local cache of one watched resource kind: it lists the current state once, watches for changes through the Kubernetes API, and notifies its consumers on every add, update, and delete. Reads go through the cache rather than the API server, and the JOSDK layers its event sources on top of exactly this mechanism, as described in the [Java Operator SDK documentation](https://javaoperatorsdk.io/docs/documentation/eventing/).

Every controller pairs a primary informer with a set of secondaries:

- The primary informer watches the controller's own resource kind. It feeds the reconcile triggers, and its cache is what resource reads are served from.
- Secondary informers watch related resources, and each carries a secondary-to-primary mapping. Every add, update, and delete on a secondary triggers a reconciliation of each primary it maps to, so the owning resource reacts to its dependents without polling them.

| Controller                           | Secondary                                             | Mapped Back By                                                                            |
|--------------------------------------|-------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `FlinkDeployment`                    | The JobManager Deployment and the Ingress             | Their labels, carrying the owning resource's name                                         |
| `FlinkDeployment`                    | `FlinkSessionJob` resources                           | An index on `spec.deploymentName`, a job change re-reconciles its session cluster         |
| `FlinkSessionJob`                    | The parent `FlinkDeployment`                          | The reverse of the same index, a cluster change re-reconciles every job running on it     |
| `FlinkDeployment`, `FlinkSessionJob` | `FlinkStateSnapshot` resources                        | `spec.jobReference`, a snapshot change re-reconciles the job it captures                  |
| `FlinkStateSnapshot`                 | The referenced `FlinkDeployment` or `FlinkSessionJob` | An index on `spec.jobReference`, a job change re-reconciles every snapshot referencing it |
| `FlinkBlueGreenDeployment`           | The child deployments and the active Ingress          | Kubernetes owner references                                                               |

### Threads and Timing

A handful of thread pools carry the whole operator, and their sizes bound what happens concurrently: with 200 custom resources and the default reconciliation pool of 50, at most 50 reconcile at any instant, while the rest have their pending events queued and are picked up as threads free. This works because a reconciliation is typically short, and the pool size is the knob to raise when many resources contend or reconciliations block on slow REST calls.

| Pool                        | Size                                                                          | Used For                                          |
|-----------------------------|-------------------------------------------------------------------------------|---------------------------------------------------|
| Reconciliation pool         | `kubernetes.operator.reconcile.parallelism`, default `50`, `-1` for unbounded | Every reconciliation, one in flight per resource  |
| Flink REST client I/O       | Matches the reconciliation pool size                                          | REST calls against the managed Flink clusters     |
| Informer I/O                | Cached, grows on demand                                                       | LIST and WATCH wire traffic and reconnects        |
| Per-informer dispatch       | A logical serial queue per informer                                           | Delivering events in order                        |
| Retry and reschedule timers | One per controller                                                            | Retry backoff and the periodic reconcile interval |
| Health probe server         | One accept thread plus 2 × cores workers                                      | The `/health` endpoint                            |
| Metrics registry            | Two scheduled threads                                                         | Pushing metrics through the configured reporters  |
| Dynamic config watcher      | One scheduled thread                                                          | Re-reading `flink-operator-config`                |
| Canary checker              | Three scheduled threads                                                       | Canary resource health checks                     |

Standalone-mode deployments add a small Kubernetes client pool of their own.

An event travels one path from the API server to a reconciliation, and every concurrency rule lives at one of its stages. The full fan is drawn here, one funnel per controller with its primary informer first:

```
┌──────────────┐
│              ├─► FlinkDeployment Informer ─────────┐
│              ├─► JM Deployment Informer ───────────┤
│              ├─► Ingress Informer ─────────────────┼─► FlinkDeployment ──────────┐
│              ├─► FlinkSessionJob Informer ─────────┤   event processor           │
│              ├─► FlinkStateSnapshot Informer ──────┘                             │
│              │                                                                   │
│ watch events ├─► FlinkSessionJob Informer ─────────┐                             │
│              ├─► FlinkDeployment Informer ─────────┼─► FlinkSessionJob ──────────┤  ┌─────────────────────────┐
│   Informer   ├─► FlinkStateSnapshot Informer ──────┘   event processor           │  │                         │
│  I/O threads │                                                                   ├─►│   reconciliation pool   │
│              ├─► FlinkStateSnapshot Informer ──────┐                             │  │(one thread per resource)│
│              ├─► FlinkDeployment Informer ─────────┼─► FlinkStateSnapshot ───────┤  │                         │
│              ├─► FlinkSessionJob Informer ─────────┘   event processor           │  └─────────────────────────┘
│              │                                                                   │
│              ├─► FlinkBlueGreenDeployment Informer ┐                             │
│              ├─► FlinkDeployment Informer ─────────┼─► FlinkBlueGreenDeployment ─┘
│              ├─► Ingress Informer ─────────────────┘   event processor
└──────────────┘
```

Each arrow is a non-blocking handoff: a serial stage passes the event on and immediately turns to the next one, so it orders the handoffs without pacing execution. Reconciliations handed over one after another still run at the same time once pool threads pick them up.

The event processor is bookkeeping rather than a pool: each controller has one, fed by all of its informers and timers, and it runs on whichever thread calls into it, the dispatch thread when an event arrives, a timer thread when a retry or the reconcile interval fires, and the finishing pool thread when a folded event needs a follow-up. That is why the pool table above has no row for it, and the four processors all submit into the one shared pool.

These threads draw a clear line between what is parallel and what is serialized:

| Work                                   | Runs                                                                                          |
|----------------------------------------|-----------------------------------------------------------------------------------------------|
| Reconciliations of different resources | In parallel, up to the thread count of the shared pool, all four controllers included         |
| Reconciliations of the same resource   | Strictly one at a time. Events arriving mid-reconciliation are folded into a single follow-up |
| Event delivery within one informer     | Serially, in arrival order                                                                    |
| Event delivery across informers        | Independently, in parallel                                                                    |

The full trigger and concurrency semantics live under [Controllers → Triggers and Concurrency]({{< ref "docs/internals/controllers#triggers-and-concurrency" >}}).

Two timing facts worth keeping in mind:

- Every watch is recycled every 5 to 10 minutes, and reconnects with exponential backoff on API server disconnects.
- Each informer must complete its initial LIST and start its WATCH within the 2-minute cache sync timeout. If `kubernetes.operator.startup.stop-on-informer-error` is left at its default of `true`, exceeding it kills the operator, otherwise startup continues and retries in the background.

With the controllers registered and their informers synced, the startup is complete. Every reconciliation from here on follows the controller pipeline, with its triggers and flow described under [Controllers]({{< ref "docs/internals/controllers" >}}).
