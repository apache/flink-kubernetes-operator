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

This page traces what happens when the operator pod boots, from the `FlinkOperator` main entry point through configuration loading, SPI discovery, recorder and autoscaler wiring, controller registration with the Java Operator SDK (JOSDK), and the launch of the health probe and metrics services.

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
                                  │      Volume items: config.yaml (default) or flink-conf.yaml,
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
 - The `flink-operator-config` ConfigMap is created only when `defaultConfiguration.create` is set to `true`, and it carries six data keys (`config.yaml`, `flink-conf.yaml`, `log4j-operator.properties`, `log4j-console.properties`, `logback-operator.xml`, `logback-console.xml`) populated with two different strategies:
   - **Append**: chart's `conf/flink-conf.yaml`, `conf/log4j-operator.properties`, and `conf/log4j-console.properties` baselines (when `defaultConfiguration.append=true`), followed by user-supplied `defaultConfiguration."config.yaml"` / `"flink-conf.yaml"` / `"log4j-operator.properties"` / `"log4j-console.properties"` overrides concatenated on top.
   - **Replace**: user-supplied `defaultConfiguration."logback-operator.xml"` / `"logback-console.xml"` if set, otherwise the chart's `conf/logback-*.xml` baseline when `defaultConfiguration.append=true`. The two sources are mutually exclusive, not concatenated.
   - For the two top-level YAML keys (`config.yaml`, `flink-conf.yaml`), `kubernetes.operator.watched.namespaces` and `kubernetes.operator.health.probe.*` are auto-injected when the matching `values.yaml` fields are set.
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

Once the JVM begins at `FlinkOperator.main()`, the reconciliation backbone is built on top of three layered libraries. This section names the components, sketches the startup path, and lists the threads and timing primitives. It is intentionally a high-level baseline.

### Backbone Components

{{< img src="/img/internals/backbone-layers.svg" alt="Flink operator startup backbone layers" >}}

The Flink Operator owns only the top layer and everything below is library code, swappable in principle. These docs do not deep-dive into JOSDK or Fabric8 details, those live in their respective project sources. This section gives just enough vocabulary to read the rest of the Internals pages.

{{< hint info >}}
The diagram is intentionally high-level and only highlights the most important backbone components. A few things to keep in mind:

 - Not every JOSDK or Fabric8 component is shown. The omitted internals (in **bold** below) sit between the boxes already drawn and are covered in their respective project sources:
   - JOSDK registration chain: `Controller` -> **`EventSourceContext`** -> **`EventSources`** -> `EventSourceManager` -> `InformerEventSource` -> **`InformerManager`** -> **`InformerWrapper`** -> Fabric8 informer. Reconcile dispatch chain: `EventProcessor` -> **`ReconciliationDispatcher`** -> **`ManagedWorkflow`** -> `Reconciler`.
   - Fabric8 watch chain: `KubernetesClient` -> **`Reflector`** -> **`ReflectorWatcher`** -> `DefaultSharedIndexInformer` -> `ProcessorStore` -> **`SharedProcessor`** -> `ProcessorListener` -> JOSDK event source.
 - `KubernetesOperatorMetricGroup` connects to many more components than the diagram shows (each per-controller context, each `StatusRecorder`, autoscaler internals, etc.). Those edges were omitted for readability.
 - The `StatusRecorder` inside the `FlinkBlueGreenDeployment` cluster is also wired into the `FlinkResourceListener` chain, the same way as the `StatusRecorder` in the other three clusters. The arrow was omitted for the same readability reason.
 - `DefaultSharedIndexInformer` is drawn as one box per controller for readability, but at runtime there is one `DefaultSharedIndexInformer` per registered `EventSource`. With default settings that is 5 per `FlinkDeploymentController` (one primary plus `Deployment`, `Ingress`, `SessionJob`, `StateSnapshot`), 3 per `FlinkSessionJobController`, 3 per `FlinkStateSnapshotController`, and up to 3 per `FlinkBlueGreenDeploymentController`.
{{< /hint >}}

The table below describes the components highlighted in the diagram above.

| Component                                                                                                                            | Layer          | Purpose                                                                                                                                                                                                                                                                    |
|--------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FlinkOperator`                                                                                                                      | Flink Operator | Operator entry point (`main()`). Builds the shared dependencies (`KubernetesClient`, `EventRecorder`, `FlinkResourceContextFactory`, validators, listeners), registers the four controllers with JOSDK, and starts the operator.                                           |
| `FlinkConfigManager`                                                                                                                 | Flink Operator | Manages the operator's effective configuration. Loads `flink-operator-config` on startup, watches for hot-reloads when `kubernetes.operator.dynamic.config.enabled=true`, and caches per-resource configs.                                                                 |
| `OperatorHealthService`                                                                                                              | Flink Operator | Serves the operator's `/health` HTTP endpoint when `kubernetes.operator.health.probe.enabled=true`, fed by `HealthProbe.INSTANCE`.                                                                                                                               |
| `KubernetesOperatorMetricGroup`                                                                                                      | Flink Operator | Root metric group for the operator JVM. Bridged to JOSDK via `OperatorJosdkMetrics` and consumed by every `MetricManager`, `StatusRecorder`, autoscaler component, and the `KubernetesClient` itself.                                                                      |
| `FlinkResourceListener`                                                                                                              | Flink Operator | SPI. Discovered once at startup via `ListenerUtils.discoverListeners(...)`. Receives every event written by every `EventRecorder` and every status change emitted by every `StatusRecorder`.                                                                               |
| `FlinkResourceValidator`                                                                                                             | Flink Operator | SPI. Discovered once at startup via `ValidatorUtils.discoverValidators(...)`. Validates incoming specs both at admission time (webhook process) and inside the reconcile loop.                                                                                             |
| Reconciler implementations (`ApplicationReconciler`, `SessionReconciler`, `SessionJobReconciler`, `StateSnapshotReconciler`)         | Flink Operator | The actual business logic for each CR type. `FlinkDeploymentController` selects between `ApplicationReconciler` (application mode) and `SessionReconciler` (session mode) via `ReconcilerFactory`. `SessionJobReconciler` and `StateSnapshotReconciler` are used directly. |
| Observer implementations (`ApplicationObserver`, `SessionObserver`, `FlinkSessionJobObserver`, `StateSnapshotObserver`)              | Flink Operator | Refresh `status` from the Flink REST API and Kubernetes API. Same factory selection as the reconciler in the deployment case (`FlinkDeploymentObserverFactory`).                                                                                                           |
| `StatusRecorder`                                                                                                                     | Flink Operator | Per-controller. Patches the CR's `status` subresource, caches the most recently observed status to skip no-op updates, and fans out every status change to the `FlinkResourceListener` chain.                                                                              |
| `EventRecorder`                                                                                                                      | Flink Operator | Per-controller. Emits Kubernetes `Event` records on the involved resource and fans them out to the `FlinkResourceListener` chain, see [Events]({{< ref "docs/operations/events" >}}).                                                                     |
| `JobAutoScaler`                                                                                                                      | Flink Operator | Per-controller, only for `FlinkDeployment` and `FlinkSessionJob`. Runs inside the reconcile loop, queries vertex metrics through `FlinkService`, evaluates scaling decisions, and applies them via a `ScalingRealizer`.                                                    |
| `CanaryResourceManager`                                                                                                              | Flink Operator | Per-controller, only for `FlinkDeployment` and `FlinkSessionJob`. Tracks "canary" CRs whose reconcile latency feeds the operator's liveness probe via `HealthProbe.INSTANCE`.                                                                                              |
| Per-controller context objects (`FlinkDeploymentContext`, `FlinkSessionJobContext`, `FlinkStateSnapshotContext`, `BlueGreenContext`) | Flink Operator | Built once per reconcile cycle by `FlinkResourceContextFactory`. Carry the resource, the effective `Configuration`, and lazily-initialized references to `FlinkService`, `KubernetesJobAutoScalerContext`, and operator-side recorders.                                    |
| `BlueGreenStateHandlerRegistry`                                                                                                      | Flink Operator | Only on `FlinkBlueGreenDeploymentController`. Routes the blue/green state machine through the handler that matches the current `blueGreenState` (`INITIALIZING_BLUE`, `ACTIVE_BLUE`, `SAVEPOINTING_BLUE`, ...).                                                            |
| `FlinkDeploymentController`, `FlinkSessionJobController`, `FlinkStateSnapshotController`, `FlinkBlueGreenDeploymentController`       | Flink Operator | The four `Reconciler` implementations registered with JOSDK. Each handles its own CR type and routes through the validator, observer, and reconciler pipeline.                                                                                                             |
| `Operator`                                                                                                                           | JOSDK          | Top-level orchestrator. Holds `ControllerManager`, registers the shutdown hook, starts and stops the whole graph.                                                                                                                                                          |
| `OperatorJosdkMetrics`                                                                                                               | JOSDK          | The operator's `Metrics` implementation. Single instance, configured via `ConfigurationServiceOverrider.withMetrics(...)`. JOSDK calls into it for `controllerRegistered`, `receivedEvent`, `reconcileCustomResource`, and the rest, see [Metrics]({{< ref "docs/operations/metrics" >}}).                        |
| `ControllerManager`                                                                                                                  | JOSDK          | Holds one `Controller` per registered reconciler. Starts and stops them as a group.                                                                                                                                                                                        |
| `LeaderElectionManager`                                                                                                              | JOSDK          | Optional. Coordinates leader election when HA is enabled (`kubernetes.operator.leader-election.enabled`).                                                                                                                                                                  |
| `Controller`                                                                                                                         | JOSDK          | One per registered reconciler. Wraps `EventSourceManager`, `EventProcessor`, `ControllerHealthInfo`, and the user reconciler.                                                                                                                                              |
| `ControllerHealthInfo`                                                                                                               | JOSDK          | Liveness / readiness data for the controller, surfaced through `RegisteredController.getControllerHealthInfo()` and consumed by the operator's health probe.                                                                                                               |
| `EventSourceManager`                                                                                                                 | JOSDK          | Owns the set of event sources for one controller. Coordinates their lifecycle (start, stop, namespace changes, dynamic registration).                                                                                                                                      |
| `EventProcessor`                                                                                                                     | JOSDK          | Receives events from every source, manages per-resource `ResourceState`, applies retry and rate-limit, dispatches to the reconcile thread pool.                                                                                                                            |
| `ControllerEventSource`                                                                                                              | JOSDK          | Primary-resource informer and primary cache. Watches the CR being reconciled (e.g., `FlinkDeployment`).                                                                                                                                                                    |
| `InformerEventSource`                                                                                                                | JOSDK          | Secondary-resource informer. Translates events on a related Kubernetes object (e.g. JM `Deployment`, `Ingress`, `FlinkSessionJob`) back to the primary `ResourceID`.                                                                                                       |
| `TimerEventSource`                                                                                                                   | JOSDK          | Retry / reschedule timer. Fires when `UpdateControl.rescheduleAfter(...)` or the retry policy schedules a future reconcile, and on the per-resource reconcile interval.                                                                                                    |
| `KubernetesClientMetrics`                                                                                                            | Fabric8        | Metric source plugged into the `KubernetesClient` config (`kubernetes.operator.kubernetes.client.metrics.enabled`). Surfaces the `KubeClient.HttpRequest.*` and `KubeClient.HttpResponse.*` metrics, see [Metrics]({{< ref "docs/operations/metrics" >}}).                                                   |
| `DefaultSharedIndexInformer`                                                                                                         | Fabric8        | The actual informer. One per registered event source at runtime (the diagram collapses them to one box per Controller). Owns the LIST/WATCH loop and the in-memory cache.                                                                                                  |
| `ProcessorStore`                                                                                                                     | Fabric8        | Bridge between the watch stream and the listener chain. Mutates the in-memory cache (`CacheImpl` + indexers) and pushes `Add` / `Update` / `Delete` notifications onward.                                                                                                  |
| `ProcessorListener`                                                                                                                  | Fabric8        | One per informer in our setup. Calls the registered `ResourceEventHandler` (the JOSDK event source) on every notification, in event order via a `SerialExecutor`.                                                                                                          |
| `KubernetesClient`                                                                                                                   | Fabric8        | Single shared HTTP client instance. Used by every informer, plus operator-side direct calls (status patches, event emission, `FlinkService`, autoscaler ConfigMaps).                                                                                                       |
| K8s API Server                                                                                                                       | Kubernetes     | The source of truth. Serves the LIST/WATCH protocol, stores resources in etcd, enforces RBAC, applies admission webhooks.                                                                                                                                                  |

### Startup Path

```text
FlinkOperator.main()
    │  builds KubernetesClient, EventRecorder, FlinkResourceContextFactory,
    │  validators, listeners, OperatorHealthService
    ▼
operator.register(controller, ...)                            [× 4 controllers]
    │  builds the JOSDK Controller (reconciler, EventSourceManager, EventProcessor, ManagedWorkflow, ...)
    │  reconciler.prepareEventSources(ctx) registers InformerEventSources with ESM
    │  Controller is added to ControllerManager
    ▼
operator.start()
    │  ControllerManager iterates controllers
    ▼
Controller.start()                                            (one per registered reconciler)
    │
    ├─► EventSourceManager.start()
    │       │
    │       ├─ Primary, sequential FIRST:
    │       │     startEventSource(ControllerEventSource)
    │       │           └─► ManagedInformerEventSource.start()
    │       │                 └─► InformerManager.start()    (one wrapper per namespace)
    │       │                       └─► InformerWrapper.start()
    │       │                             └─► DefaultSharedIndexInformer.start()
    │       │                                   └─► Reflector.listSyncAndWatch()
    │       │                                         ├─ LIST  (resourceVersion=0, optional limit)
    │       │                                         └─ WATCH (resourceVersion=lastSyncRV, timeout 10m)
    │       │
    │       └─ Secondary, concurrent AFTER:
    │             boundedExecuteAndWaitForAllToComplete(DEFAULT-priority sources)
    │                 ├─ TimerEventSource                    (in-process scheduler, no API call)
    │                 └─ Each InformerEventSource            (same downstream chain, with labelSelector)
    │
    └─► EventProcessor.start()
            │  executor = reconcileExecutorService()
            │  running = true
            │  handleAlreadyMarkedEvents()
            ▼
        log "'<name>' controller started"
```

### Threads, Executors, and Pools

| Pool                                                      | Provided by  | Approximate size                                  | Used for                                                                                                |
|-----------------------------------------------------------|--------------|---------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `reconcileExecutorService` (the **reconciliation pool**)  | JOSDK        | `50` by default, set via `kubernetes.operator.reconcile.parallelism` (`-1` for unbounded cached pool) | Every `reconciler.reconcile()` invocation runs here. One in-flight reconcile per primary `ResourceID`.   |
| Informer executor (Fabric8 client shared executor)         | Fabric8      | OkHttp dispatcher max, default `64` HTTP threads  | LIST/WATCH I/O, `Reflector` retry scheduling, watch timeout cancellation, listener notification dispatch. |
| `SerialExecutor` wrapping the informer executor           | Fabric8      | Logical single-thread queue per informer          | Ensures `ResourceEventHandler` callbacks for each informer are invoked in event order.                  |
| Health probe HTTP server                                  | Operator     | 1 thread                                          | Serves `/health` on the configured port (default `8085`) when `kubernetes.operator.health.probe.enabled=true`. |
| Dynamic config watcher                                    | Operator     | 1 scheduled thread                                | Periodically re-reads `flink-operator-config` when `kubernetes.operator.dynamic.config.enabled=true`.    |

Two timing facts worth keeping in mind:

- **Watch lifetime**: every Fabric8 watch is force-cycled by the `Reflector` every 5 to 10 minutes (randomized within `[MIN_TIMEOUT, 2 * MIN_TIMEOUT]`, default `MIN_TIMEOUT = 5 minutes`). On API-server disconnect or `HTTP 410 Gone`, the Reflector reconnects with exponential backoff.
- **Cache sync timeout**: each informer must finish its initial LIST and start its WATCH within `ConfigurationService.cacheSyncTimeout()` (default 2 minutes). If `kubernetes.operator.startup.stop-on-informer-error` is left at its default of `true`, exceeding this kills the operator. Otherwise the operator stays up and retries in the background.

With the controllers registered and their informers synced, the startup is complete. From here on every pass follows the controller pipeline, and the reconcile triggers, threading, and flow are described under [Controllers]({{< ref "docs/internals/controllers" >}}).
