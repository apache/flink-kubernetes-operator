---
title: "Application Profiling & Debugging"
weight: 2
type: docs
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

# Application Profiling & Debugging

This page covers how to profile and debug the JVM processes that run Flink on Kubernetes, for the case where a process is up and the problem lives inside it. The symptom might be slow checkpoints, backpressure, or memory growth in a Flink job, or stalled reconciliation and a blocked thread in the operator.

There are two possible debugging targets, configured through different surfaces:

- The **Flink Cluster**, meaning the JobManager and TaskManager processes the operator deploys. These are configured through the custom resource.
- The **Operator** process itself, a standalone Deployment. This is configured through the Helm chart.

The important idea is that every technique on this page applies to both targets. Only the delivery surface changes. This page does not re-teach JVM or Flink debugging. The Flink [Application Profiling & Debugging](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/debugging/application_profiling/) guide is the reference for what Java Flight Recorder, GC logs, heap dumps, and flame graphs mean and how to read them. Only the delivery and access through the operator are covered here.

{{< hint info >}}
The examples below use synchronized tabs. Pick **Flink Cluster** or **Operator** in any tab and every tab on the page follows that choice, so the whole page can be read from a single target's point of view.
{{< /hint >}}

## Delivery Surfaces

The table below is the map for the rest of the page. For each configurable concern, it gives the custom resource key for the Flink cluster and the Helm value for the operator. Most techniques come down to just two of these rows: passing a JVM argument, and, for anything that writes a file (a heap dump, a GC log, a JFR recording) or opens a port (remote debugging), mounting a writable volume.

| Concern               | Flink Cluster (custom resource)                                                                                        | Operator (Helm chart)                                                         |
|-----------------------|------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| JVM arguments         | `env.java.opts.all` / `.jobmanager` / `.taskmanager` in `spec.flinkConfiguration`                                      | `jvmArgs.operator` (maps to the `JVM_ARGS` env var)                           |
| Environment variables | `spec.podTemplate` (`flink-main-container` `env`), or `containerized.master.env.*` / `containerized.taskmanager.env.*` | `operatorPod.env` and `operatorPod.envFrom`                                   |
| Writable volumes      | `spec.podTemplate` `volumes` and `volumeMounts`                                                                        | `operatorVolumes` and `operatorVolumeMounts` (set `create: true`)             |
| Log configuration     | `spec.logConfiguration` (`log4j-console.properties`)                                                                   | `defaultConfiguration` (`log4j-operator.properties`)                          |
| Process access        | Web UI, `port-forward svc/<name>-rest 8081`, `kubectl exec`                                                            | `kubectl exec` / `logs` / `cp` with `-c flink-kubernetes-operator`, no Web UI |
| Applying a change     | reconciles automatically                                                                                               | `helm upgrade`, then a manual pod restart                                     |

{{< hint warning >}}
Custom resource changes are reconciled automatically. Helm changes are not. After a `helm upgrade` the operator pod has to be restarted (deleted so the Deployment redeploys it), and that restart rolls the operator. This applies to every operator-side change below, except the on-demand [thread dump](#thread-dumps), which needs no restart.
{{< /hint >}}

{{< hint info >}}
A `FlinkSessionJob` runs on the TaskManagers of its parent session cluster, so all cluster-side debug configuration (JVM args, environment variables, volumes, log config) goes on the parent `FlinkDeployment` it references through `deploymentName`, never on the session job. A `FlinkSessionJob` has no `podTemplate` or `logConfiguration` of its own, and its `flinkConfiguration` carries only job-level overrides merged at submission, so it cannot change the cluster's JVM or pods.
{{< /hint >}}

{{< hint info >}}
The operator pod also runs a second JVM, the admission webhook (the `flink-webhook` container). It is debugged with the same techniques, using the webhook's own knobs: `jvmArgs.webhook` for JVM arguments, `operatorPod.webhook.container.env` for environment variables, and `-c flink-webhook` for `kubectl exec` / `logs` / `cp`. It shares the operator's `log4j-operator.properties` logging. The webhook is a small, stateless validation and mutation service, so it rarely needs profiling, which is why the tabs on this page cover the operator and the cluster rather than the webhook.
{{< /hint >}}

## Reaching the Process

Before any flag, here is how to get to each process. Later sections reuse these access patterns.

{{< tabs "reach" >}}
{{< tab "Flink Cluster" >}}
The operator exposes the JobManager REST endpoint as a Kubernetes service named `<deployment-name>-rest` on port `8081`. Forward it to reach the Flink Web UI, where the interactive tooling (the Thread Dump tab, flame graphs, the profiler) lives:

```bash
kubectl port-forward svc/<deployment-name>-rest 8081
```

The dashboard is then at `http://localhost:8081`. For persistent external access, configure an [Ingress]({{< ref "docs/custom-resource/ingress" >}}).
{{< /tab >}}
{{< tab "Operator" >}}
The operator has no Web UI. It is reached with `kubectl exec`, `kubectl logs`, and `kubectl cp` against its Deployment. The operator pod runs two containers, the operator and the webhook, so always select the operator one with `-c flink-kubernetes-operator`:

```bash
kubectl exec -it deploy/flink-kubernetes-operator -n <operator-namespace> \
  -c flink-kubernetes-operator -- bash
```

Every operator command below uses this same `-c flink-kubernetes-operator` selector.
{{< /tab >}}
{{< /tabs >}}

## Passing JVM Arguments

This is the foundation. Heap dumps, GC logging, remote debugging, and JFR are all just flags delivered this way.

{{< tabs "jvmargs" >}}
{{< tab "Flink Cluster" >}}
Set JVM options through `spec.flinkConfiguration`. Use `env.java.opts.all` for both processes, or `env.java.opts.jobmanager` / `env.java.opts.taskmanager` to target one:

```yaml
spec:
  flinkConfiguration:
    env.java.opts.all: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/flink-data"
```

{{< hint warning >}}
On Flink 1.19+ a user-supplied `env.java.opts.all` is appended to the operator's own `env.java.default-opts.all` defaults, so adding options is safe. On Flink 1.18 there is no default option and the supplied value replaces the operator defaults. See [Java Options for Java 17 Based Flink Images]({{< ref "docs/deployment/configuration#java-options-for-java-17-based-flink-images" >}}).
{{< /hint >}}
{{< /tab >}}
{{< tab "Operator" >}}
Set JVM options through the `jvmArgs.operator` Helm value. It maps to the `JVM_ARGS` environment variable on the operator container:

```yaml
# values.yaml
jvmArgs:
  operator: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/flink-data"
```
{{< /tab >}}
{{< /tabs >}}

### Per Flink Version

There is one override that blurs the two surfaces. The operator can apply configuration to managed clusters of a specific Flink version through its own ConfigMap, using the key pattern `kubernetes.operator.default-configuration.flink-version.<version>.<configKey>`. Versions use underscores (`v1_18`), and a `+` suffix means that version and higher (`v1_19+`). This is delivered through Helm, but it configures the cluster, not the operator.

It accepts any Flink config key, so it works for both JVM options and environment variables across every deployment of a version:

```yaml
# operator configuration (defaultConfiguration / operator ConfigMap)
kubernetes.operator.default-configuration.flink-version.v1_18.env.java.opts.all: "-XX:+UseG1GC"
kubernetes.operator.default-configuration.flink-version.v1_19+.containerized.taskmanager.env.MY_FLAG: "true"
```

See [Flink Version and Namespace Specific Defaults]({{< ref "docs/deployment/configuration#flink-version-and-namespace-specific-defaults" >}}).

## Environment Variables

{{< tabs "env" >}}
{{< tab "Flink Cluster" >}}
The preferred way is the pod template, which gives the full Kubernetes `env` syntax including `valueFrom`:

```yaml
spec:
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: MY_DEBUG_FLAG
              value: "true"
```

As an alternative, `containerized.master.env.<KEY>` and `containerized.taskmanager.env.<KEY>` in `spec.flinkConfiguration` set variables on the JobManager and TaskManager respectively. To set a variable for every deployment of a Flink version, use the [per-version](#per-flink-version) override above.
{{< /tab >}}
{{< tab "Operator" >}}
Set variables through `operatorPod.env`, or reference a ConfigMap or Secret with `operatorPod.envFrom`:

```yaml
# values.yaml
operatorPod:
  env:
    - name: MY_DEBUG_FLAG
      value: "true"
```

These apply to the operator container only. The webhook has its own `operatorPod.webhook.container.env`.
{{< /tab >}}
{{< /tabs >}}

## Heap Dumps and OOM

This is the first technique that writes a file, so it introduces the writable-volume pattern that GC logging and JFR reuse. Combine the JVM flag with a mounted volume so the `.hprof` file survives the pod.

{{< tabs "heapdump" >}}
{{< tab "Flink Cluster" >}}
```yaml
spec:
  flinkConfiguration:
    env.java.opts.all: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/flink-data"
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          volumeMounts:
            - mountPath: /flink-data
              name: dumps
      volumes:
        - name: dumps
          emptyDir: {}
```

Retrieve the file with:

```bash
kubectl cp <namespace>/<pod>:/flink-data/<file>.hprof ./<file>.hprof
```
{{< /tab >}}
{{< tab "Operator" >}}
```yaml
# values.yaml
jvmArgs:
  operator: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/flink-data"

operatorVolumeMounts:
  create: true
  data:
    - name: dumps
      mountPath: /flink-data
operatorVolumes:
  create: true
  data:
    - name: dumps
      emptyDir: {}
```

Retrieve the file with the operator container selector:

```bash
kubectl cp <operator-namespace>/<operator-pod>:/flink-data/<file>.hprof ./<file>.hprof \
  -c flink-kubernetes-operator
```

{{< hint warning >}}
`operatorVolumeMounts.create` and `operatorVolumes.create` default to `false`, in which case a default `emptyDir` is mounted at `/opt/flink/artifacts`. Both must be `true` to mount a custom volume. An `emptyDir` is ephemeral, so use a `persistentVolumeClaim` (or a `hostPath`) when the dump must outlive the pod, which matters because the operator restart that Helm changes require deletes the pod.
{{< /hint >}}

{{< /tab >}}
{{< /tabs >}}

## GC Logging

Garbage-collection logs help diagnose pauses and memory pressure. This reuses the volume from [Heap Dumps and OOM](#heap-dumps-and-oom), so only the flag changes.

{{< tabs "gclog" >}}
{{< tab "Flink Cluster" >}}
```yaml
spec:
  flinkConfiguration:
    env.java.opts.all: "-Xlog:gc*:file=/flink-data/gc.log:time,uptime:filecount=5,filesize=10m"
```
{{< /tab >}}
{{< tab "Operator" >}}
```yaml
# values.yaml
jvmArgs:
  operator: "-Xlog:gc*:file=/flink-data/gc.log:time,uptime:filecount=5,filesize=10m"
```
{{< /tab >}}
{{< /tabs >}}

Mount the `/flink-data` volume exactly as in the heap-dump example, then copy `gc.log` out the same way.

## CPU Profiling, Flame Graphs, and JFR

This is where the two targets diverge most. The cluster has Web UI tooling. The operator does not, so it relies on Java Flight Recorder, which both targets support.

{{< tabs "profiling" >}}
{{< tab "Flink Cluster" >}}
Flink ships built-in, Web-UI-driven tooling. Enable it through `spec.flinkConfiguration`:

```yaml
spec:
  flinkConfiguration:
    rest.flamegraph.enabled: "true"
    rest.profiling.enabled: "true"
```

`rest.flamegraph.enabled` (available since Flink 1.13) adds a Flame Graph tab to each operator vertex. `rest.profiling.enabled` (available since Flink 1.19) enables the built-in async-profiler integration, producing interactive HTML reports per JobManager or TaskManager. Both are read in the [Web UI](#reaching-the-process). For offline analysis, JFR also works here, see the note below.

See the Flink [Flame Graphs](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/debugging/flame_graphs/) and [Profiler](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/debugging/profiler/) guides for how to read the output.
{{< /tab >}}
{{< tab "Operator" >}}
The operator has no Web UI, so the flame-graph and profiler tabs do not apply. Java Flight Recorder is the equivalent. Record to a `.jfr` file on a mounted volume, then analyze it locally in a tool such as JDK Mission Control:

```yaml
# values.yaml
jvmArgs:
  operator: "-XX:StartFlightRecording=name=operator,filename=/flink-data/operator.jfr,dumponexit=true"
```

Mount the `/flink-data` volume as in the [heap-dump example](#heap-dumps-and-oom) and copy the `.jfr` file out with `kubectl cp ... -c flink-kubernetes-operator`. The operator's JRE base image fully supports JFR.
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
JFR is not operator-only. The same `-XX:StartFlightRecording=...` can be attached to a cluster through `env.java.opts.*` when an offline recording is preferred over the Web UI. Every technique on this page works on both targets.
{{< /hint >}}

## Remote JVM Debugging

To attach a debugger, add a JDWP agent through the JVM options and forward the debug port. Keep `suspend=n` so the JVM does not block startup waiting for a debugger.

{{< tabs "jdwp" >}}
{{< tab "Flink Cluster" >}}
```yaml
spec:
  flinkConfiguration:
    env.java.opts.jobmanager: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
```

```bash
kubectl port-forward <jobmanager-pod> 5005:5005
```

Target the JobManager or TaskManager with the matching `env.java.opts.*` key, then attach the IDE to `localhost:5005`.
{{< /tab >}}
{{< tab "Operator" >}}
```yaml
# values.yaml
jvmArgs:
  operator: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
```

```bash
kubectl port-forward deploy/flink-kubernetes-operator -n <operator-namespace> 5005:5005
```

Attach the IDE to `localhost:5005`. With multiple operator replicas, only the active leader is doing work, so a single replica is simplest to debug.
{{< /tab >}}
{{< /tabs >}}

## Thread Dumps

A thread dump is the first thing to capture for a hung or backpressured process. It needs no JVM flags and no restart, which makes it the one technique that runs on a live process at any time.

{{< tabs "threaddump" >}}
{{< tab "Flink Cluster" >}}
Three options, easiest first:

1. **Web UI:** Open the dashboard, select the JobManager or a TaskManager, and use the **Thread Dump** tab.
2. **REST API:** With the [port-forward](#reaching-the-process) in place:

   ```bash
   curl http://localhost:8081/jobmanager/thread-dump
   curl http://localhost:8081/taskmanagers/<tm-id>/thread-dump
   ```

3. **`kubectl exec`:** Signal the JVM directly. The Flink process is PID 1, so `SIGQUIT` prints a dump to the pod logs:

   ```bash
   kubectl exec <pod-name> -- bash -c 'kill -3 1'
   kubectl logs <pod-name>
   ```

   The dump is interleaved with the pod logs, so extract it with the same `--since` and `sed` shown in the Operator tab.
{{< /tab >}}
{{< tab "Operator" >}}
The operator has no Web UI or REST endpoint, and its JRE image has no `jstack`, so use `SIGQUIT`. The JVM is PID 1, and the dump goes to the operator's stdout interleaved with reconcile logs, so narrow the window with `--since` and carve out the dump with `sed`:

```bash
# trigger the dump
kubectl exec deploy/flink-kubernetes-operator -n <operator-namespace> \
  -c flink-kubernetes-operator -- bash -c 'kill -3 1'

# extract just the dump from the last few seconds of logs
kubectl logs deploy/flink-kubernetes-operator -n <operator-namespace> \
  -c flink-kubernetes-operator --since=20s \
  | sed -n '/Full thread dump/,/^JNI global refs/p' > operator-thread-dump.txt
```

The `sed` range starts at the `Full thread dump` header and stops at the `JNI global refs` line that terminates it, so the surrounding logs are dropped. With multiple operator replicas, `deploy/flink-kubernetes-operator` selects an arbitrary pod. When [leader election]({{< ref "docs/deployment/leader-election" >}}) is enabled, dump the pod that holds the lease, since only it reconciles.
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
Despite the name, `kill -3` does not terminate the process. `-3` is `SIGQUIT`, which the JVM handles by printing a thread dump and then continuing to run. It is safe on a live cluster or operator and needs no restart.
{{< /hint >}}

## Adjusting Log Configuration

Raising the log level, or enriching the log pattern, is often enough to diagnose a problem without a profiler. Both targets ship a Log4j2 configuration (the default) and a Logback configuration, so the override key depends on the framework in use.

{{< tabs "logging" >}}
{{< tab "Flink Cluster" >}}
Override the Flink log configuration through `spec.logConfiguration`, which replaces the container's log config. Use the `log4j-console.properties` key for Log4j2 (the default) or `logback-console.xml` for Logback:

```yaml
spec:
  logConfiguration:
    "log4j-console.properties": |
      rootLogger.level = DEBUG
      rootLogger.appenderRef.console.ref = LogConsole
      appender.console.name = LogConsole
      appender.console.type = CONSOLE
      appender.console.layout.type = PatternLayout
      appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
```

For scoped changes, set a single logger instead of the root level (for example `logger.netty.name` and `logger.netty.level = DEBUG`).
{{< /tab >}}
{{< tab "Operator" >}}
Raise the operator's log level through `defaultConfiguration`, the operator's own log config, which is distinct from the cluster's. Use the `log4j-operator.properties` key for Log4j2 (the default) or `logback-operator.xml` when the chart is installed with `logging.framework: logback`. Keep `append: true` to add to the shipped defaults:

```yaml
# values.yaml
defaultConfiguration:
  create: true
  append: true
  log4j-operator.properties: |+
    rootLogger.level = DEBUG
```

See [Configuration]({{< ref "docs/deployment/configuration" >}}) for how `defaultConfiguration` and the operator ConfigMap are managed.
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
The default log pattern does not include the thread name. When chasing a hang or a concurrency issue, add `%t` (Log4j2) or `%thread` (Logback) to the appender's conversion pattern so each line shows the thread that emitted it. This pairs naturally with a [thread dump](#thread-dumps).
{{< /hint >}}

See [Logging]({{< ref "docs/operations/logging" >}}) for the full logging setup.
