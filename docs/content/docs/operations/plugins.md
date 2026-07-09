---
title: "Custom Operator Plugins"
weight: 5
type: docs
aliases:
- /operations/plugins.html
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

# Custom Operator Plugins

The operator provides a customizable platform for Flink resource management. Users can develop plugins to tailor the operator behaviour to their own needs.

## Custom Flink Resource Validators

`FlinkResourceValidator`, an interface for validating the resources of `FlinkDeployment` and `FlinkSessionJob`,  is a pluggable component based on the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism. During development, we can customize the implementation of `FlinkResourceValidator` and make sure to retain the service definition in `META-INF/services`.
The following steps demonstrate how to develop and use a custom validator.

1. Implement `FlinkResourceValidator` interface:
    ```java
    package org.apache.flink.kubernetes.operator.validation;

    import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
    import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;

    import java.util.Optional;

    /** Custom validator implementation of {@link FlinkResourceValidator}. */
    public class CustomValidator implements FlinkResourceValidator {

        @Override
        public Optional<String> validateDeployment(FlinkDeployment deployment) {
            if (deployment.getSpec().getFlinkVersion() == null) {
              return Optional.of("Flink Version must be defined.");
            }
            return Optional.empty();
        }

        @Override
        public Optional<String> validateSessionJob(
                 FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {
            if (sessionJob.getSpec().getJob() == null) {
              return Optional.of("The job spec should not be empty");
            }
            return Optional.empty();
        }
    }
    ```

2. Create service definition file `org.apache.flink.kubernetes.operator.api.validation.FlinkResourceValidator` in `META-INF/services`.   With custom `FlinkResourceValidator` implementation, the service definition describes as follows:
    ```text
    org.apache.flink.kubernetes.operator.validation.CustomValidator
    ```

3. Use the Maven tool to package the project and generate the custom validator JAR.

4. Create Dockerfile to build a custom image from the `apache/flink-kubernetes-operator` official image and copy the generated JAR to custom validator plugin directory.
    `/opt/flink/plugins` is the value of `FLINK_PLUGINS_DIR` environment variable in the flink-kubernetes-operator helm chart. The structure of custom validator directory under `/opt/flink/plugins` is as follows:
    ```text
    /opt/flink/plugins
        ├── custom-validator
        │   ├── custom-validator.jar
        └── ...
    ```

    With the custom validator directory location, the Dockerfile is defined as follows:
    ```shell script
    FROM apache/flink-kubernetes-operator
    ENV FLINK_PLUGINS_DIR=/opt/flink/plugins
    ENV CUSTOM_VALIDATOR_DIR=custom-validator
    RUN mkdir $FLINK_PLUGINS_DIR/$CUSTOM_VALIDATOR_DIR
    COPY custom-validator.jar $FLINK_PLUGINS_DIR/$CUSTOM_VALIDATOR_DIR/
    ```

5. Install the flink-kubernetes-operator helm chart with the custom image and verify the `deploy/flink-kubernetes-operator` log has:
    ```text
    2022-05-04 14:01:46,551 o.a.f.k.o.u.FlinkUtils         [INFO ] Discovered resource validator from plugin directory[/opt/flink/plugins]: org.apache.flink.kubernetes.operator.validation.CustomValidator.
    ```

## Custom Flink Resource Listeners

The Flink Kubernetes Operator allows users to listen to events and status updates triggered for the Flink Resources managed by the operator.
This feature enables tighter integration with the user's own data platform.

By implementing the `FlinkResourceListener` interface users can listen to both events and status updates per resource type (`FlinkDeployment` / `FlinkSessionJob`). These methods will be called after the respective events have been triggered by the system.
Using the context provided on each listener method users can also get access to the related Flink resource and the `KubernetesClient` itself in order to trigger any further events etc on demand.

Similar to custom validator implementations, resource listeners are loaded via the Flink [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism.

In order to enable your custom `FlinkResourceListener` you need to:

 1. Implement the interface
 2. Add your listener class to `org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener` in `META-INF/services`
 3. Package your JAR and add it to the plugins directory of your operator image (`/opt/flink/plugins`)


## Additional Dependencies
In some cases, users may need to add required dependencies onto the operator classpath.

When building the custom image, The additional dependencies shall be copied to `/opt/flink/operator-lib` with the environment variable: `OPERATOR_LIB`
That folder is added to classpath upon initialization.

```shell script
    FROM apache/flink-kubernetes-operator
    ARG ARTIFACT1=custom-artifact1.jar
    ARG ARTIFACT2=custom-artifact2.jar
    COPY target/$ARTIFACT1 $OPERATOR_LIB
    COPY target/$ARTIFACT2 $OPERATOR_LIB
```

## Custom Flink Resource Mutators

`FlinkResourceMutator`, an interface for mutating the resources of `FlinkDeployment` and `FlinkSessionJob`,  is a pluggable component based on the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism. During development, we can customize the implementation of `FlinkResourceMutator` and make sure to retain the service definition in `META-INF/services`.
The following steps demonstrate how to develop and use a custom mutator.

1. Implement `FlinkResourceMutator` interface:
   ```java
   package org.apache.flink.mutator;

   import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
   import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
   import org.apache.flink.kubernetes.operator.api.mutator.FlinkResourceMutator;
   
   import java.util.Optional;
   
   /** Custom Flink Mutator. */
   public class CustomFlinkMutator implements FlinkResourceMutator {
   
      @Override
      public FlinkDeployment mutateDeployment(FlinkDeployment deployment) {
        return deployment;
      }
   
      @Override
      public FlinkSessionJob mutateSessionJob(
            FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {
        return sessionJob;
      }
   }
   ```

2. Create service definition file `org.apache.flink.kubernetes.operator.api.mutator.FlinkResourceMutator` in `META-INF/services`.   With custom `FlinkResourceMutator` implementation, the service definition describes as follows:
    ```text
    org.apache.flink.mutator.CustomFlinkMutator
    ```

3. Use the Maven tool to package the project and generate the custom mutator JAR.

4. Create Dockerfile to build a custom image from the `apache/flink-kubernetes-operator` official image and copy the generated JAR to custom mutator plugin directory.
   `/opt/flink/plugins` is the value of `FLINK_PLUGINS_DIR` environment variable in the flink-kubernetes-operator helm chart. The structure of custom mutator directory under `/opt/flink/plugins` is as follows:
    ```text
    /opt/flink/plugins
        ├── custom-mutator
        │   ├── custom-mutator.jar
        └── ...
    ```

   With the custom mutator directory location, the Dockerfile is defined as follows:
    ```shell script
    FROM apache/flink-kubernetes-operator
    ENV FLINK_PLUGINS_DIR=/opt/flink/plugins
    ENV CUSTOM_MUTATOR_DIR=custom-mutator
    RUN mkdir $FLINK_PLUGINS_DIR/$CUSTOM_MUTATOR_DIR
    COPY custom-mutator.jar $FLINK_PLUGINS_DIR/$CUSTOM_MUTATOR_DIR/
    ```

5. Install the flink-kubernetes-operator helm chart with the custom image and verify the `deploy/flink-kubernetes-operator` log has:
    ```text
    2023-12-12 06:26:56,667 o.a.f.k.o.u.MutatorUtils [INFO ] Discovered mutator from plugin directory[/opt/flink/plugins]: org.apache.flink.mutator.CustomFlinkMutator.
    ```

## Custom Scaling Metric Evaluators

`ScalingMetricsEvaluatorPlugin` is a pluggable component that allows users to provide custom scaling-metric evaluation logic on top of the metrics evaluated internally by the autoscaler. Custom metric evaluators are discovered through the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism when running inside the Kubernetes operator, and through the standard Java `ServiceLoader` mechanism when running with `flink-autoscaler-standalone`. In both cases the implementation class must be registered in `META-INF/services`.

For each evaluation cycle, the autoscaler invokes the custom metric evaluator selected via the `job.autoscaler.metrics.custom-evaluators` configuration option once per job vertex. The metrics returned by the custom evaluator are merged on top of the internally evaluated metrics, allowing users to override or augment specific `ScalingMetric` values (e.g. `TARGET_DATA_RATE`, `TRUE_PROCESSING_RATE`, `CATCH_UP_DATA_RATE`).

All `job.autoscaler.*` keys related to custom metric evaluators also support the legacy `kubernetes.operator.`-prefixed form as a fallback (for example, `kubernetes.operator.job.autoscaler.metrics.custom-evaluators`). The canonical key takes precedence on overlap.

The following steps demonstrate how to develop and use a custom metric evaluator.

1. Implement the `ScalingMetricsEvaluatorPlugin` interface:
    ```java
    package org.apache.flink.autoscaler.custom;

    import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
    import org.apache.flink.autoscaler.metrics.ScalingMetricsEvaluatorPlugin;
    import org.apache.flink.autoscaler.metrics.ScalingMetric;
    import org.apache.flink.runtime.jobgraph.JobVertexID;

    import java.util.HashMap;
    import java.util.Map;

    /** Custom metric evaluator implementation of {@link ScalingMetricsEvaluatorPlugin}. */
    public class CustomEvaluator implements ScalingMetricsEvaluatorPlugin {

        @Override
        public Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
                JobVertexID vertex,
                Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
                Context<?> context) {
            Map<ScalingMetric, EvaluatedScalingMetric> overrides = new HashMap<>();
            // Example: override the target data rate for source vertices based
            // on an evaluator-specific option read from the merged configuration.
            double target = context.getConfiguration().getDouble("target-data-rate", 0.0);
            if (target > 0 && context.getJobTopology().isSource(vertex)) {
                overrides.put(
                        ScalingMetric.TARGET_DATA_RATE,
                        EvaluatedScalingMetric.avg(target));
            }
            return overrides;
        }
    }
    ```

   The `Context` extends `JobAutoScalerContext`, exposing the metric history, job topology and max restart time through the inherited accessors, plus the previously evaluated vertex metrics (evaluation happens topologically) and backlog status. Its `getConfiguration()` returns the job configuration with the evaluator-specific options (prefix stripped) merged on top, so an evaluator-specific key overrides the job-level value of the same key.

2. Create the service definition file `org.apache.flink.autoscaler.metrics.ScalingMetricsEvaluatorPlugin` in `META-INF/services` with the fully-qualified class name of your implementation:
    ```text
    org.apache.flink.autoscaler.custom.CustomEvaluator
    ```

3. Use the Maven tool to package the project and generate the custom metric evaluator JAR.

4. Select the custom metric evaluator via configuration. The evaluator whose implementation class FQN matches the configured `job.autoscaler.metrics.custom-evaluator.<name>.class` value will be invoked, and any other `job.autoscaler.metrics.custom-evaluator.<name>.*` entries are merged on top of the job configuration with the `job.autoscaler.metrics.custom-evaluator.<instance>.` prefix stripped, and exposed via `Context.getConfiguration()`, taking precedence over the job-level value of the same key. The configuration shape mirrors Flink's metric-reporter idiom: a list of named instances, plus a `.class` and free-form options under each instance namespace. Selection is purely by class FQN.
    ```yaml
    job.autoscaler.metrics.custom-evaluators: my-evaluator
    job.autoscaler.metrics.custom-evaluator.my-evaluator.class: org.apache.flink.autoscaler.custom.CustomEvaluator
    job.autoscaler.metrics.custom-evaluator.my-evaluator.target-data-rate: 100000.0
    ```
   {{< hint warning >}}
   **Only one custom metric evaluator per pipeline is supported for now**. Currently, `job.autoscaler.metrics.custom-evaluators` is parsed as a list, but if more than one entry is configured the autoscaler logs a warning and falls back to the first entry, ignoring the rest. Registering multiple implementations via `META-INF/services` is fine as they form a registry that different jobs can select from by class FQN, but a single job cannot chain or compose more than one evaluator. Multi-instance support, including a priority/ordering contract aligned with [FLIP-575](https://cwiki.apache.org/confluence/display/FLINK/FLIP-575%3A+Scaling+Executor+Plugin+SPI+for+Flink+Autoscaler) (`ScalingExecutorPlugin`), will be added as a follow-up.
   {{< /hint >}}

5. Deploy the evaluator.

    - **Operator deployment** - create a Dockerfile to build a custom image from the `apache/flink-kubernetes-operator` official image and copy the generated JAR to a custom metric evaluator plugin directory under `/opt/flink/plugins` (the value of the `FLINK_PLUGINS_DIR` environment variable in the flink-kubernetes-operator helm chart). The structure of the custom evaluator directory under `/opt/flink/plugins` is as follows:
        ```text
        /opt/flink/plugins
            ├── custom-evaluator
            │   ├── custom-evaluator.jar
            └── ...
        ```

        With the custom metric evaluator directory location, the Dockerfile is defined as follows:
        ```shell script
        FROM apache/flink-kubernetes-operator
        ENV FLINK_PLUGINS_DIR=/opt/flink/plugins
        ENV CUSTOM_EVALUATOR_DIR=custom-evaluator
        RUN mkdir $FLINK_PLUGINS_DIR/$CUSTOM_EVALUATOR_DIR
        COPY custom-evaluator.jar $FLINK_PLUGINS_DIR/$CUSTOM_EVALUATOR_DIR/
        ```

        Install the flink-kubernetes-operator helm chart with the custom image and verify the `deploy/flink-kubernetes-operator` log has:
        ```text
        o.a.f.k.o.a.AutoscalerUtils [INFO ] Discovered custom metric evaluator from plugin directory[/opt/flink/plugins]: org.apache.flink.autoscaler.custom.CustomEvaluator.
        ```

    - **Standalone autoscaler** - simply place the custom metric evaluator JAR on the classpath of the `flink-autoscaler-standalone` process. It will be picked up automatically via Java's `ServiceLoader` and discovery will be logged:
        ```text
        o.a.f.a.s.AutoscalerUtils [INFO ] Discovered custom metric evaluator via ServiceLoader: org.apache.flink.autoscaler.custom.CustomEvaluator.
        ```

## Custom Parallelism Alignment Modes

The autoscaler aligns a vertex's computed target parallelism to the number of key groups (keyBy) or source partitions to reduce data skew. The built-in modes (`BALANCED`, `EVENLY_SPREAD`, `OFF`) are selected by name through `job.autoscaler.scaling.parallelism-alignment.mode`. A custom alignment mode can also be provided as a plugin by implementing the `ParallelismAlignmentMode` SPI. Custom modes are discovered through the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism when running inside the Kubernetes operator, and through the standard Java `ServiceLoader` mechanism when running with `flink-autoscaler-standalone`. In both cases the implementation class must be registered in `META-INF/services`.

All `job.autoscaler.*` keys related to custom alignment modes also support the legacy `kubernetes.operator.`-prefixed form as a fallback (for example, `kubernetes.operator.job.autoscaler.scaling.parallelism-alignment.mode`). The canonical key takes precedence on overlap.

The following steps demonstrate how to develop and use a custom alignment mode.

1. Implement `org.apache.flink.autoscaler.alignment.ParallelismAlignmentMode`:
    ```java
    package org.apache.flink.autoscaler.alignment;

    /** Custom alignment mode snapping the computed parallelism down to the nearest divisor. */
    public class CustomAlignmentMode implements ParallelismAlignmentMode {

        /** Apply to every vertex, not only the source and keyBy vertices the built-ins handle. */
        @Override
        public boolean isApplicable(Context<?> ctx) {
            return true;
        }

        @Override
        public int alignParallelism(Context<?> ctx) {
            // Number of key groups, or source partitions for a partitioned source.
            int n = ctx.getNumSourcePartitions() > 0
                    ? ctx.getNumSourcePartitions()
                    : ctx.getMaxParallelism();
            // Optional mode-specific parameter, read from the per-mode configuration.
            int minParallelism = ctx.getConfiguration().getInteger("min-parallelism", 1);
            for (int p = ctx.getNewParallelism(); p >= minParallelism; p--) {
                if (n % p == 0) {
                    return p;
                }
            }
            return ctx.getNewParallelism();
        }
    }
    ```
   The `Context` extends `JobAutoScalerContext`, adding the per-vertex alignment inputs: the current and computed target parallelism, the number of key groups or source partitions, the input ship strategies, and the vertex's evaluated metrics. The job topology is available through the inherited `getJobTopology()`, and `getConfiguration()` returns the job configuration with this mode's prefix-stripped per-mode options merged on top. The autoscaler calls `alignParallelism` only when `isApplicable(Context)` returns true. That method defaults to keyBy (hash) vertices and to partitioned sources that report a partition count (Kafka and Pulsar do by default), and a custom mode can override it to widen the scope, for example to align custom partitioned vertices.

2. Create the service definition file `org.apache.flink.autoscaler.alignment.ParallelismAlignmentMode` in `META-INF/services`:
    ```text
    org.apache.flink.autoscaler.alignment.CustomAlignmentMode
    ```

3. Use the Maven tool to package the project and generate the custom alignment mode JAR.

4. Select the custom mode by name and point it at your implementation class. Any other `job.autoscaler.scaling.parallelism-alignment.mode.<name>.*` entries are merged (prefix-stripped) on top of the job configuration and exposed to the mode through `Context#getConfiguration()`:
    ```yaml
    job.autoscaler.scaling.parallelism-alignment.mode: custom-mode
    job.autoscaler.scaling.parallelism-alignment.mode.custom-mode.class: org.apache.flink.autoscaler.alignment.CustomAlignmentMode
    job.autoscaler.scaling.parallelism-alignment.mode.custom-mode.min-parallelism: 4
    ```
   {{< hint info >}}
   The `<name>` is any identifier you choose. `custom-mode` is used here, but `CUSTOM_MODE` or any other style works just as well. It is matched exactly, including case, so the value of `scaling.parallelism-alignment.mode` and the `<name>` in `scaling.parallelism-alignment.mode.<name>.class` must be identical. As a result, `CUSTOM_MODE` and `custom-mode` are two different modes, not aliases. The only reserved names are the built-ins (`BALANCED`, `EVENLY_SPREAD`, `OFF`), which always resolve to the built-in modes regardless of any configured class.
   {{< /hint >}}

5. Deploy the mode.

    - **Operator deployment** - create a Dockerfile to build a custom image from the `apache/flink-kubernetes-operator` official image and copy the generated JAR to a custom alignment mode plugin directory under `/opt/flink/plugins` (the value of the `FLINK_PLUGINS_DIR` environment variable in the flink-kubernetes-operator helm chart). The structure of the custom alignment mode directory under `/opt/flink/plugins` is as follows:
        ```text
        /opt/flink/plugins
            ├── custom-alignment-mode
            │   ├── custom-alignment-mode.jar
            └── ...
        ```

        With the custom alignment mode directory location, the Dockerfile is defined as follows:
        ```shell script
        FROM apache/flink-kubernetes-operator
        ENV FLINK_PLUGINS_DIR=/opt/flink/plugins
        ENV CUSTOM_ALIGNMENT_MODE_DIR=custom-alignment-mode
        RUN mkdir $FLINK_PLUGINS_DIR/$CUSTOM_ALIGNMENT_MODE_DIR
        COPY custom-alignment-mode.jar $FLINK_PLUGINS_DIR/$CUSTOM_ALIGNMENT_MODE_DIR/
        ```

        Install the flink-kubernetes-operator helm chart with the custom image and verify the `deploy/flink-kubernetes-operator` log has:
        ```text
        o.a.f.k.o.u.AutoscalerUtils [INFO ] Discovered custom alignment mode for autoscaler from plugin directory[/opt/flink/plugins]: org.apache.flink.autoscaler.alignment.CustomAlignmentMode.
        ```

    - **Standalone autoscaler** - simply place the custom alignment mode JAR on the classpath of the `flink-autoscaler-standalone` process. It will be picked up automatically via Java's `ServiceLoader` and discovery will be logged:
        ```text
        o.a.f.a.s.AutoscalerUtils [INFO ] Discovered custom alignment mode via ServiceLoader: org.apache.flink.autoscaler.alignment.CustomAlignmentMode.
        ```

## Custom Scaling Executors

`ScalingExecutorPlugin` is a pluggable component that allows users to intercept, modify, or veto computed scaling decisions before they are applied. Custom scaling executors are discovered through the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism when running inside the Kubernetes operator, and through the standard Java `ServiceLoader` mechanism when running with `flink-autoscaler-standalone`. In both cases the implementation class must be registered in `META-INF/services`.

For each scaling cycle, the autoscaler invokes every custom scaling executor selected via the `job.autoscaler.scaling.custom-executors` configuration option. Plugins are chained in priority order (lower `priority()` values execute first) and each plugin receives the (possibly modified) output of the previous one. A plugin can approve the decision unchanged, modify the per-vertex `ScalingSummary` map, or veto the entire scaling operation by returning an empty map. A veto is surfaced as a `ScalingVetoed` Kubernetes event keyed by the plugin instance name. When the chain approves, the scaling proceeds and the final (possibly adjusted) decision is reported via the regular `ScalingReport` event.

All `job.autoscaler.*` keys related to custom scaling executors also support the legacy `kubernetes.operator.`-prefixed form as a fallback (for example, `kubernetes.operator.job.autoscaler.scaling.custom-executors`). The canonical key takes precedence on overlap.

The following steps demonstrate how to develop and use a custom scaling executor.

1. Implement the `ScalingExecutorPlugin` interface:
    ```java
    package org.apache.flink.autoscaler.custom;

    import org.apache.flink.autoscaler.ScalingExecutorPlugin;
    import org.apache.flink.autoscaler.ScalingSummary;
    import org.apache.flink.runtime.jobgraph.JobVertexID;

    import java.util.Collections;
    import java.util.Map;

    /** Custom implementation of {@link ScalingExecutorPlugin} that caps parallelism. */
    public class CustomScalingExecutor<KEY> implements ScalingExecutorPlugin<KEY> {

        @Override
        public int priority() {
            // Lower values execute earlier in the chain. Default is 0.
            return 0;
        }

        @Override
        public Map<JobVertexID, ScalingSummary> apply(
                Context<KEY> context, Map<JobVertexID, ScalingSummary> scalingSummaries) {
            // Read plugin-specific options from the prefix-stripped scoped Configuration.
            int maxParallelism =
                    context.getConfiguration().getInteger("max-parallelism", Integer.MAX_VALUE);

            // Example: veto the scaling operation if any vertex would exceed the cap.
            for (var summary : scalingSummaries.values()) {
                if (summary.getNewParallelism() > maxParallelism) {
                    return Collections.emptyMap();
                }
            }
            // Approve the decision unchanged.
            return scalingSummaries;
        }
    }
    ```

   The `Context` extends `JobAutoScalerContext` for the current job, exposing the evaluated scaling metrics and job topology through the inherited accessors. Its `getConfiguration()` returns the job configuration with this plugin instance's prefix-stripped options merged on top.

2. Create the service definition file `org.apache.flink.autoscaler.ScalingExecutorPlugin` in `META-INF/services` with the fully-qualified class name of your implementation:
    ```text
    org.apache.flink.autoscaler.custom.CustomScalingExecutor
    ```

3. Use the Maven tool to package the project and generate the custom scaling executor JAR.

4. Select one or more custom scaling executors via configuration. Each instance whose implementation class FQN matches the configured `job.autoscaler.scaling.custom-executor.<name>.class` value will be invoked, and any other `job.autoscaler.scaling.custom-executor.<name>.*` entries are passed to the plugin as plugin-specific configuration with the `job.autoscaler.scaling.custom-executor.<instance>.` prefix stripped. The configuration shape mirrors Flink's metric-reporter idiom: a list of named instances, plus a `.class` and free-form options under each instance namespace. Selection is purely by class FQN, and multiple instances of the same plugin class can be registered under different `<name>`s, each receiving its own scoped configuration. The chain executes in `priority()` order.
    ```yaml
    job.autoscaler.scaling.custom-executors: cap,audit
    job.autoscaler.scaling.custom-executor.cap.class: org.apache.flink.autoscaler.custom.CustomScalingExecutor
    job.autoscaler.scaling.custom-executor.cap.max-parallelism: 64
    job.autoscaler.scaling.custom-executor.audit.class: org.apache.flink.autoscaler.custom.AuditScalingExecutor
    ```

5. Deploy the custom scaling executor.

    - **Operator deployment** - create a Dockerfile to build a custom image from the `apache/flink-kubernetes-operator` official image and copy the generated JAR to a custom scaling executor plugin directory under `/opt/flink/plugins` (the value of the `FLINK_PLUGINS_DIR` environment variable in the flink-kubernetes-operator helm chart). The structure of the custom scaling executor directory under `/opt/flink/plugins` is as follows:
        ```text
        /opt/flink/plugins
            ├── custom-scaling-executor
            │   ├── custom-scaling-executor.jar
            └── ...
        ```

      With the custom scaling executor directory location, the Dockerfile is defined as follows:
        ```shell script
        FROM apache/flink-kubernetes-operator
        ENV FLINK_PLUGINS_DIR=/opt/flink/plugins
        ENV CUSTOM_SCALING_EXECUTOR_DIR=custom-scaling-executor
        RUN mkdir $FLINK_PLUGINS_DIR/$CUSTOM_SCALING_EXECUTOR_DIR
        COPY custom-scaling-executor.jar $FLINK_PLUGINS_DIR/$CUSTOM_SCALING_EXECUTOR_DIR/
        ```

      Install the flink-kubernetes-operator helm chart with the custom image and verify the `deploy/flink-kubernetes-operator` log has:
        ```text
        o.a.f.k.o.u.AutoscalerUtils [INFO ] Discovered custom scaling executor for autoscaler from plugin directory[/opt/flink/plugins]: org.apache.flink.autoscaler.custom.CustomScalingExecutor.
        ```

    - **Standalone autoscaler** - simply place the custom scaling executor JAR on the classpath of the `flink-autoscaler-standalone` process. It will be picked up automatically via Java's `ServiceLoader` and discovery will be logged:
        ```text
        o.a.f.a.s.u.AutoscalerUtils [INFO ] Discovered custom scaling executor via ServiceLoader: org.apache.flink.autoscaler.custom.CustomScalingExecutor.
        ```

   {{< hint info >}}
   Once configured, on every reconciliation `ScalingExecutor` resolves the chain against the per-job `Configuration`, sorts it by ascending `priority()`, and emits an INFO log line listing the executor plugins that will be applied in order. This is useful for verifying that all expected instances were picked up and chained correctly:
   ```text
   o.a.f.a.ScalingExecutor [INFO ] Custom scaling executors resolved and sorted by priority: [[name=cap, class=org.apache.flink.autoscaler.custom.CustomScalingExecutor, priority=0], [name=audit, class=org.apache.flink.autoscaler.custom.AuditScalingExecutor, priority=100]]
   ```
   {{< /hint >}}
