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

2. Create service definition file `org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator` in `META-INF/services`.   With custom `FlinkResourceValidator` implementation, the service definition describes as follows:
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

`FlinkResourceMutator`, an interface for ,mutating the resources of `FlinkDeployment` and `FlinkSessionJob`,  is a pluggable component based on the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism. During development, we can customize the implementation of `FlinkResourceMutator` and make sure to retain the service definition in `META-INF/services`.
The following steps demonstrate how to develop and use a custom mutator.

1. Implement `FlinkResourceMutator` interface:
   ```java
   package org.apache.flink.mutator;

   import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
   import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
   import org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator;
   
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

2. Create service definition file `org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator` in `META-INF/services`.   With custom `FlinkResourceMutator` implementation, the service definition describes as follows:
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

`FlinkAutoscalerEvaluator` is a pluggable component that allows users to provide custom scaling-metric evaluation logic on top of the metrics evaluated internally by the autoscaler. Custom metric evaluators are discovered through the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism when running inside the Kubernetes operator, and through the standard Java `ServiceLoader` mechanism when running with `flink-autoscaler-standalone`. In both cases the implementation class must be registered in `META-INF/services`.

For each evaluation cycle, the autoscaler invokes the custom metric evaluator selected via the `job.autoscaler.metrics.custom-evaluators` configuration option once per job vertex. The metrics returned by the custom evaluator are merged on top of the internally evaluated metrics, allowing users to override or augment specific `ScalingMetric` values (e.g. `TARGET_DATA_RATE`, `TRUE_PROCESSING_RATE`, `CATCH_UP_DATA_RATE`).

All `job.autoscaler.*` keys related to custom metric evaluators also support the legacy `kubernetes.operator.`-prefixed form as a fallback (for example, `kubernetes.operator.job.autoscaler.metrics.custom-evaluators`). The canonical key takes precedence on overlap.

The following steps demonstrate how to develop and use a custom metric evaluator.

1. Implement the `FlinkAutoscalerEvaluator` interface:
    ```java
    package org.apache.flink.autoscaler.custom;

    import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
    import org.apache.flink.autoscaler.metrics.FlinkAutoscalerEvaluator;
    import org.apache.flink.autoscaler.metrics.ScalingMetric;
    import org.apache.flink.runtime.jobgraph.JobVertexID;

    import java.util.HashMap;
    import java.util.Map;

    /** Custom metric evaluator implementation of {@link FlinkAutoscalerEvaluator}. */
    public class CustomEvaluator implements FlinkAutoscalerEvaluator {


        @Override
        public Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
                JobVertexID vertex,
                Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
                Context context) {
            Map<ScalingMetric, EvaluatedScalingMetric> overrides = new HashMap<>();
            // Example: override the target data rate for source vertices based
            // on a value read from the evaluator-specific configuration.
            double target = context.getCustomEvaluatorConf().getDouble("target-data-rate", 0.0);
            if (target > 0 && context.getTopology().isSource(vertex)) {
                overrides.put(
                        ScalingMetric.TARGET_DATA_RATE,
                        EvaluatedScalingMetric.avg(target));
            }
            return overrides;
        }
    }
    ```

   The `Context` object exposes an un-modifiable view of the job configuration, the metrics history, previously evaluated vertex metrics (evaluation happens topologically), the job topology, backlog status, max restart time, and the evaluator-specific configuration.

2. Create the service definition file `org.apache.flink.autoscaler.metrics.FlinkAutoscalerEvaluator` in `META-INF/services` with the fully-qualified class name of your implementation:
    ```text
    org.apache.flink.autoscaler.custom.CustomEvaluator
    ```

3. Use the Maven tool to package the project and generate the custom metric evaluator JAR.

4. Select the custom metric evaluator via configuration. The evaluator whose implementation class FQN matches the configured `job.autoscaler.metrics.custom-evaluator.<name>.class` value will be invoked, and any other `job.autoscaler.metrics.custom-evaluator.<name>.*` entries are passed to the evaluator as evaluator-specific configuration with the `job.autoscaler.metrics.custom-evaluator.<instance>.` prefix stripped. The configuration shape mirrors Flink's metric-reporter idiom: a list of named instances, plus a `.class` and free-form options under each instance namespace. Selection is purely by class FQN.
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
