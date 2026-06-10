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

`FlinkResourceMutator`, an interface for mutating the resources of `FlinkDeployment` and `FlinkSessionJob`,  is a pluggable component based on the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism. During development, we can customize the implementation of `FlinkResourceMutator` and make sure to retain the service definition in `META-INF/services`.
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

## Custom Parallelism Alignment Modes

The autoscaler aligns a vertex's computed target parallelism to the number of key groups (keyBy) or source partitions to reduce data skew. The built-in modes (`BALANCED`, `EVENLY_SPREAD`, `OFF`) are selected by name through `job.autoscaler.scaling.alignment.mode`. You can also provide a custom alignment mode as a plugin by implementing the `@Experimental` `AlignmentMode` SPI. Custom modes are discovered via the [Plugins](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/plugins) mechanism in the operator, and via `ServiceLoader` in the standalone autoscaler.

The following steps demonstrate how to develop and use a custom alignment mode.

1. Implement `org.apache.flink.autoscaler.alignment.AlignmentMode`:
    ```java
    package org.apache.flink.autoscaler.alignment;

    /** Custom alignment mode snapping the computed parallelism down to the nearest divisor. */
    public class CustomAlignmentMode implements AlignmentMode {

        /** Apply to every vertex, not only the source and keyBy vertices the built-ins handle. */
        @Override
        public boolean isApplicable(AlignmentContext ctx) {
            return true;
        }

        @Override
        public int align(AlignmentContext ctx) {
            int n = ctx.numKeyGroupsOrPartitions();
            for (int p = ctx.getNewParallelism(); p >= 1; p--) {
                if (n % p == 0) {
                    return p;
                }
            }
            return ctx.getNewParallelism();
        }
    }
    ```
   The `AlignmentContext` exposes the current and computed target parallelism, the number of key groups or source partitions, the input ship strategies, the `JobAutoScalerContext`, the per-vertex evaluated metrics, the job topology, and the prefix-stripped per-mode `Configuration` (`getModeConfiguration()`). The autoscaler calls `align` only when `isApplicable(AlignmentContext)` returns true. That method defaults to source and keyBy (hash) vertices, and a custom mode can override it to widen the scope, for example to align custom partitioned vertices.

2. Create the service definition file `org.apache.flink.autoscaler.alignment.AlignmentMode` in `META-INF/services`:
    ```text
    org.apache.flink.autoscaler.alignment.CustomAlignmentMode
    ```

3. Use the Maven tool to package the project and add the generated JAR to the operator plugins directory (`/opt/flink/plugins`), the same way as a custom validator above (a Dockerfile copying the JAR into a subdirectory of `FLINK_PLUGINS_DIR`). For the standalone autoscaler, place the JAR on the application classpath instead.

4. Select the custom mode by name and point it at your implementation class. Any `job.autoscaler.scaling.alignment.mode.<name>.*` keys are passed to the mode (prefix-stripped) through `AlignmentContext#getModeConfiguration()`:
    ```yaml
    job.autoscaler.scaling.alignment.mode: custom-mode
    job.autoscaler.scaling.alignment.mode.custom-mode.class: org.apache.flink.autoscaler.alignment.CustomAlignmentMode
    ```
   {{< hint info >}}
   The `<name>` is any identifier you choose. `custom-mode` is used here, but `CUSTOM_MODE` or any other style works just as well. It is matched exactly, including case, so the value of `scaling.alignment.mode` and the `<name>` in `scaling.alignment.mode.<name>.class` must be identical. As a result, `CUSTOM_MODE` and `custom-mode` are two different modes, not aliases. The only reserved names are the built-ins (`BALANCED`, `EVENLY_SPREAD`, `OFF`), which always resolve to the built-in modes regardless of any configured class.
   {{< /hint >}}

5. Install the flink-kubernetes-operator helm chart with the custom image and verify the `deploy/flink-kubernetes-operator` log has:
    ```text
    o.a.f.k.o.u.AutoscalerUtils [INFO ] Discovered alignment mode plugin: org.apache.flink.autoscaler.alignment.CustomAlignmentMode.
    ```
