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

# Flink Kubernetes Autoscaling Example

## Overview

This example contains two Flink applications showcasing the Flink Autoscaler capabilities:

- `AutoscalingExample.java` with its accompanying `autoscaling.yaml` containing the `FlinkDeployment` definition
- `LoadSimulationPipeline.java` with its accompanying `autoscaling-dynamic.yaml` containing the `FlinkDeployment` definition

Both applications are packaged into a single fat jar, which is then included in a Docker image
built from the provided `Dockerfile`.

### AutoscalingExample

This application contains a source that emits long values, a map function with an emulated
processing load, and a discard sink. The processing load per record can be configured by
adjusting the job argument in `autoscaling.yaml`:

```
job:
  args: ["10"]
```

The argument value specifies how many synthetic iterations are performed for each record.

### LoadSimulationPipeline

This application simulates fluctuating load that could be configured via the job arguments in 
`autoscaling-dynamic.yaml`:

```
job:
    args:
    - --maxLoadPerTask "1;2;4;8;16;\n16;8;4;2;1\n8;4;16;1;2" --repeatsAfterMinutes "60"
```

Refer to `LoadSimulationPipeline.java`'s JavaDoc and comments for the details concerning the argument
notation and simulated load pattern.

## Usage

The following steps assume that you have the Flink Kubernetes Operator installed and running in 
your environment. If not, please follow the Flink Kubernetes Operator [quickstart](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/) to start with.

**Step 1**: Build Autoscaling example maven project
```bash
cd examples/autoscaling
mvn clean package
```

**Step 2**: Build docker image
```bash
# Uncomment when building for local minikube env:
# eval $(minikube docker-env)

DOCKER_BUILDKIT=1 docker build . -t autoscaling-example:latest
```

This step will create an image based on an official Flink base image including the Autoscaling application jar.

**Step 3**: Only for AutoscalingExample: Mount volume to keep savepoints and checkpoints

```bash
# Assuming minikube is used for local testing or alternatively ensure any other k8s cluster setup with access to a persistent volume
mkdir /tmp/flink # or any other local directory
minikube mount --uid=9999 --gid=9999 /tmp/flink:/tmp/flink
```

**Step 4**: Submit FlinkDeployment Yaml

For *AutoscalingExample*:

```bash
kubectl apply -f autoscaling.yaml
```

or for *LoadSimulationPipeline*:

```bash
kubectl apply -f autoscaling-dynamic.yaml
```
