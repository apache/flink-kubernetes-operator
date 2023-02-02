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

# Flink Kubernetes Operator Beam application example

## Overview

This is an end-to-end example of running Beam application using the Flink Kubernetes Operator.

It is only intended to serve as a showcase of how Beam applications can be executed on the operator and users are expected to extend the implementation and dependencies based on their production needs. 

*What's in this example?*

 1. Word count Beam Java application in the Beam examples package
 2. DockerFile to build custom image for the application
 3. Example YAML for submitting the application using the operator

## How does it work?

The word count Beam application is firstly compiled with Flink runner and package into a Flink application. Then, shaded uber jar will be package into the docker image.

In the end, we use the built docker image in our `FlinkDeployment` yaml file for the deployment with Flink Operator.

## Usage

The following steps assume that you have the Flink Kubernetes Operator installed and running in your environment. If not, please follow
the Flink Kubernetes Operator [quickstart](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/) to start with.

**Step 1**: Build Beam example maven project
```bash
cd examples/flink-beam-example
mvn clean package
```

**Step 2**: The beam-examples-java artifact has many Beam applications. You may also use different the beam application to run by changing the mainClass of the pom file. 

**Step 3**: Build docker image
```bash
# Uncomment when building for local minikube env:
# eval $(minikube docker-env)

DOCKER_BUILDKIT=1 docker build . -t flink-beam-example:latest
```
This step will create an image based on an official Flink base image including Beam application jar.

**Step 4**: Create FlinkDeployment Yaml and Submit

Edit the included `beam-example.yaml` so that the main class and the `job.args` section points to application that you wish to execute, then submit it.

```bash
kubectl apply -f beam-example.yaml
```