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

# Flink Kubernetes Operator SQL Example

## Overview

This is an end-to-end example of running Flink SQL scripts using the Flink Kubernetes Operator.

It is only intended to serve as a showcase of how Flink SQL can be executed on the operator and users are expected to extend the implementation and dependencies based on their production needs. 

Currently, it is not planned to add direct API support for SQL submission to the Kubernetes operator due to the complexity of image and dependency management that is specific to each use-case.
At the same time we are confident that using these examples as a starting point the operator would cover all user needs. If Apache Flink itself extends the SQL support for Application mode in the future, the operator will aim to support that.

*What's in this example?*

 1. SQL Script runner Flink Java application
 2. DockerFile to build custom image with script runner + SQL scripts
 3. Example YAML for submitting scripts using the operator

## How does it work?

As Flink doesn't support submitting SQL scripts directly as jobs, we have created a simple Flink Java application that takes the user script and executes it using the `TableEnvironment#executeSql` method.

The SQL Runner will allow us to execute SQL scripts as if they were simple Flink Application jars, something that already works quite well with the operator. We package the included SQL Runner implementation together with the SQL scripts under `sql-scripts` into a docker image and we use it in our `FlinkDeployment` yaml file.

***Note:*** *While the included SqlRunner should work for most simple cases, it is not expected to be very robust or battle tested. If you find any bugs or limitations, feel free to open Jira tickets and bugfix PRs.*

## Usage

The following steps assume that you have the Flink Kubernetes Operator installed and running in your environment.

**Step 1**: Build Sql Runner maven project
```bash
cd examples/flink-sql-runner-example
mvn clean package
```

**Step 2**: Add your SQL script files under the `sql-scripts` directory

**Step 3**: Build docker image
```bash
# Uncomment when building for local minikube env:
# eval $(minikube docker-env)

DOCKER_BUILDKIT=1 docker build . -t flink-sql-runner-example:latest
```
This step will create an image based on an official Flink base image including the SQL runner jar and your user scripts.

**Step 4**: Create FlinkDeployment Yaml and Submit

Edit the included `sql-example.yaml` so that the `job.args` section points to the SQL script that you wish to execute, then submit it.

```bash
kubectl apply -f sql-example.yaml
```

## Connectors and other extensions

This example will only work with the very basic table and connector types out of the box, however enabling new ones is very easy.

Simply find your [required connector](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/overview/) and add it as compile dependency to the `flink-sql-runner-example` project `pom.xml`. This will ensure that is packaged into the sql-runner fatjar and will be available for you on the cluster.

Once you dive deeper you will quickly find that the SqlRunner implementation is very basic and might not cover your more advanced needs. Feel free to simply extend or customise the code as necessary for your requirements.
