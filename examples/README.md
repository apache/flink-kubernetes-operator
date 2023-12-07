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

# Flink Kubernetes Operator Examples

## Overview

This directory contains few examples for the Flink Kubernetes Operator.
These examples should only serve as starting points when familiarizing yourself with the
Flink Kubernetes Operator and users are expected to extend these based on their production needs.

## Usage

Please refer to the [Quick Start](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start)
for setting up your environment to run the examples.

## Examples

### Basic Application Deployment example

This is a simple deployment defined by a minimal deployment file.
The configuration contains the following:
- Defines the job to run
- Assigns the resources available for the job
- Defines the parallelism used

To run the job submit the yaml file using kubectl:
```bash
kubectl apply -f basic.yaml
```

### Checkpointing & High Availability

Basic example to configure Flink Deployments in
[HA mode](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/ha/overview/).
The example shows how to set savepoint directory, checkpoint directory and HA. To try out this run the following command:
```bash
kubectl apply -f basic-checkpoint-ha.yaml
```

### Basic Session Deployment example

This example shows how to create a basic Session Cluster and then how to submit specific jobs to this cluster if needed.

#### Without jobs 

The Flink Deployment could be created without any jobs.
In this case the Flink jobs could be created later by submitting the jobs
separately.

The Flink Deployment configuration contains the following:
- The name of the Flink Deployment
- The resources available for the Flink Deployment

The Flink Deployment configuration does __NOT__ contain the following:
- The job to run
- Any job specific configurations

To create a Flink Deployment with the specific resources without any jobs run the following command:
```bash
kubectl apply -f basic-session-deployment-only.yaml
```

##### Adding jobs

For an existing Flink Deployment another configuration could be used to create new jobs.
This configuration should contain the following:
- The Flink Deployment to use
- The job to run
- Any job specific configurations

If the Flink Deployment is created by `basic-session-deployment-only.yaml` new job could be added
by the following command:
```bash
kubectl apply -f basic-session-job-only.yaml
```

#### Creating Deployment and Jobs together

Alternatively the Flink Deployment and the Flink Session Job configurations can be submitted together.

To try out this run the following command:
```bash
kubectl apply -f basic-session-deployment-and-job.yaml
```

### SQL runner

For running Flink SQL scripts check this [example](flink-sql-runner-example/README.md).

### Python example

For running Flink Python jobs check this [example](flink-python-example/README.md).

### Advanced examples

There are typical requirements for production systems and the examples below contain configuration files
showing how to archive some of these.

#### Custom logging

This example adds specific logging configuration for the Flink Deployment using the
`logConfiguration` property. To try out this run the following command:
```bash
kubectl apply -f custom-logging.yaml
```

#### Pod templates

Flink Kubernetes Operator provides the possibility to simplify the deployment descriptors by using
[Pod Templates](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/pod-template/).

This example shows how these templates are created and used. To try out this run the following command:
```bash
kubectl apply -f pod-template.yaml
```

#### Ingress

Flink's Web UI access can be configured by the
[Ingress](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/ingress/)
entries.

##### Basic Ingress example

Simple domain based ingress configuration could be tried out by running this command:
```bash
kubectl apply -f basic-ingress.yaml
```

##### Advanced Ingress example

It is possible to generate path based routing. To try out this run the following command:
```bash
kubectl apply -f advanced-ingress.yaml
```

#### Horizontal Pod Autoscaler

It is possible to provide the Horizontal Pod Autoscaler configuration through the yaml files.
To run this example Kubernetes 1.23 or newer is needed, so the `autoscaling/v2` is available.

The feature is experimental so use it with caution but here is an example configuration:
```bash
kubectl apply -f hpa/basic-hpa.yaml
```

#### Using Kustomize

[This](kustomize) example shows
how to override default values using [kustomize](https://kustomize.io/)

For the detailed description of advanced configuration techniques follow this 
[link](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/helm/#advanced-customization-techniques).

#### Enabling TLS on your deployments

In order for the operator to communicate with the rest service of a deployment you need to mount a jks/pkcs12 secret onto the operator that uses the same ca certificate as those used by your deployments.
N.b. Make sure you use the same mount location and keystore password for your deployment as you have for the operator
[This](flink-tls-example) example provides a pre-install.yaml file that you would need to apply to your cluster before helm installing your cluster using the value tls.create=true. It creates an issuer that is CA Certificate backed that can be used for both the operator and the examples provided