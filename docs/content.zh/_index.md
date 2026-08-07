---
title: Apache Flink Kubernetes Operator
type: docs
bookToc: false
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

# Flink Kubernetes Operator

The [Flink Kubernetes Operator](https://github.com/apache/flink-kubernetes-operator), developed under the umbrella of [Apache Flink](https://flink.apache.org/), extends the [Kubernetes](https://kubernetes.io/) API with custom resources for running Flink applications. Flink deployments are declared like any other Kubernetes workload, and the operator runs their whole operational life:

- **Lifecycle Management**: deployment, stateful upgrades, rollbacks, and self-healing
- **Zero-Downtime Upgrades**: blue/green deployments that switch over only once the new version is proven healthy
- **Autoscaling**: parallelism and memory continuously right-sized to the observed load
- **Kubernetes-Native Operations**: Helm installation, RBAC, high availability, metrics, logging, and ingress

{{< img src="/img/overview.svg" alt="Flink Kubernetes Operator Overview" >}}

For an overview and brief demo of the Flink Kubernetes Operator you can check out our initial meetup talk [recording](https://www.youtube.com/watch?v=bedzs9jBUfc&t=2121s).

{{< columns >}}
## Try It Out

If you're interested in playing around with the operator, check out the [Quick Start]({{< ref "docs/try-flink-kubernetes-operator/quick-start" >}}). It provides a step-by-step introduction to deploying the operator and an example job.

<--->

## Explore the Documentation

The [Concepts]({{< ref "docs/concepts/overview" >}}) section gives the complete picture of what the operator delivers and how it works, the [Custom Resource]({{< ref "docs/custom-resource/overview" >}}) reference describes the resources through which everything is declared, and the [Managing Flink Jobs]({{< ref "docs/managing/job-management" >}}) pages cover day-to-day job operations.

{{< /columns >}}

{{< columns >}}
## Deploy to Production

The [Deployment]({{< ref "docs/deployment/overview" >}}) section covers the Helm installation, configuration, security, and high availability of the operator itself.

<--->

## Get Help

If you get stuck, check out the Flink [community support resources](https://flink.apache.org/community.html). In particular, Apache Flink's user mailing list is one of the most active in the Apache community, and a great way to get help quickly.

{{< /columns >}}
