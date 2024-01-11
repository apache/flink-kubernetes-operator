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

The [Flink Kubernetes Operator](https://github.com/apache/flink-kubernetes-operator) extends the [Kubernetes](https://kubernetes.io/) API with the ability to manage and operate
Flink Deployments. The operator features the following amongst others:
- Deploy and monitor Flink Application and Session deployments
- Upgrade, suspend and delete deployments
- Full logging and metrics integration
- Flexible deployments and native integration with Kubernetes tooling
- Flink Job Autoscaler

{{< img src="/img/overview.svg" alt="Flink Kubernetes Operator Overview" >}}

For an overview and brief demo of the Flink Kubernetes Operator you can check out our initial meetup talk [recording](https://www.youtube.com/watch?v=bedzs9jBUfc&t=2121s).

{{< columns >}}
## Try the Flink Kubernetes Operator

If you’re interested in playing around with the operator, check out our [quickstart]({{< ref "docs/try-flink-kubernetes-operator/quick-start" >}}). It provides a step by
step introduction to deploying the operator and an example job.

<--->

## Get Help with the Flink Kubernetes Operator

If you get stuck, check out our [community support
resources](https://flink.apache.org/community.html). In particular, Apache
Flink’s user mailing list is consistently ranked as one of the most active of
any Apache project, and is a great way to get help quickly.

{{< /columns >}}

The Flink Kubernetes Operator is developed under the umbrella of [Apache
Flink](https://flink.apache.org/).
