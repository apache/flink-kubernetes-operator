---
title: "Overview"
weight: 1
type: docs
aliases:
- /internals/overview.html
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

# Overview

The Internals section documents how the operator is implemented: the runtime flows, the classes behind them, and the exact decision points, with pointers into the code. It is written for contributors, and for operators who need to understand behavior beyond what the user-facing sections describe. It assumes the concept-level picture from [Architecture]({{< ref "docs/concepts/architecture" >}}).

The pages follow the operator's own runtime order:

- [Startup]({{< ref "docs/internals/startup" >}}): from the Helm-installed Deployment to a running JVM, configuration loading, SPI discovery, controller registration, and the launch of the health and metrics services.
- [Controllers]({{< ref "docs/internals/controllers" >}}): the reconcile machinery, the resource class hierarchy, the controllers, observers, and reconcilers, the spec diff, and the blue/green controller.
- [Webhook]({{< ref "docs/internals/webhook" >}}): the admission path, from the TLS endpoint through the mutator and validator chains.
- [Autoscaler]({{< ref "docs/internals/autoscaler" >}}): the full derivation of a scaling decision, every metric, formula, and branch, and how the result reaches the running job.
