---
title: Building from Source
weight: 2
type: docs
aliases:
  - /development/building.html
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

# Build Flink Kubernetes Operator

This page covers how to build Flink Kubernetes Operator {{< version >}} from sources.

In order to build the operator you need to [clone the git repository]({{< github_repo >}}).

```bash
git clone {{< github_repo >}}
```

In addition you need **Maven 3** and a **JDK** (Java Development Kit). Flink Kubernetes Operator requires **Java 11** to build.

The simplest way of building is by running:

```bash
mvn clean install
```

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new binary (`install`).

To speed up the build you can:
- skip tests by using ' -DskipTests'
- use Maven's parallel build feature, e.g., 'mvn package -T 1C' will attempt to build 1 module for each CPU core in parallel.

The build script will be:
```bash
mvn clean install -DskipTests -T 1C
```


