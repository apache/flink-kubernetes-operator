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

# Flink Kubernetes Client Code Example

## Overview

This is an end-to-end example of running Flink Job via code using the Flink Kubernetes Operator.

*What's in this example?*

1. Sample code for submitting an application similar to examples/basic.yaml programmatically.

## How does it work?

Just like when we apply k8 yaml file to k8 cluster, internally operator does create all deployment objects and submit.


## Usage

The following steps assume that you have the Flink Kubernetes Operator installed and running in your environment.

**Step 1**: Build K8 client maven project
```bash
cd examples/kubernetes-client-examples
mvn clean package
```

**Step 2**: Run the main method in `Basic.java`