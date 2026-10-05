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

# Flink Blue/Green ADVANCED Mode Example

## Overview

A job for the `ADVANCED` transition mode of `FlinkBlueGreenDeployment`, whose output shows how a
transition split the records between the two deployments. It is the job of the
`e2e-tests/test_bluegreen_advanced.sh` end-to-end test.

The records carry a sequence id, and their event time follows the id: most are in order, every
10th is out of order within the 2 second watermark bound, and every 100th is late, 2 minutes older
than its neighbours. The file sink writes one line per record, `<color>,<id>,<eventTime>`, where the
color is the deployment that wrote it, and commits on checkpoints.

The job bundles the gate classes, `flink-kubernetes-operator-bluegreen-client`, built against the
Flink version of the image: the default build targets Flink 1.x, and `-Dflink.version=<2.x>` builds
it against Flink 2.x.

## Usage

Build the job against the Flink version of the image, then the image:

```bash
mvn install -DskipTests -pl flink-kubernetes-operator-bluegreen-client -am
mvn package -DskipTests -Dflink.version=2.2.1 \
  -pl flink-kubernetes-operator-bluegreen-client,examples/flink-bluegreen-advanced-example
docker build --build-arg FLINK_IMAGE=flink:2.2-java17 -t flink-bluegreen-advanced-example:latest \
  examples/flink-bluegreen-advanced-example
```

`e2e-tests/data/bluegreen-advanced.yaml` deploys it, with the gate reading the event time through
`bluegreen.gate.watermark.field-path: eventTime`.
