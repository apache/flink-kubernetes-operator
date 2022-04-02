---
title: "Configuration"
weight: 2
type: docs
aliases:
- /operations/configuration.html
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

# Configuration

## Operator

| Key  | Default | Type | Description |
| ---- | ------- | ---- | ----------- |
| operator.reconciler.reschedule.interval     |    60s     |  Duration    | The interval for the controller to reschedule the reconcile process.            |
| operator.observer.rest-ready.delay    |  10s       |     Duration |     Final delay before deployment is marked ready after port becomes accessible.        |
| operator.reconciler.max.parallelism     |     5    |  Integer    |    The maximum number of threads running the reconciliation loop. Use -1 for infinite.         |
| operator.observer.progress-check.interval     |  10s       |  Duration    |     The interval for observing status for in-progress operations such as deployment and savepoints.        |
| operator.observer.savepoint.trigger.grace-period     |     10s    |   Duration   |   The interval before a savepoint trigger attempt is marked as unsuccessful.          |
| operator.observer.flink.client.timeout     |     10s    |  Duration    | The timeout for the observer to wait the flink rest client to return.            |
