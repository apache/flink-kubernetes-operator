/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.metrics.MetricGroup;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** FlinkSessionJob metrics. */
public class FlinkSessionJobMetrics implements CustomResourceMetrics<FlinkSessionJob> {

    private final Set<String> sessionJobs = ConcurrentHashMap.newKeySet();
    public static final String METRIC_GROUP_NAME = "FlinkSessionJob";

    public FlinkSessionJobMetrics(MetricGroup parentMetricGroup) {
        MetricGroup flinkSessionJobMetrics = parentMetricGroup.addGroup(METRIC_GROUP_NAME);
        flinkSessionJobMetrics.gauge("Count", () -> sessionJobs.size());
    }

    public void onUpdate(FlinkSessionJob sessionJob) {
        sessionJobs.add(sessionJob.getMetadata().getName());
    }

    public void onRemove(FlinkSessionJob sessionJob) {
        sessionJobs.remove(sessionJob.getMetadata().getName());
    }
}
