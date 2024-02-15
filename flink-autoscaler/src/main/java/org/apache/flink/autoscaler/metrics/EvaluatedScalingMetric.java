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

package org.apache.flink.autoscaler.metrics;

import lombok.Data;
import lombok.NoArgsConstructor;

/** Evaluated scaling metric. */
@Data
@NoArgsConstructor
public class EvaluatedScalingMetric {
    private double current;

    private double average;

    public EvaluatedScalingMetric(double current, double average) {
        this.current = ScalingMetrics.roundMetric(current);
        this.average = ScalingMetrics.roundMetric(average);
    }

    public static EvaluatedScalingMetric of(double current) {
        return new EvaluatedScalingMetric(current, Double.NaN);
    }

    public static EvaluatedScalingMetric avg(double average) {
        return new EvaluatedScalingMetric(Double.NaN, average);
    }
}
