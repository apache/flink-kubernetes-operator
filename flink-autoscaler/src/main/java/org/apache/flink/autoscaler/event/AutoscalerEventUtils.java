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

package org.apache.flink.autoscaler.event;

import org.apache.flink.annotation.Experimental;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** The utils of {@link AutoScalerEventHandler}. */
@Experimental
public class AutoscalerEventUtils {

    private static final Pattern SCALING_REPORT_SEPARATOR = Pattern.compile("\\{(.+?)\\}");
    private static final Pattern VERTEX_SCALING_REPORT_PATTERN =
            Pattern.compile(
                    "Vertex ID (.*?) \\| Parallelism (.*?) -> (.*?) \\| Processing capacity (.*?) -> (.*?) \\| Target data rate (.*)");

    /** Parse the scaling report from original scaling report event. */
    public static List<VertexScalingReport> parseVertexScalingReports(String scalingReport) {
        final List<String> originalVertexScalingReports =
                extractOriginalVertexScalingReports(scalingReport);
        return originalVertexScalingReports.stream()
                .map(AutoscalerEventUtils::extractVertexScalingReport)
                .collect(Collectors.toList());
    }

    private static List<String> extractOriginalVertexScalingReports(String scalingReport) {
        var result = new ArrayList<String>();
        var m = SCALING_REPORT_SEPARATOR.matcher(scalingReport);

        while (m.find()) {
            result.add(m.group(1));
        }
        return result;
    }

    private static VertexScalingReport extractVertexScalingReport(String vertexScalingReportStr) {
        final var vertexScalingReport = new VertexScalingReport();
        var m = VERTEX_SCALING_REPORT_PATTERN.matcher(vertexScalingReportStr);

        if (m.find()) {
            vertexScalingReport.setVertexId(m.group(1));
            vertexScalingReport.setCurrentParallelism(Integer.parseInt(m.group(2)));
            vertexScalingReport.setNewParallelism(Integer.parseInt(m.group(3)));
            vertexScalingReport.setCurrentProcessCapacity(Double.parseDouble(m.group(4)));
            vertexScalingReport.setExpectedProcessCapacity(Double.parseDouble(m.group(5)));
            vertexScalingReport.setTargetDataRate(Double.parseDouble(m.group(6)));
        }
        return vertexScalingReport;
    }
}
