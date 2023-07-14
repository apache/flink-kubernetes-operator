/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Copied from Flink. Should be removed once the client dependency is upgraded to 1.18. */
public class JobResourceRequirements implements Serializable {

    private static final long serialVersionUID = 1L;

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder. */
    public static final class Builder {

        private final Map<JobVertexID, JobVertexResourceRequirements> vertexResources =
                new HashMap<>();

        public Builder setParallelismForJobVertex(
                JobVertexID jobVertexId, int lowerBound, int upperBound) {
            vertexResources.put(
                    jobVertexId,
                    new JobVertexResourceRequirements(
                            new JobVertexResourceRequirements.Parallelism(lowerBound, upperBound)));
            return this;
        }

        public JobResourceRequirements build() {
            return new JobResourceRequirements(vertexResources);
        }
    }

    private final Map<JobVertexID, JobVertexResourceRequirements> vertexResources;

    public JobResourceRequirements(
            Map<JobVertexID, JobVertexResourceRequirements> vertexResources) {
        this.vertexResources =
                Collections.unmodifiableMap(new HashMap<>(checkNotNull(vertexResources)));
    }

    public JobVertexResourceRequirements.Parallelism getParallelism(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(JobVertexResourceRequirements::getParallelism)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No requirement set for vertex " + jobVertexId));
    }

    public Map<JobVertexID, JobVertexResourceRequirements> getJobVertexParallelisms() {
        return vertexResources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JobResourceRequirements that = (JobResourceRequirements) o;
        return Objects.equals(vertexResources, that.vertexResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexResources);
    }

    @Override
    public String toString() {
        return "JobResourceRequirements{" + "vertexResources=" + vertexResources + '}';
    }
}
