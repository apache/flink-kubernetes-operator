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

package org.apache.flink.kubernetes.operator.validation;

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;

import java.util.Optional;

/** Validator for different resources. */
public interface FlinkResourceValidator extends Plugin {

    /**
     * Validate and return optional error.
     *
     * @param deployment A Flink application or session cluster deployment.
     * @return Optional error string, should be present iff validation resulted in an error
     */
    Optional<String> validateDeployment(FlinkDeployment deployment);

    /**
     * Validate and return optional error.
     *
     * @param sessionJob the session job to be validated.
     * @param session the target session cluster of the session job to be validated.
     * @return Optional error string, should be present iff validation resulted in an error
     */
    Optional<String> validateSessionJob(
            FlinkSessionJob sessionJob, Optional<FlinkDeployment> session);

    /**
     * Validate and return optional error.
     *
     * @param savepoint the savepoint to be validated.
     * @param target the target resource of the savepoint to be validated.
     * @return Optional error string, should be present iff validation resulted in an error
     */
    Optional<String> validateStateSnapshot(
            FlinkStateSnapshot savepoint, Optional<AbstractFlinkResource<?, ?>> target);
}
