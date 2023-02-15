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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.kubernetes.operator.reconciler.deployment.JobAutoScalerFactory;
import org.apache.flink.kubernetes.operator.reconciler.deployment.NoopJobAutoscalerFactory;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

/** Loads the active Autoscaler implementation from the classpath. */
public class AutoscalerLoader {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerLoader.class);

    public static JobAutoScalerFactory loadJobAutoscalerFactory() {
        JobAutoScalerFactory factory = null;
        boolean singleImplementation = true;

        for (JobAutoScalerFactory discoveredFactory :
                ServiceLoader.load(JobAutoScalerFactory.class)) {
            LOG.info(
                    "Discovered JobAutoScaler factory: {}", discoveredFactory.getClass().getName());
            singleImplementation = factory == null;
            factory = discoveredFactory;
        }

        if (factory == null) {
            LOG.info("No JobAutoscaler implementation found. Autoscaling is disabled.");
            return new NoopJobAutoscalerFactory();
        }

        Preconditions.checkState(
                singleImplementation,
                "Found multiple implementation for JobAutoScalerFactory. Please ensure only one implementation is present.");

        return factory;
    }
}
