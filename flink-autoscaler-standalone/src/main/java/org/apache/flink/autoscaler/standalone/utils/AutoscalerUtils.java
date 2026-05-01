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

package org.apache.flink.autoscaler.standalone.utils;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingExecutorPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

/** Autoscaler related utility methods for the standalone autoscaler. */
public class AutoscalerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    /**
     * Discovers custom scaling executors for the standalone autoscaler via Java's {@link
     * ServiceLoader} mechanism. Implementations must be registered under {@code
     * META-INF/services/org.apache.flink.autoscaler.ScalingExecutorPlugin} on the classpath.
     *
     * @return The list of discovered custom scaling executors.
     */
    @SuppressWarnings("unchecked")
    public static <KEY, Context extends JobAutoScalerContext<KEY>>
            Collection<ScalingExecutorPlugin<KEY, Context>> discoverCustomScalingExecutors() {
        List<ScalingExecutorPlugin<KEY, Context>> customScalingExecutors = new ArrayList<>();
        ServiceLoader.load(ScalingExecutorPlugin.class)
                .forEach(
                        plugin -> {
                            LOG.info(
                                    "Discovered custom scaling executor via ServiceLoader: {}.",
                                    plugin.getClass().getName());
                            customScalingExecutors.add(
                                    (ScalingExecutorPlugin<KEY, Context>) plugin);
                        });
        return customScalingExecutors;
    }
}
