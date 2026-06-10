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

import org.apache.flink.autoscaler.alignment.AlignmentMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Utilities for wiring the autoscaler in the Kubernetes operator runtime. */
public class AutoscalerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    private AutoscalerUtils() {}

    /**
     * Discovers custom {@link AlignmentMode} implementations from the operator plugin directory
     * using Flink's {@code PluginManager} (isolated classloaders). Built-in modes are not plugins
     * and are resolved by name, so they are not returned here.
     */
    public static Collection<AlignmentMode> discoverAlignmentModes(Configuration conf) {
        List<AlignmentMode> alignmentModes = new ArrayList<>();
        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(AlignmentMode.class)
                .forEachRemaining(
                        mode -> {
                            LOG.info(
                                    "Discovered alignment mode plugin: {}.",
                                    mode.getClass().getName());
                            alignmentModes.add(mode);
                        });
        return alignmentModes;
    }
}
