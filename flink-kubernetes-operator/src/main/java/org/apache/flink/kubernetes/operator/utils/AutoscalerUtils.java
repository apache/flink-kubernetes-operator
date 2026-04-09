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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/** Autoscaler utilities. */
public class AutoscalerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    public static <T> T discoverOrDefault(
            Configuration conf, Class<T> clazz, Supplier<T> defaultFactory) {
        var iterator = PluginUtils.createPluginManagerFromRootFolder(conf).load(clazz);
        if (iterator.hasNext()) {
            T custom = iterator.next();
            LOG.info(
                    "Discovered custom {} from plugin directory: {}",
                    clazz.getSimpleName(),
                    custom.getClass().getName());
            if (iterator.hasNext()) {
                LOG.warn(
                        "Multiple implementations of {} found. Using: {}",
                        clazz.getSimpleName(),
                        custom.getClass().getName());
            }
            return custom;
        }
        LOG.info("No custom {} found in plugin directory, using default.", clazz.getSimpleName());
        return defaultFactory.get();
    }
}
