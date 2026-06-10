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

import org.apache.flink.autoscaler.alignment.AlignmentMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

/** Utilities for wiring the standalone autoscaler. */
public class AutoscalerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    private AutoscalerUtils() {}

    /**
     * Discovers custom {@link AlignmentMode} implementations from the application classpath using
     * {@link ServiceLoader}. Built-in modes are not plugins and are resolved by name, so they are
     * not returned here.
     */
    public static Collection<AlignmentMode> discoverAlignmentModes() {
        List<AlignmentMode> alignmentModes = new ArrayList<>();
        ServiceLoader.load(AlignmentMode.class)
                .forEach(
                        mode -> {
                            LOG.info("Discovered alignment mode: {}.", mode.getClass().getName());
                            alignmentModes.add(mode);
                        });
        return alignmentModes;
    }
}
