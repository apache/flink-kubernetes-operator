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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;

import io.javaoperatorsdk.operator.config.runtime.AnnotationConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Custom config for {@link FlinkDeploymentController}. */
public class FlinkControllerConfig extends AnnotationConfiguration<FlinkDeployment> {

    private static final String NAMESPACES_SPLITTER_KEY = ",";

    public FlinkControllerConfig(FlinkDeploymentController reconciler) {
        super(reconciler);
    }

    @Override
    public Set<String> getNamespaces() {
        String watchedNamespaces = EnvUtils.get(EnvUtils.ENV_WATCHED_NAMESPACES);

        if (StringUtils.isEmpty(watchedNamespaces)) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(Arrays.asList(watchedNamespaces.split(NAMESPACES_SPLITTER_KEY)));
        }
    }
}
