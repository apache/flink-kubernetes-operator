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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;

import java.time.Instant;
import java.util.List;

/** Utility methods for the FlinkBlueGreenDeploymentController. */
public class FlinkBlueGreenDeploymentUtils {

    public static ObjectMeta getDependentObjectMeta(FlinkBlueGreenDeployment bgDeployment) {
        ObjectMeta bgMeta = bgDeployment.getMetadata();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace(bgMeta.getNamespace());
        objectMeta.setOwnerReferences(
                List.of(
                        new OwnerReference(
                                bgDeployment.getApiVersion(),
                                true,
                                false,
                                bgDeployment.getKind(),
                                bgMeta.getName(),
                                bgMeta.getUid())));
        return objectMeta;
    }

    public static <T> T adjustNameReferences(
            T spec,
            String deploymentName,
            String childDeploymentName,
            String wrapperKey,
            Class<T> valueType) {
        String serializedSpec = SpecUtils.writeSpecAsJSON(spec, wrapperKey);
        String replacedSerializedSpec = serializedSpec.replace(deploymentName, childDeploymentName);
        return SpecUtils.readSpecFromJSON(replacedSerializedSpec, wrapperKey, valueType);
    }

    public static String millisToInstantStr(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }

    public static long instantStrToMillis(String instant) {
        if (instant == null) {
            return 0;
        }
        return Instant.parse(instant).toEpochMilli();
    }

    public static <T> T getConfigOption(
            FlinkBlueGreenDeployment bgDeployment, ConfigOption<T> option) {
        return Configuration.fromMap(bgDeployment.getSpec().getTemplate().getConfiguration())
                .get(option);
    }
}
