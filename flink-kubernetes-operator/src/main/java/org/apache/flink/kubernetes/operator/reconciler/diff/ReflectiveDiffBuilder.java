/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.reconciler.diff;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.diff.Diffable;
import org.apache.flink.kubernetes.operator.api.diff.SpecDiff;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import lombok.NonNull;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.Builder;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.flink.kubernetes.operator.api.diff.DiffType.UPGRADE;

/**
 * Assists in comparing {@link Diffable} objects with reflection.
 *
 * <p>Inspired by:
 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/builder/ReflectionDiffBuilder.java
 */
@Experimental
public class ReflectiveDiffBuilder<T> implements Builder<DiffResult<T>> {

    private final KubernetesDeploymentMode deploymentMode;
    private final Object before;
    private final Object after;
    private final DiffBuilder<T> diffBuilder;

    public ReflectiveDiffBuilder(
            KubernetesDeploymentMode deploymentMode,
            @NonNull final T before,
            @NonNull final T after) {
        this.deploymentMode = deploymentMode;
        this.before = before;
        this.after = after;
        diffBuilder = new DiffBuilder<>(before, after);
        clearIgnoredFields(before);
        clearIgnoredFields(after);
    }

    @Override
    public DiffResult<T> build() {
        if (before.equals(after)) {
            return diffBuilder.build();
        }

        appendFields(before.getClass());
        return diffBuilder.build();
    }

    private void appendFields(final Class<?> clazz) {
        for (final Field field : FieldUtils.getAllFields(clazz)) {
            if (accept(field)) {
                try {
                    var leftField = readField(field, before, true);
                    var rightField = readField(field, after, true);

                    if (field.isAnnotationPresent(SpecDiff.Config.class)
                            && Map.class.isAssignableFrom(field.getType())) {
                        diffBuilder.append(
                                field.getName(),
                                configDiff(
                                        field,
                                        (leftField != null)
                                                ? (Map<String, String>) leftField
                                                : new HashMap<>(),
                                        (rightField != null)
                                                ? (Map<String, String>) rightField
                                                : new HashMap<>()));
                    } else if (field.isAnnotationPresent(SpecDiff.class)) {
                        var annotation = field.getAnnotation(SpecDiff.class);
                        var modes = annotation.mode();
                        boolean modeApplies =
                                modes.length == 0 || Arrays.asList(modes).contains(deploymentMode);
                        if (rightField != null || !annotation.onNullIgnore()) {
                            diffBuilder.append(
                                    field.getName(),
                                    leftField,
                                    rightField,
                                    modeApplies ? annotation.value() : UPGRADE);
                        }
                    } else if (Diffable.class.isAssignableFrom(field.getType())
                            && ObjectUtils.allNotNull(leftField, rightField)) {
                        diffBuilder.append(
                                field.getName(),
                                new ReflectiveDiffBuilder<T>(
                                                deploymentMode, (T) leftField, (T) rightField)
                                        .build());

                    } else {
                        diffBuilder.append(field.getName(), leftField, rightField, UPGRADE);
                    }

                } catch (final IllegalAccessException ex) {
                    throw new RuntimeException(
                            "Unexpected IllegalAccessException: " + ex.getMessage(), ex);
                }
            }
        }
    }

    private boolean accept(final Field field) {
        if (field.getName().indexOf(ClassUtils.INNER_CLASS_SEPARATOR_CHAR) != -1) {
            return false;
        }
        if (Modifier.isTransient(field.getModifiers())) {
            return false;
        }
        return !Modifier.isStatic(field.getModifiers());
    }

    private DiffResult<Map<String, String>> configDiff(
            Field field, Map<String, String> left, Map<String, String> right) {
        var keys = new HashSet<String>();
        keys.addAll(left.keySet());
        keys.addAll(right.keySet());
        var diffBuilder = new DiffBuilder<>(left, right);
        var annotation = field.getAnnotation(SpecDiff.Config.class);

        keys.forEach(
                key -> {
                    if (annotation != null) {
                        DiffType diffType = getType(annotation, key);
                        diffBuilder.append(key, left.get(key), right.get(key), diffType);
                    } else {
                        diffBuilder.append(key, left.get(key), right.get(key), UPGRADE);
                    }
                });

        return diffBuilder.build();
    }

    private DiffType getType(SpecDiff.Config annotation, String key) {
        DiffType diffType = UPGRADE;
        for (var entry : annotation.value()) {
            if (entry.mode().length > 0 && !Arrays.asList(entry.mode()).contains(deploymentMode)) {
                // This annotation does not apply to the current deploy mode
                continue;
            }

            if (key.startsWith(entry.prefix())) {
                return entry.type();
            }
        }
        return diffType;
    }

    /**
     * This method is responsible for clearing / nulling out deprecated fields that should be
     * ignored during spec diff comparison. These fields may still be present in the
     * lastReconciledSpec.
     *
     * @param o Object to be cleaned.
     */
    private static void clearIgnoredFields(Object o) {
        if (o == null) {
            return;
        }
        if (o instanceof FlinkDeploymentSpec) {
            var spec = (FlinkDeploymentSpec) o;
            clearPodTemplateAdditionalProps(spec.getPodTemplate());
            if (spec.getJobManager() != null) {
                clearPodTemplateAdditionalProps(spec.getJobManager().getPodTemplate());
            }
            if (spec.getTaskManager() != null) {
                clearPodTemplateAdditionalProps(spec.getTaskManager().getPodTemplate());
            }
        }
    }

    /**
     * Remove additional props from deserialized PodTemplateSpec which could still be there when we
     * moved Pod -> PodTemplateSpec.
     *
     * @param o Object to be cleaned.
     */
    private static void clearPodTemplateAdditionalProps(Object o) {
        if (o != null && o instanceof PodTemplateSpec) {
            ((PodTemplateSpec) o).setAdditionalProperties(null);
        }
    }
}
