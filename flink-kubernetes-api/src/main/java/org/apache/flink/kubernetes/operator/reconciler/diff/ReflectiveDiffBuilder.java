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

import lombok.NonNull;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.Builder;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.flink.kubernetes.operator.reconciler.diff.DiffType.UPGRADE;

/**
 * Assists in implementing {@link Diffable#diff(Object)} methods with reflection.
 *
 * <p>Inspired by:
 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/builder/ReflectionDiffBuilder.java
 */
@Experimental
public class ReflectiveDiffBuilder<T> implements Builder<DiffResult<T>> {

    private final Object left;
    private final Object right;
    private final DiffBuilder<T> diffBuilder;

    public ReflectiveDiffBuilder(@NonNull final T lhs, @NonNull final T rhs) {
        this.left = lhs;
        this.right = rhs;
        diffBuilder = new DiffBuilder<>(lhs, rhs);
    }

    @Override
    public DiffResult<T> build() {
        if (left.equals(right)) {
            return diffBuilder.build();
        }

        appendFields(left.getClass());
        return diffBuilder.build();
    }

    private void appendFields(final Class<?> clazz) {
        for (final Field field : FieldUtils.getAllFields(clazz)) {
            if (accept(field)) {
                try {
                    var leftField = readField(field, left, true);
                    var rightField = readField(field, right, true);
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
                        diffBuilder.append(
                                field.getName(), leftField, rightField, annotation.value());
                    } else if (Diffable.class.isAssignableFrom(field.getType())
                            && ObjectUtils.allNotNull(leftField, rightField)) {

                        diffBuilder.append(
                                field.getName(), ((Diffable<T>) leftField).diff((T) rightField));
                    } else {
                        diffBuilder.append(
                                field.getName(),
                                readField(field, left, true),
                                readField(field, right, true),
                                UPGRADE);
                    }

                } catch (final IllegalAccessException ex) {
                    throw new RuntimeException(
                            "Unexpected IllegalAccessException: " + ex.getMessage());
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

    private static DiffResult<Map<String, String>> configDiff(
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

    private static DiffType getType(SpecDiff.Config annotation, String key) {
        DiffType diffType = UPGRADE;
        for (var entry : annotation.value()) {
            if (key.startsWith(entry.prefix())) {
                return entry.type();
            }
        }
        return diffType;
    }
}
