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

package org.apache.flink.kubernetes.operator.reconciler.diff;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.diff.Diffable;

import lombok.NonNull;
import org.apache.commons.lang3.builder.Builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Assists in implementing comparison of {@link Diffable} instances.
 *
 * <p>Inspired by:
 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/builder/DiffBuilder.java
 */
@Experimental
public class DiffBuilder<T> implements Builder<DiffResult<?>> {

    private static final String DELIMITER = ".";

    private final T before;
    private final T after;

    private final List<Diff<?>> diffs;
    private boolean triviallyEqual;

    public DiffBuilder(@NonNull final T before, @NonNull final T after) {
        this.diffs = new ArrayList<>();
        this.before = before;
        this.after = after;
        this.triviallyEqual = before == after || before.equals(after);
    }

    public <S> DiffBuilder<T> append(
            @NonNull final String fieldName,
            S left,
            S right,
            BiFunction<S, S, Boolean> equals,
            DiffType type) {
        if (triviallyEqual) {
            return this;
        }
        if (!equals.apply(left, right)) {
            diffs.add(new Diff<>(fieldName, left, right, type));
        }
        return this;
    }

    public DiffBuilder<T> append(
            @NonNull final String fieldName, final Object left, final Object right, DiffType type) {

        if (triviallyEqual) {
            return this;
        }
        if (left == right) {
            return this;
        }

        final Object objectToTest = Objects.requireNonNullElse(left, right);

        if (objectToTest.getClass().isArray()) {
            if (objectToTest instanceof boolean[]) {
                return append(fieldName, (boolean[]) left, (boolean[]) right, Arrays::equals, type);
            }
            if (objectToTest instanceof byte[]) {
                return append(fieldName, (byte[]) left, (byte[]) right, Arrays::equals, type);
            }
            if (objectToTest instanceof char[]) {
                return append(fieldName, (char[]) left, (char[]) right, Arrays::equals, type);
            }
            if (objectToTest instanceof double[]) {
                return append(fieldName, (double[]) left, (double[]) right, Arrays::equals, type);
            }
            if (objectToTest instanceof float[]) {
                return append(fieldName, (float[]) left, (float[]) right, Arrays::equals, type);
            }
            if (objectToTest instanceof int[]) {
                return append(fieldName, (int[]) left, (int[]) right, Arrays::equals, type);
            }
            if (objectToTest instanceof long[]) {
                return append(fieldName, (long[]) left, (long[]) right, Arrays::equals, type);
            }
            if (objectToTest instanceof short[]) {
                return append(fieldName, (short[]) left, (short[]) right, Arrays::equals, type);
            }

            return append(fieldName, (Object[]) left, (Object[]) right, Arrays::equals, type);
        }

        if (left != null && left.equals(right)) {
            return this;
        }

        diffs.add(new Diff<>(fieldName, left, right, type));

        return this;
    }

    public DiffBuilder<T> append(
            @NonNull final String fieldName,
            final Object[] left,
            final Object[] right,
            DiffType type) {

        if (triviallyEqual) {
            return this;
        }

        if (!Arrays.equals(left, right)) {
            diffs.add(new Diff<>(fieldName, left, right, type));
        }

        return this;
    }

    public DiffBuilder<T> append(
            @NonNull final String fieldName, @NonNull final DiffResult<?> diffResult) {
        if (triviallyEqual) {
            return this;
        }
        diffResult
                .getDiffList()
                .forEach(
                        diff ->
                                append(
                                        fieldName + DELIMITER + diff.getFieldName(),
                                        diff.getLeft(),
                                        diff.getRight(),
                                        diff.getType()));
        return this;
    }

    @Override
    public DiffResult<T> build() {
        return new DiffResult<>(before, after, diffs);
    }
}
