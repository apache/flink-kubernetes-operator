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

import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

/**
 * Contains a collection of the differences between two {@link Diffable} objects.
 *
 * <p>Inspired by:
 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/builder/DiffResult.java
 */
@Experimental
@Getter
public class DiffResult<T> {
    @NonNull private final List<Diff<?>> diffList;
    @NonNull private final T left;
    @NonNull private final T right;
    @NonNull private final DiffType type;

    DiffResult(@NonNull T left, @NonNull T right, @NonNull List<Diff<?>> diffList) {
        this.left = left;
        this.right = right;
        this.diffList = diffList;
        this.type = getSpechChangeType(diffList);
    }

    public int getNumDiffs() {
        return diffList.size();
    }

    @Override
    public String toString() {
        if (diffList.isEmpty()) {
            return "";
        }

        final ToStringBuilder lhsBuilder =
                new ToStringBuilder(left, ToStringStyle.SHORT_PREFIX_STYLE);
        final ToStringBuilder rhsBuilder =
                new ToStringBuilder(right, ToStringStyle.SHORT_PREFIX_STYLE);

        diffList.forEach(
                diff -> {
                    lhsBuilder.append(diff.getFieldName(), diff.getLeft());
                    rhsBuilder.append(diff.getFieldName(), diff.getRight());
                });

        return String.format("%s differs from %s", lhsBuilder.build(), rhsBuilder.build());
    }

    private static DiffType getSpechChangeType(List<Diff<?>> diffs) {
        var type = DiffType.IGNORE;
        for (var diff : diffs) {
            type = DiffType.max(type, diff.getType());
            if (type == DiffType.UPGRADE) {
                return type;
            }
        }
        return type;
    }
}
