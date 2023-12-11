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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.zjsonpatch.JsonDiff;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.stream.Collectors;

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
    @NonNull private final T before;
    @NonNull private final T after;
    @NonNull private final DiffType type;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    DiffResult(@NonNull T before, @NonNull T after, @NonNull List<Diff<?>> diffList) {
        this.before = before;
        this.after = after;
        this.diffList = diffList;
        this.type =
                DiffType.from(diffList.stream().map(Diff::getType).collect(Collectors.toList()));
    }

    public int getNumDiffs() {
        return diffList.size();
    }

    @Override
    public String toString() {
        if (diffList.isEmpty()) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();
        builder.append(before.getClass().getSimpleName()).append("[");

        diffList.forEach(
                diff -> {
                    try {
                        JsonNode diffBefore =
                                objectMapper.readTree(
                                        objectMapper.writeValueAsString(diff.getLeft()));
                        JsonNode diffAfter =
                                objectMapper.readTree(
                                        objectMapper.writeValueAsString(diff.getRight()));
                        JsonNode jsonDiff = JsonDiff.asJson(diffBefore, diffAfter);
                        jsonDiff.forEach(
                                row -> {
                                    addField(
                                            builder,
                                            diffBefore,
                                            diffAfter,
                                            diff.getFieldName(),
                                            row);
                                    builder.append(", ");
                                });
                        builder.setLength(builder.length() - 2);
                    } catch (Exception je) {
                        builder.append(diff.getFieldName())
                                .append(" : ")
                                .append(diff.getLeft())
                                .append(" -> ")
                                .append(diff.getRight());
                    }
                    builder.append(", ");
                });
        builder.setLength(builder.length() - 2);
        builder.append("]");
        return String.format("Diff: %s", builder);
    }

    private static void addField(
            StringBuilder sb,
            JsonNode parentBefore,
            JsonNode parentAfter,
            String fieldName,
            JsonNode diff) {
        JsonNode beforeNode = parentBefore;
        JsonNode afterNode = parentAfter;
        String extraPath = "";
        if (!diff.get("path").asText().equals("/")) {
            extraPath = diff.get("path").asText().replaceAll("/", ".");
            beforeNode = beforeNode.at(diff.get("path").asText());
            afterNode = afterNode.at(diff.get("path").asText());
        }
        sb.append(fieldName).append(extraPath).append(" : ");
        if ((afterNode.isNull() || afterNode.isMissingNode()) && beforeNode.asText().equals("")) {
            sb.append("{..}");
        } else {
            sb.append(getText(beforeNode));
        }
        sb.append(" -> ").append(getText(afterNode));
    }

    private static String getText(JsonNode node) {
        if (node.isNull() || node.isMissingNode()) {
            return null;
        }
        String text = node.asText();
        return text.equals("") ? node.toString() : text;
    }
}
