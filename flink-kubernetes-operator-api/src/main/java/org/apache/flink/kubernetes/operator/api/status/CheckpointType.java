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

package org.apache.flink.kubernetes.operator.api.status;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** Checkpoint format type. */
public enum CheckpointType implements DescribedEnum {
    FULL("A comprehensive snapshot, saving the complete state of a data stream."),
    INCREMENTAL(
            "A more efficient, reduced snapshot, saving only the differences in state data since the last checkpoint."),
    /** Checkpoint format unknown, if the checkpoint was not triggered by the operator. */
    UNKNOWN("Only for internal purposes.");

    private final InlineElement description;

    CheckpointType(String description) {
        this.description = text(description);
    }

    @Override
    @Internal
    public InlineElement getDescription() {
        return description;
    }
}
