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

import org.apache.flink.annotation.Experimental;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;

/** Represents information about a finished checkpoint. */
@Experimental
@Data
@NoArgsConstructor
public class Checkpoint implements Snapshot {
    /** Millisecond timestamp at the start of the checkpoint operation. */
    private long timeStamp;

    /** Checkpoint trigger mechanism. */
    private SnapshotTriggerType triggerType = SnapshotTriggerType.UNKNOWN;

    /** Checkpoint format. */
    private CheckpointType formatType = CheckpointType.UNKNOWN;

    /**
     * Nonce value used when the checkpoint was triggered manually {@link
     * SnapshotTriggerType#MANUAL}, null for other types of checkpoint.
     */
    private Long triggerNonce;

    public Checkpoint(
            long timeStamp,
            @Nullable SnapshotTriggerType triggerType,
            @Nullable CheckpointType formatType,
            @Nullable Long triggerNonce) {
        this.timeStamp = timeStamp;
        if (triggerType != null) {
            this.triggerType = triggerType;
        }
        if (formatType != null) {
            this.formatType = formatType;
        }
        this.triggerNonce = triggerNonce;
    }

    public static Checkpoint of(long timeStamp, SnapshotTriggerType triggerType) {
        return new Checkpoint(timeStamp, triggerType, CheckpointType.UNKNOWN, null);
    }

    public static Checkpoint of(SnapshotTriggerType triggerType) {
        return new Checkpoint(
                System.currentTimeMillis(), triggerType, CheckpointType.UNKNOWN, null);
    }

    public static Checkpoint of(SnapshotTriggerType triggerType, CheckpointType formatType) {
        return new Checkpoint(System.currentTimeMillis(), triggerType, formatType, null);
    }
}
