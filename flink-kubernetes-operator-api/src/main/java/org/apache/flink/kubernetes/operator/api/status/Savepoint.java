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

/** Represents information about a finished savepoint. */
@Experimental
@Data
@NoArgsConstructor
public class Savepoint {
    /** Millisecond timestamp at the start of the savepoint operation. */
    private long timeStamp;

    /** External pointer of the savepoint can be used to recover jobs. */
    private String location;

    /** Savepoint trigger mechanism. */
    private SavepointTriggerType triggerType = SavepointTriggerType.UNKNOWN;

    private SavepointFormatType formatType = SavepointFormatType.UNKNOWN;

    /**
     * Nonce value used when the savepoint was triggered manually {@link
     * SavepointTriggerType#MANUAL}, defaults to 0.
     */
    private Long triggerNonce = 0L;

    public Savepoint(
            long timeStamp,
            String location,
            SavepointTriggerType triggerType,
            SavepointFormatType formatType,
            Long triggerNonce) {
        this.timeStamp = timeStamp;
        this.location = location;
        this.triggerType = triggerType;
        this.formatType = formatType;
        setTriggerNonce(triggerNonce);
    }

    public static Savepoint of(String location, SavepointTriggerType triggerType) {
        return new Savepoint(
                System.currentTimeMillis(), location, triggerType, SavepointFormatType.UNKNOWN, 0L);
    }

    public static Savepoint of(
            String location, SavepointTriggerType triggerType, SavepointFormatType formatType) {
        return new Savepoint(System.currentTimeMillis(), location, triggerType, formatType, 0L);
    }

    public void setTriggerNonce(Long triggerNonce) {
        if (triggerNonce == null) {
            this.triggerNonce = 0L;
        } else {
            this.triggerNonce = triggerNonce;
        }
    }
}
