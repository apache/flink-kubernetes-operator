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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.kubernetes.operator.crd.status.Savepoint;

import lombok.Value;

/** Result of a fetch savepoint operation. */
@Value
public class SavepointFetchResult {
    private final Savepoint savepoint;
    private final boolean isTriggered;
    private final String error;

    public static SavepointFetchResult notTriggered() {
        return new SavepointFetchResult(null, false, null);
    }

    public static SavepointFetchResult error(String error) {
        return new SavepointFetchResult(null, false, error);
    }

    public static SavepointFetchResult pending() {
        return new SavepointFetchResult(null, true, null);
    }

    public static SavepointFetchResult completed(Savepoint savepoint) {
        return new SavepointFetchResult(savepoint, true, null);
    }
}
