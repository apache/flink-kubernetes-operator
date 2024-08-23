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

package org.apache.flink.autoscaler.jdbc.event;

import org.apache.flink.autoscaler.event.AutoScalerEventHandler;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Countable {@link JdbcEventInteractor}. */
class CountableJdbcEventInteractor extends JdbcEventInteractor {

    private final AtomicLong queryCounter;
    private final AtomicLong createCounter;
    private final AtomicLong updateCounter;
    private final AtomicLong deleteExpiredCounter;

    public CountableJdbcEventInteractor(Connection conn) {
        super(conn);
        queryCounter = new AtomicLong();
        createCounter = new AtomicLong();
        updateCounter = new AtomicLong();
        deleteExpiredCounter = new AtomicLong();
    }

    @Override
    public Optional<AutoScalerEvent> queryLatestEvent(String jobKey, String reason, String eventKey)
            throws Exception {
        queryCounter.incrementAndGet();
        return super.queryLatestEvent(jobKey, reason, eventKey);
    }

    @Override
    public void createEvent(
            String jobKey,
            String reason,
            AutoScalerEventHandler.Type type,
            String message,
            String eventKey)
            throws Exception {
        createCounter.incrementAndGet();
        super.createEvent(jobKey, reason, type, message, eventKey);
    }

    @Override
    public void updateEvent(long id, String message, int eventCount) throws Exception {
        updateCounter.incrementAndGet();
        super.updateEvent(id, message, eventCount);
    }

    @Override
    int deleteExpiredEventsByIdRangeAndDate(long startId, long endId, Timestamp timestamp)
            throws Exception {
        deleteExpiredCounter.incrementAndGet();
        return super.deleteExpiredEventsByIdRangeAndDate(startId, endId, timestamp);
    }

    public void assertCounters(
            long expectedQueryCounter, long expectedUpdateCounter, long expectedCreateCounter) {
        assertThat(queryCounter).hasValue(expectedQueryCounter);
        assertThat(updateCounter).hasValue(expectedUpdateCounter);
        assertThat(createCounter).hasValue(expectedCreateCounter);
    }

    public void assertDeleteExpiredCounter(long expectedCounter) {
        assertThat(deleteExpiredCounter).hasValue(expectedCounter);
    }
}
