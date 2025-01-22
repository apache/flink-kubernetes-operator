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
import org.apache.flink.autoscaler.jdbc.testutils.databases.DatabaseTest;
import org.apache.flink.autoscaler.jdbc.testutils.databases.derby.DerbyTestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL56TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL57TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL8TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.postgres.PostgreSQLTestBase;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** The abstract IT case for {@link JdbcEventInteractor}. */
abstract class AbstractJdbcEventInteractorITCase implements DatabaseTest {

    @Test
    void testAllOperations() throws Exception {
        var jobKey = "jobKey";
        var reason = "ScalingReport";
        var message = "Expected event message.";
        var eventKey = Integer.toString(34567);

        // The datetime precision is seconds in MySQL by default.
        var createTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        try (var jdbcEventInteractor = new JdbcEventInteractor(getDataSource())) {
            jdbcEventInteractor.setClock(Clock.fixed(createTime, ZoneId.systemDefault()));

            jdbcEventInteractor.createEvent(
                    jobKey, reason, AutoScalerEventHandler.Type.Normal, message, eventKey);
            var firstEventOptional = jdbcEventInteractor.queryLatestEvent(jobKey, reason, eventKey);
            assertThat(firstEventOptional).isPresent();
            assertEvent(
                    firstEventOptional.get(),
                    createTime,
                    createTime,
                    jobKey,
                    reason,
                    message,
                    1,
                    eventKey);

            // The create time is changed for the second event.
            var secondCreateTime = createTime.plusSeconds(5);
            jdbcEventInteractor.setClock(Clock.fixed(secondCreateTime, ZoneId.systemDefault()));
            jdbcEventInteractor.createEvent(
                    jobKey, reason, AutoScalerEventHandler.Type.Normal, message + 2, eventKey);
            // The latest event should be the second event.
            var secondEventOptional =
                    jdbcEventInteractor.queryLatestEvent(jobKey, reason, eventKey);
            assertThat(secondEventOptional).isPresent();
            var secondEvent = secondEventOptional.get();
            assertEvent(
                    secondEvent,
                    secondCreateTime,
                    secondCreateTime,
                    jobKey,
                    reason,
                    message + 2,
                    1,
                    eventKey);

            // Update event
            var updateTime = secondCreateTime.plusSeconds(3);
            jdbcEventInteractor.setClock(Clock.fixed(updateTime, ZoneId.systemDefault()));
            jdbcEventInteractor.updateEvent(
                    secondEvent.getId(), secondEvent.getMessage() + 3, secondEvent.getCount() + 1);

            var updatedEventOptional =
                    jdbcEventInteractor.queryLatestEvent(jobKey, reason, eventKey);
            assertThat(updatedEventOptional).isPresent();
            var updatedEvent = updatedEventOptional.get();
            assertEvent(
                    updatedEvent,
                    secondCreateTime,
                    updateTime,
                    jobKey,
                    reason,
                    secondEvent.getMessage() + 3,
                    2,
                    eventKey);
        }
    }

    private void assertEvent(
            AutoScalerEvent event,
            Instant expectedCreateTime,
            Instant expectedUpdateTime,
            String expectedJobKey,
            String expectedReason,
            String expectedMessage,
            int expectedCount,
            String expectedEventKey) {
        assertThat(event.getCreateTime()).isEqualTo(expectedCreateTime);
        assertThat(event.getUpdateTime()).isEqualTo(expectedUpdateTime);
        assertThat(event.getJobKey()).isEqualTo(expectedJobKey);
        assertThat(event.getReason()).isEqualTo(expectedReason);
        assertThat(event.getEventType()).isEqualTo(AutoScalerEventHandler.Type.Normal.toString());
        assertThat(event.getMessage()).isEqualTo(expectedMessage);
        assertThat(event.getCount()).isEqualTo(expectedCount);
        assertThat(event.getEventKey()).isEqualTo(expectedEventKey);
    }
}

/** Test {@link JdbcEventInteractor} via Derby. */
class DerbyJdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements DerbyTestBase {}

/** Test {@link JdbcEventInteractor} via MySQL 5.6.x. */
class MySQL56JdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements MySQL56TestBase {}

/** Test {@link JdbcEventInteractor} via MySQL 5.7.x. */
class MySQL57JdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements MySQL57TestBase {}

/** Test {@link JdbcEventInteractor} via MySQL 8.x. */
class MySQL8JdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements MySQL8TestBase {}

/** Test {@link JdbcEventInteractor} via Postgre SQL. */
class PostgreSQLJdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements PostgreSQLTestBase {}
