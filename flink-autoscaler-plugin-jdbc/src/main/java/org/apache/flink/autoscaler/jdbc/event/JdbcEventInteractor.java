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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** Responsible for interacting with the database. */
public class JdbcEventInteractor {

    private final Connection conn;
    private Clock clock = Clock.systemDefaultZone();

    public JdbcEventInteractor(Connection conn) {
        this.conn = conn;
    }

    public Optional<AutoScalerEvent> queryLatestEvent(String jobKey, String reason, String eventKey)
            throws Exception {
        var query =
                "select * from t_flink_autoscaler_event_handler "
                        + "where job_key = ? and reason = ? and event_key = ? ";

        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            pstmt.setString(2, reason);
            pstmt.setString(3, eventKey);

            var rs = pstmt.executeQuery();
            // A better approach of finding the latestEvent is sql query desc the id and limit 1,
            // but the limit syntax is different for different databases.
            AutoScalerEvent latestEvent = null;
            while (rs.next()) {
                var currentEvent = generateEvent(rs);
                if (latestEvent == null || latestEvent.getId() < currentEvent.getId()) {
                    // If the current event is newer than the latestEvent, then update the
                    // latestEvent.
                    latestEvent = currentEvent;
                }
            }
            return Optional.ofNullable(latestEvent);
        }
    }

    private AutoScalerEvent generateEvent(ResultSet rs) throws SQLException {
        return new AutoScalerEvent(
                rs.getLong("id"),
                rs.getTimestamp("create_time").toInstant(),
                rs.getTimestamp("update_time").toInstant(),
                rs.getString("job_key"),
                rs.getString("reason"),
                rs.getString("event_type"),
                rs.getString("message"),
                rs.getInt("event_count"),
                rs.getString("event_key"));
    }

    public void createEvent(
            String jobKey,
            String reason,
            AutoScalerEventHandler.Type type,
            String message,
            String eventKey)
            throws Exception {
        var query =
                "INSERT INTO t_flink_autoscaler_event_handler ("
                        + "create_time, update_time, job_key, reason, event_type, message, event_count, event_key)"
                        + " values (?, ?, ?, ?, ?, ?, ?, ?)";

        var createTime = Timestamp.from(clock.instant());
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setTimestamp(1, createTime);
            pstmt.setTimestamp(2, createTime);
            pstmt.setString(3, jobKey);
            pstmt.setString(4, reason);
            pstmt.setString(5, type.toString());
            pstmt.setString(6, message);
            pstmt.setInt(7, 1);
            pstmt.setString(8, eventKey);
            pstmt.executeUpdate();
        }
    }

    public void updateEvent(long id, String message, int eventCount) throws Exception {
        var query =
                "UPDATE t_flink_autoscaler_event_handler set update_time = ?, message = ?, event_count = ? where id = ?";

        var updateTime = Timestamp.from(clock.instant());
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setTimestamp(1, updateTime);
            pstmt.setString(2, message);
            pstmt.setInt(3, eventCount);
            pstmt.setLong(4, id);
            checkState(pstmt.executeUpdate() == 1, "Update event id=[%s] fails.", id);
        }
    }

    public Instant getCurrentInstant() {
        return clock.instant();
    }

    @VisibleForTesting
    protected List<AutoScalerEvent> queryEvents(String jobKey, String reason) throws Exception {
        var query =
                "select * from t_flink_autoscaler_event_handler "
                        + "where job_key = ? and reason = ? ";

        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            pstmt.setString(2, reason);

            var rs = pstmt.executeQuery();
            var events = new ArrayList<AutoScalerEvent>();
            while (rs.next()) {
                events.add(generateEvent(rs));
            }
            return events;
        }
    }

    @VisibleForTesting
    void setClock(@Nonnull Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }

    @Nullable
    Long queryMinEventIdByCreateTime(Timestamp timestamp) throws Exception {
        var sql =
                "SELECT id from t_flink_autoscaler_event_handler "
                        + "           where id = (SELECT id FROM t_flink_autoscaler_event_handler order by id asc limit 1) "
                        + "           and create_time < ?";
        try (var pstmt = conn.prepareStatement(sql)) {
            pstmt.setObject(1, timestamp);
            ResultSet resultSet = pstmt.executeQuery();
            return resultSet.next() ? resultSet.getLong(1) : null;
        }
    }

    int deleteExpiredEventsByIdRangeAndDate(long startId, long endId, Timestamp timestamp)
            throws Exception {
        var query =
                "delete from t_flink_autoscaler_event_handler where id >= ? and id < ? and create_time < ?";
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setObject(1, startId);
            pstmt.setObject(2, endId);
            pstmt.setObject(3, timestamp);
            return pstmt.executeUpdate();
        }
    }
}
