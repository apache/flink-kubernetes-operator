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

package org.apache.flink.autoscaler.jdbc.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Responsible for interacting with the database. */
public class JdbcStateInteractor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcStateInteractor.class);

    private final DataSource dataSource;

    public JdbcStateInteractor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Map<StateType, String> queryData(String jobKey) throws Exception {
        var query =
                "select state_type, state_value from t_flink_autoscaler_state_store where job_key = ?";
        var data = new HashMap<StateType, String>();
        try (var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            var rs = pstmt.executeQuery();
            while (rs.next()) {
                var stateTypeIdentifier = rs.getString("state_type");
                var stateType = StateType.createFromIdentifier(stateTypeIdentifier);
                var stateValue = rs.getString("state_value");
                data.put(stateType, stateValue);
            }
        }
        return data;
    }

    public void deleteData(String jobKey, List<StateType> deletedStateTypes) throws Exception {
        var query =
                String.format(
                        "DELETE FROM t_flink_autoscaler_state_store where job_key = ? and state_type in (%s)",
                        String.join(",", Collections.nCopies(deletedStateTypes.size(), "?")));
        try (var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            int i = 2;
            for (var stateType : deletedStateTypes) {
                pstmt.setString(i++, stateType.getIdentifier());
            }
            pstmt.execute();
        }
        LOG.info("Delete jobKey: {} stateTypes: {} from database.", jobKey, deletedStateTypes);
    }

    public void createData(
            String jobKey, List<StateType> createdStateTypes, Map<StateType, String> data)
            throws Exception {
        var query =
                "INSERT INTO t_flink_autoscaler_state_store (update_time, job_key, state_type, state_value) values (?, ?, ?, ?)";
        var updateTime = Timestamp.from(Instant.now());
        try (var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement(query)) {
            for (var stateType : createdStateTypes) {
                pstmt.setTimestamp(1, updateTime);
                pstmt.setString(2, jobKey);
                pstmt.setString(3, stateType.getIdentifier());

                String stateValue = data.get(stateType);
                checkState(
                        stateValue != null,
                        "The state value shouldn't be null during inserting. "
                                + "It may be a bug, please raise a JIRA to Flink Community.");
                pstmt.setString(4, stateValue);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        LOG.info("Insert jobKey: {} stateTypes: {} from database.", jobKey, createdStateTypes);
    }

    public void updateData(
            String jobKey, List<StateType> updatedStateTypes, Map<StateType, String> data)
            throws Exception {
        var query =
                "UPDATE t_flink_autoscaler_state_store set update_time = ?, state_value = ? where job_key = ? and state_type = ?";

        var updateTime = Timestamp.from(Instant.now());
        try (var conn = dataSource.getConnection();
                var pstmt = conn.prepareStatement(query)) {
            for (var stateType : updatedStateTypes) {
                pstmt.setTimestamp(1, updateTime);

                String stateValue = data.get(stateType);
                checkState(
                        stateValue != null,
                        "The state value shouldn't be null during inserting. "
                                + "It may be a bug, please raise a JIRA to Flink Community.");
                pstmt.setString(2, stateValue);
                pstmt.setString(3, jobKey);
                pstmt.setString(4, stateType.getIdentifier());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    @Override
    public void close() throws Exception {
        if (dataSource instanceof AutoCloseable) {
            ((AutoCloseable) dataSource).close();
        }
    }
}
