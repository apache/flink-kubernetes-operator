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

package org.apache.flink.autoscaler.standalone;

import org.apache.flink.autoscaler.jdbc.state.JdbcAutoScalerStateStore;
import org.apache.flink.autoscaler.standalone.utils.HikariJDBCUtil;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.apache.flink.autoscaler.standalone.AutoscalerStateStoreFactory.StateStoreType.JDBC;
import static org.apache.flink.autoscaler.standalone.AutoscalerStateStoreFactory.StateStoreType.MEMORY;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.JDBC_URL;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.STATE_STORE_TYPE;
import static org.apache.flink.autoscaler.standalone.utils.HikariJDBCUtil.JDBC_URL_REQUIRED_HINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AutoscalerStateStoreFactoryTest {

    @Test
    void testCreateDefaultStateStore() throws Exception {
        // Test for memory state store is created by default.
        var stateStore = AutoscalerStateStoreFactory.create(new Configuration());
        assertThat(stateStore).isInstanceOf(InMemoryAutoScalerStateStore.class);
    }

    @Test
    void testCreateInMemoryStateStore() throws Exception {
        // Test for memory state store is created explicitly.
        final var conf = new Configuration();
        conf.set(STATE_STORE_TYPE, MEMORY);
        var stateStore = AutoscalerStateStoreFactory.create(conf);
        assertThat(stateStore).isInstanceOf(InMemoryAutoScalerStateStore.class);
    }

    @Test
    void testCreateJdbcStateStoreWithoutURL() {
        // Test for missing the jdbc url.
        final var conf = new Configuration();
        conf.set(STATE_STORE_TYPE, JDBC);
        assertThatThrownBy(() -> AutoscalerStateStoreFactory.create(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(JDBC_URL_REQUIRED_HINT);
    }

    @Test
    void testCreateJdbcStateStore() throws Exception {
        final var jdbcUrl = "jdbc:derby:memory:test";
        // Test for create JDBC State store.
        final var conf = new Configuration();
        conf.set(STATE_STORE_TYPE, JDBC);
        conf.set(JDBC_URL, String.format("%s;create=true", jdbcUrl));
        HikariJDBCUtil.getConnection(conf).close();

        var stateStore = AutoscalerStateStoreFactory.create(conf);
        assertThat(stateStore).isInstanceOf(JdbcAutoScalerStateStore.class);

        try {
            conf.set(JDBC_URL, String.format("%s;shutdown=true", jdbcUrl));
            HikariJDBCUtil.getConnection(conf).close();
        } catch (RuntimeException ignored) {
            // database shutdown ignored exception
        }
    }
}
