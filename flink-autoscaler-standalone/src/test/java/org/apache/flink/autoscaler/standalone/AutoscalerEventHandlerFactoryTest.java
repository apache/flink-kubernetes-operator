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

import org.apache.flink.autoscaler.event.LoggingEventHandler;
import org.apache.flink.autoscaler.jdbc.event.JdbcAutoScalerEventHandler;
import org.apache.flink.autoscaler.standalone.utils.HikariJDBCUtil;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.apache.flink.autoscaler.standalone.AutoscalerEventHandlerFactory.EventHandlerType.JDBC;
import static org.apache.flink.autoscaler.standalone.AutoscalerEventHandlerFactory.EventHandlerType.LOGGING;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.EVENT_HANDLER_TYPE;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.JDBC_URL;
import static org.apache.flink.autoscaler.standalone.utils.HikariJDBCUtil.JDBC_URL_REQUIRED_HINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AutoscalerEventHandlerFactory}. */
class AutoscalerEventHandlerFactoryTest {

    @Test
    void testCreateDefaultEventHandler() throws Exception {
        // Test for logging event handler is created by default.
        var eventHandler = AutoscalerEventHandlerFactory.create(new Configuration());
        assertThat(eventHandler).isInstanceOf(LoggingEventHandler.class);
    }

    @Test
    void testCreateInMemoryEventHandler() throws Exception {
        // Test for logging event handler is created explicitly.
        final var conf = new Configuration();
        conf.set(EVENT_HANDLER_TYPE, LOGGING);
        var eventHandler = AutoscalerEventHandlerFactory.create(conf);
        assertThat(eventHandler).isInstanceOf(LoggingEventHandler.class);
    }

    @Test
    void testCreateJdbcEventHandlerWithoutURL() {
        // Test for missing the jdbc url.
        final var conf = new Configuration();
        conf.set(EVENT_HANDLER_TYPE, JDBC);
        assertThatThrownBy(() -> AutoscalerEventHandlerFactory.create(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(JDBC_URL_REQUIRED_HINT);
    }

    @Test
    void testCreateJdbcEventHandler() throws Exception {
        final var jdbcUrl = "jdbc:derby:memory:test";
        // Test for create JDBC Event Handler.
        final var conf = new Configuration();
        conf.set(EVENT_HANDLER_TYPE, JDBC);
        conf.set(JDBC_URL, String.format("%s;create=true", jdbcUrl));
        HikariJDBCUtil.getConnection(conf).close();

        var eventHandler = AutoscalerEventHandlerFactory.create(conf);
        assertThat(eventHandler).isInstanceOf(JdbcAutoScalerEventHandler.class);

        try {
            conf.set(JDBC_URL, String.format("%s;shutdown=true", jdbcUrl));
            HikariJDBCUtil.getConnection(conf).close();
        } catch (RuntimeException ignored) {
            // database shutdown ignored exception
        }
    }
}
