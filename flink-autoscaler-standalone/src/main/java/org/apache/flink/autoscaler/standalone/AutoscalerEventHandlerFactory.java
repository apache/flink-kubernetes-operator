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

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.event.LoggingEventHandler;
import org.apache.flink.autoscaler.jdbc.event.JdbcAutoScalerEventHandler;
import org.apache.flink.autoscaler.jdbc.event.JdbcEventInteractor;
import org.apache.flink.autoscaler.standalone.utils.HikariJDBCUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.autoscaler.standalone.AutoscalerEventHandlerFactory.EventHandlerType.JDBC;
import static org.apache.flink.autoscaler.standalone.AutoscalerEventHandlerFactory.EventHandlerType.LOGGING;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.EVENT_HANDLER_TYPE;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.JDBC_EVENT_HANDLER_TTL;
import static org.apache.flink.configuration.description.TextElement.text;

/** The factory of {@link AutoScalerEventHandler}. */
public class AutoscalerEventHandlerFactory {

    /** Out-of-box event handler type. */
    public enum EventHandlerType implements DescribedEnum {
        LOGGING("The event handler based on logging."),
        JDBC(
                "The event handler which persists all events in JDBC related database. It's recommended in production.");

        private final InlineElement description;

        EventHandlerType(String description) {
            this.description = text(description);
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    public static <KEY, Context extends JobAutoScalerContext<KEY>>
            AutoScalerEventHandler<KEY, Context> create(Configuration conf) throws Exception {
        var eventHandlerType = conf.get(EVENT_HANDLER_TYPE);
        switch (eventHandlerType) {
            case LOGGING:
                return new LoggingEventHandler<>();
            case JDBC:
                return createJdbcEventHandler(conf);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown event handler type : %s. Optional event handlers are: %s and %s.",
                                eventHandlerType, LOGGING, JDBC));
        }
    }

    private static <KEY, Context extends JobAutoScalerContext<KEY>>
            AutoScalerEventHandler<KEY, Context> createJdbcEventHandler(Configuration conf)
                    throws Exception {
        var conn = HikariJDBCUtil.getConnection(conf);
        return new JdbcAutoScalerEventHandler<>(
                new JdbcEventInteractor(conn), conf.get(JDBC_EVENT_HANDLER_TTL));
    }
}
