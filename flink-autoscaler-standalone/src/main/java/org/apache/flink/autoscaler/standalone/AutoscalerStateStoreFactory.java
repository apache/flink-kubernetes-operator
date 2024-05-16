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
import org.apache.flink.autoscaler.jdbc.state.JdbcAutoScalerStateStore;
import org.apache.flink.autoscaler.jdbc.state.JdbcStateInteractor;
import org.apache.flink.autoscaler.jdbc.state.JdbcStateStore;
import org.apache.flink.autoscaler.standalone.utils.HikariJDBCUtil;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.autoscaler.standalone.AutoscalerStateStoreFactory.StateStoreType.JDBC;
import static org.apache.flink.autoscaler.standalone.AutoscalerStateStoreFactory.StateStoreType.MEMORY;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.STATE_STORE_TYPE;
import static org.apache.flink.configuration.description.TextElement.text;

/** The factory of {@link AutoScalerStateStore}. */
public class AutoscalerStateStoreFactory {

    /** Out-of-box state store type. */
    public enum StateStoreType implements DescribedEnum {
        MEMORY(
                "The state store based on the Java Heap, the state will be discarded after process restarts."),
        JDBC(
                "The state store which persists its state in JDBC related database. It's recommended in production.");

        private final InlineElement description;

        StateStoreType(String description) {
            this.description = text(description);
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    public static <KEY, Context extends JobAutoScalerContext<KEY>>
            AutoScalerStateStore<KEY, Context> create(Configuration conf) throws Exception {
        var stateStoreType = conf.get(STATE_STORE_TYPE);
        switch (stateStoreType) {
            case MEMORY:
                return new InMemoryAutoScalerStateStore<>();
            case JDBC:
                return createJdbcStateStore(conf);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown state store type : %s. Optional state store types are: %s and %s.",
                                stateStoreType, MEMORY, JDBC));
        }
    }

    private static <KEY, Context extends JobAutoScalerContext<KEY>>
            AutoScalerStateStore<KEY, Context> createJdbcStateStore(Configuration conf)
                    throws Exception {
        var conn = HikariJDBCUtil.getConnection(conf);
        return new JdbcAutoScalerStateStore<>(new JdbcStateStore(new JdbcStateInteractor(conn)));
    }
}
