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

import javax.sql.DataSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Countable {@link JdbcStateInteractor}. */
public class CountableJdbcStateInteractor extends JdbcStateInteractor {

    private final AtomicLong queryCounter;
    private final AtomicLong deleteCounter;
    private final AtomicLong createCounter;
    private final AtomicLong updateCounter;

    public CountableJdbcStateInteractor(DataSource dataSource) {
        super(dataSource);
        queryCounter = new AtomicLong();
        deleteCounter = new AtomicLong();
        createCounter = new AtomicLong();
        updateCounter = new AtomicLong();
    }

    @Override
    public Map<StateType, String> queryData(String jobKey) throws Exception {
        queryCounter.incrementAndGet();
        return super.queryData(jobKey);
    }

    @Override
    public void deleteData(String jobKey, List<StateType> deletedStateTypes) throws Exception {
        deleteCounter.incrementAndGet();
        super.deleteData(jobKey, deletedStateTypes);
    }

    @Override
    public void createData(
            String jobKey, List<StateType> createdStateTypes, Map<StateType, String> data)
            throws Exception {
        createCounter.incrementAndGet();
        super.createData(jobKey, createdStateTypes, data);
    }

    @Override
    public void updateData(
            String jobKey, List<StateType> updatedStateTypes, Map<StateType, String> data)
            throws Exception {
        updateCounter.incrementAndGet();
        super.updateData(jobKey, updatedStateTypes, data);
    }

    public void assertCountableJdbcInteractor(
            long expectedQueryCounter,
            long expectedDeleteCounter,
            long expectedUpdateCounter,
            long expectedCreateCounter) {
        assertThat(queryCounter).hasValue(expectedQueryCounter);
        assertThat(deleteCounter).hasValue(expectedDeleteCounter);
        assertThat(updateCounter).hasValue(expectedUpdateCounter);
        assertThat(createCounter).hasValue(expectedCreateCounter);
    }
}
