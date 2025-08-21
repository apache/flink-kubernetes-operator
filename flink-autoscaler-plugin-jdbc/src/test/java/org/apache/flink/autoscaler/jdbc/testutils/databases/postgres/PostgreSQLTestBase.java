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

package org.apache.flink.autoscaler.jdbc.testutils.databases.postgres;

import org.apache.flink.autoscaler.jdbc.testutils.databases.DatabaseTest;

import org.junit.jupiter.api.extension.RegisterExtension;

import javax.sql.DataSource;

/** PostgreSQL database for testing. */
public interface PostgreSQLTestBase extends DatabaseTest {

    @RegisterExtension PostgreSQLExtension POSTGRE_SQL_EXTENSION = new PostgreSQLExtension("15.1");

    default DataSource getDataSource() throws Exception {
        return POSTGRE_SQL_EXTENSION.getDataSource();
    }
}
