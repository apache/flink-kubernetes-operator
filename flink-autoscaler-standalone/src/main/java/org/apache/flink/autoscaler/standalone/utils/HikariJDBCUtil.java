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

package org.apache.flink.autoscaler.standalone.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.StringUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.JDBC_PASSWORD_ENV_VARIABLE;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.JDBC_URL;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.JDBC_USERNAME;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Hikari JDBC common util. */
public class HikariJDBCUtil {

    public static final String JDBC_URL_REQUIRED_HINT =
            String.format(
                    "%s is required when jdbc state store or jdbc event handler is used.",
                    JDBC_URL.key());

    public static Connection getConnection(Configuration conf) throws SQLException {
        final var jdbcUrl = conf.get(JDBC_URL);
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(jdbcUrl), JDBC_URL_REQUIRED_HINT);
        var user = conf.get(JDBC_USERNAME);
        var password = System.getenv().get(conf.get(JDBC_PASSWORD_ENV_VARIABLE));
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);
        return new HikariDataSource(hikariConfig).getConnection();
    }
}
