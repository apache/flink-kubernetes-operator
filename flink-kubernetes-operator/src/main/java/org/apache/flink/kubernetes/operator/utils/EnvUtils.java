/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.GlobalConfiguration;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.runtime.util.EnvironmentInformation.UNKNOWN;
import static org.apache.flink.runtime.util.EnvironmentInformation.UNKNOWN_COMMIT_ID_ABBREV;
import static org.apache.flink.runtime.util.EnvironmentInformation.getJvmStartupOptionsArray;
import static org.apache.flink.runtime.util.EnvironmentInformation.getJvmVersion;
import static org.apache.flink.runtime.util.EnvironmentInformation.getMaxJvmHeapMemory;
import static org.apache.flink.runtime.util.EnvironmentInformation.getVersion;

/** Util to get value from environments. */
public class EnvUtils {

    private static final Logger LOG = LoggerFactory.getLogger(EnvUtils.class);

    public static final String ENV_KUBERNETES_SERVICE_HOST = "KUBERNETES_SERVICE_HOST";
    public static final String ENV_WEBHOOK_KEYSTORE_FILE = "WEBHOOK_KEYSTORE_FILE";
    public static final String ENV_WEBHOOK_KEYSTORE_PASSWORD = "WEBHOOK_KEYSTORE_PASSWORD";
    public static final String ENV_WEBHOOK_KEYSTORE_TYPE = "WEBHOOK_KEYSTORE_TYPE";
    public static final String ENV_WEBHOOK_SERVER_PORT = "WEBHOOK_SERVER_PORT";
    public static final String ENV_CONF_OVERRIDE_DIR = "CONF_OVERRIDE_DIR";
    public static final String ENV_HOSTNAME = "HOSTNAME";
    public static final String ENV_OPERATOR_NAME = "OPERATOR_NAME";
    public static final String ENV_OPERATOR_NAMESPACE = "OPERATOR_NAMESPACE";
    public static final String ENV_WATCH_NAMESPACES = "WATCH_NAMESPACES";
    public static final String ENV_OPERATOR_KEYSTORE_PASSWORD = "OPERATOR_KEYSTORE_PASSWORD";
    public static final String ENV_OPERATOR_KEYSTORE_PATH = "OPERATOR_KEYSTORE_PATH";
    public static final String ENV_OPERATOR_TRUSTSTORE_PATH = "OPERATOR_TRUSTSTORE_PATH";

    private static final String PROP_FILE = ".flink-kubernetes-operator.version.properties";
    private static final String FAIL_MESSAGE =
            "The file "
                    + PROP_FILE
                    + " has not been generated correctly. You MUST run 'mvn generate-sources' in the flink-kubernetes-operator module.";
    private static final String DEFAULT_TIME_STRING = "1970-01-01T00:00:00+0000";

    /**
     * Get the value provided by environments.
     *
     * @param key the target key
     * @return the value value provided by environments.
     */
    public static Optional<String> get(String key) {
        return Optional.ofNullable(StringUtils.getIfBlank(System.getenv().get(key), () -> null));
    }

    /**
     * Get the value or default value provided by environments.
     *
     * @param key the target key
     * @param defaultValue the default value if key not exists.
     * @return the value or default value provided by environments.
     */
    public static String getOrDefault(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }

    /**
     * Get the value provided by environments.
     *
     * @param key the target key
     * @return the value provided by environments.
     */
    public static String getRequired(String key) {
        return get(key).orElseThrow(
                        () ->
                                new NoSuchElementException(
                                        "Environments: " + key + " cannot be empty"));
    }

    /**
     * Logs information about the environment, like code revision, current user, Java version, and
     * JVM parameters.
     *
     * @param log The logger to log the information to.
     * @param componentName The component name to mention in the log.
     * @param commandLineArgs The arguments accompanying the starting the component.
     */
    public static void logEnvironmentInfo(
            Logger log, String componentName, String[] commandLineArgs) {
        if (log.isInfoEnabled()) {
            Properties properties = new Properties();
            try (InputStream propFile =
                    EnvUtils.class.getClassLoader().getResourceAsStream(PROP_FILE)) {
                if (propFile != null) {
                    properties.load(propFile);
                }
            } catch (IOException e) {
                LOG.info(
                        "Cannot determine code revision: Unable to read version property file.: {}",
                        e.getMessage());
            }
            String javaHome = System.getenv("JAVA_HOME");
            String arch = System.getProperty("os.arch");
            long maxHeapMegabytes = getMaxJvmHeapMemory() >>> 20;
            log.info(
                    "--------------------------------------------------------------------------------");
            log.info(
                    " Starting "
                            + componentName
                            + " (Version: "
                            + getProperty(properties, "project.version", UNKNOWN)
                            + ", Flink Version: "
                            + getVersion()
                            + ", "
                            + "Rev:"
                            + getProperty(
                                    properties, "git.commit.id.abbrev", UNKNOWN_COMMIT_ID_ABBREV)
                            + ", "
                            + "Date:"
                            + getGitCommitTimeString(properties)
                            + ")");
            log.info(" OS current user: " + System.getProperty("user.name"));
            log.info(" JVM: " + getJvmVersion());
            log.info(" Arch: " + arch);
            log.info(" Maximum heap size: " + maxHeapMegabytes + " MiBytes");
            log.info(" JAVA_HOME: " + (javaHome == null ? "(not set)" : javaHome));
            String[] options = getJvmStartupOptionsArray();
            if (options.length == 0) {
                log.info(" JVM Options: (none)");
            } else {
                log.info(" JVM Options:");
                for (String s : options) {
                    log.info("    " + s);
                }
            }
            if (commandLineArgs == null || commandLineArgs.length == 0) {
                log.info(" Program Arguments: (none)");
            } else {
                log.info(" Program Arguments:");
                for (String s : commandLineArgs) {
                    if (GlobalConfiguration.isSensitive(s)) {
                        log.info(
                                "    "
                                        + GlobalConfiguration.HIDDEN_CONTENT
                                        + " (sensitive information)");
                    } else {
                        log.info("    " + s);
                    }
                }
            }
            log.info(" Classpath: " + System.getProperty("java.class.path"));
            log.info(
                    "--------------------------------------------------------------------------------");
        }
    }

    /**
     * @return The Instant of the last commit of this code as a String using the Europe/Berlin
     *     timezone.
     */
    private static String getGitCommitTimeString(Properties properties) {
        try {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    .withZone(ZoneId.of("Europe/Berlin"))
                    .format(
                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")
                                    .parse(
                                            getProperty(
                                                    properties,
                                                    "git.commit.time",
                                                    DEFAULT_TIME_STRING),
                                            Instant::from));
        } catch (DateTimeParseException e) {
            LOG.error("{} : {}", FAIL_MESSAGE, e);
            throw new IllegalStateException(FAIL_MESSAGE, e);
        }
    }

    private static String getProperty(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.charAt(0) == '$') {
            return defaultValue;
        }
        return value;
    }
}
