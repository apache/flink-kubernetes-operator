---
title: "Logging"
weight: 3
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Logging

The operator controls the logging behaviour for the Flink applications and the operator itself using configuration files mounted externally via ConfigMaps. [Configuration files](https://github.com/apache/flink-kubernetes-operator/tree/main/helm/flink-kubernetes-operator/conf) with default values are shipped in the Helm chart. It is recommended to review and adjust them if needed in the `values.yaml` file before deploying the operator in production environments.

The metric side of observability, including the SLF4J reporter that writes metrics into the operator logs, is covered under [Metrics]({{< ref "docs/operations/metrics" >}}).

The operator supports two logging frameworks: [Log4j2](#log4j2) (default) and [Logback](#logback). Both sets of configuration files are shipped in the Helm chart and Docker image. The active framework is selected at install time via the `logging.framework` Helm value, either in `values.yaml` or on the command line:

```shell
# Use the default Log4j2 framework (explicit, same as omitting the flag)
helm install flink-operator helm/flink-kubernetes-operator --set logging.framework=log4j2

# Switch to Logback
helm install flink-operator helm/flink-kubernetes-operator --set logging.framework=logback
```

This controls three things:
1. Which logging configuration files are mounted into `/opt/flink/conf/`
2. Which JVM system property is passed to select the framework (`log4j.configurationFile` or `logback.configurationFile`)
3. Which SLF4J binding JAR is placed on the classpath at runtime

{{< hint info >}}
Logging in the operator is intentionally succinct and does not include contextual information such as namespace or name of the FlinkDeployment objects.
The MDC provided by the operator-sdk supplies this information, used directly in the log layout.

See the [Java Operator SDK docs](https://javaoperatorsdk.io/docs/features#contextual-info-for-logging-with-mdc) for more details.
{{< /hint >}}

For accessing the job logs or changing the log level dynamically, check the [Logging](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#logging) section of the Flink Native Kubernetes documentation.

## Log4j2

Log4j2 is the default logging framework. To append to or override the default log configuration properties for the operator and the Flink deployments, define the `log4j-operator.properties` and `log4j-console.properties` keys respectively:

```yaml
defaultConfiguration:
  create: true
  append: true
  log4j-operator.properties: |+
    # Flink Operator Logging Overrides
    # rootLogger.level = DEBUG
    # The monitor interval in seconds to enable log4j automatic reconfiguration
    # monitorInterval = 30
  log4j-console.properties: |+
    # Flink Deployment Logging Overrides
    # rootLogger.level = DEBUG
    # The monitor interval in seconds to enable log4j automatic reconfiguration
    # monitorInterval = 30
```

## Logback

When using Logback, the equivalent configuration keys are `logback-operator.xml` and `logback-console.xml`:

```yaml
logging:
  framework: logback

defaultConfiguration:
  create: true
  append: true
  logback-operator.xml: |+
    <!-- Flink Operator Logback Overrides -->
    <!-- <configuration>
           <appender name="ConsoleAppender" class="ch.qos.logback.core.ConsoleAppender">
             <encoder>
               <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level %-60logger{26} %X - %msg%n</pattern>
             </encoder>
           </appender>
           <root level="DEBUG">
             <appender-ref ref="ConsoleAppender" />
           </root>
         </configuration> -->
  logback-console.xml: |+
    <!-- Flink Deployment Logback Overrides -->
    <!-- <configuration>
           <appender name="ConsoleAppender" class="ch.qos.logback.core.ConsoleAppender">
             <encoder>
               <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level %-60logger{26} %X - %msg%n</pattern>
             </encoder>
           </appender>
           <root level="DEBUG">
             <appender-ref ref="ConsoleAppender" />
           </root>
         </configuration> -->
```

{{< hint warning >}}
Logback XML overrides replace the entire default configuration. Unlike Log4j2 `.properties` files where user entries are appended, XML files cannot be concatenated (two `<configuration>` root elements produce invalid XML). Ensure the custom `logback-operator.xml` or `logback-console.xml` is complete and self-contained. Logback supports only XML-based configuration and does not provide native support for `.properties` configuration files.
{{< /hint >}}

## Logging Library Version Overrides

The operator ships with Logback 1.2.x and SLF4J 1.7.x. These versions are bundled in the Docker image and the SLF4J 1.7.x API is shaded into the operator JAR.

{{< hint warning >}}
Upgrading to Logback 1.4+/1.5+ or SLF4J 2.x is not supported. SLF4J 2.x uses a `ServiceLoader`-based binding mechanism that is incompatible with the SLF4J 1.7.x API shaded inside the operator. Replacing the JARs at runtime will result in `ClassNotFoundException: org.slf4j.impl.StaticLoggerBinder`.
{{< /hint >}}

## FlinkDeployment Logging Configuration

The default logging settings can be overridden per deployment by putting the entire log configuration into `spec.logConfiguration`.

For Log4j2:

```yaml
spec:
  ...
  logConfiguration:
    log4j-console.properties: |+
      rootLogger.level = DEBUG
      rootLogger.appenderRef.file.ref = LogFile
      ...
```

For Logback:

```yaml
spec:
  ...
  logConfiguration:
    logback-console.xml: |+
      <configuration>
        <appender name="ConsoleAppender" class="ch.qos.logback.core.ConsoleAppender">
          <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level %-60.60logger{60} %X - %msg%n</pattern>
          </encoder>
        </appender>
        <root level="DEBUG">
          <appender-ref ref="ConsoleAppender" />
        </root>
      </configuration>
```
