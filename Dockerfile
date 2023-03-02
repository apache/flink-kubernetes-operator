################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
# Build
FROM maven:3.8.4-eclipse-temurin-11 AS build
ARG SKIP_TESTS=true

WORKDIR /app

COPY . .

RUN --mount=type=cache,target=/root/.m2 mvn -ntp clean install -pl flink-kubernetes-standalone,flink-kubernetes-operator-api,flink-kubernetes-operator,flink-kubernetes-operator-autoscaler,flink-kubernetes-webhook -DskipTests=$SKIP_TESTS

RUN cd /app/tools/license; mkdir jars; cd jars; \
    cp /app/flink-kubernetes-operator/target/flink-kubernetes-operator-*-shaded.jar . && \
    cp /app/flink-kubernetes-operator-autoscaler/target/flink-kubernetes-operator-autoscaler-*.jar . && \
    cp /app/flink-kubernetes-webhook/target/flink-kubernetes-webhook-*-shaded.jar . && \
    cp /app/flink-kubernetes-standalone/target/flink-kubernetes-standalone-*-shaded.jar . && \
    cp -r /app/flink-kubernetes-operator/target/plugins ./plugins && \
    cd ../ && ./collect_license_files.sh ./jars ./licenses-output

# stage
FROM eclipse-temurin:11-jre-jammy
ENV FLINK_HOME=/opt/flink
ENV FLINK_PLUGINS_DIR=$FLINK_HOME/plugins
ENV OPERATOR_VERSION=1.5-SNAPSHOT
ENV OPERATOR_JAR=flink-kubernetes-operator-$OPERATOR_VERSION-shaded.jar
ENV AUTOSCALER_JAR=flink-kubernetes-operator-autoscaler-$OPERATOR_VERSION.jar
ENV WEBHOOK_JAR=flink-kubernetes-webhook-$OPERATOR_VERSION-shaded.jar
ENV FLINK_KUBERNETES_SHADED_JAR=flink-kubernetes-standalone-$OPERATOR_VERSION-shaded.jar

ENV OPERATOR_LIB=$FLINK_HOME/operator-lib
RUN mkdir -p $OPERATOR_LIB

WORKDIR /flink-kubernetes-operator
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink

COPY --from=build /app/flink-kubernetes-operator/target/$OPERATOR_JAR .
COPY --from=build /app/flink-kubernetes-operator-autoscaler/target/$AUTOSCALER_JAR .
COPY --from=build /app/flink-kubernetes-webhook/target/$WEBHOOK_JAR .
COPY --from=build /app/flink-kubernetes-standalone/target/$FLINK_KUBERNETES_SHADED_JAR .
COPY --from=build /app/flink-kubernetes-operator/target/plugins $FLINK_HOME/plugins
COPY --from=build /app/tools/license/licenses-output/NOTICE .
COPY --from=build /app/tools/license/licenses-output/licenses ./licenses
COPY docker-entrypoint.sh /

RUN chown -R flink:flink $FLINK_HOME && \
    chown flink:flink $OPERATOR_JAR && \
    chown flink:flink $AUTOSCALER_JAR && \
    chown flink:flink $WEBHOOK_JAR && \
    chown flink:flink $FLINK_KUBERNETES_SHADED_JAR && \
    chown flink:flink /docker-entrypoint.sh

ARG SKIP_OS_UPDATE=true

# Updating Debian
RUN if [ "$SKIP_OS_UPDATE" = "false" ]; then apt-get update; fi
RUN if [ "$SKIP_OS_UPDATE" = "false" ]; then apt-get upgrade -y; fi

USER flink
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
