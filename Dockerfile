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
FROM maven:3.8.4-openjdk-11 AS build
ARG SKIP_TESTS=true

WORKDIR /app

COPY . .

RUN --mount=type=cache,target=/root/.m2 mvn -ntp clean install -pl !flink-kubernetes-docs -DskipTests=$SKIP_TESTS

RUN cd /app/tools/license; mkdir jars; cd jars; \
    cp /app/flink-kubernetes-operator/target/flink-kubernetes-operator-*-shaded.jar . && \
    cp /app/flink-kubernetes-webhook/target/flink-kubernetes-webhook-*.jar . && \
    cp /app/flink-kubernetes-shaded/target/flink-kubernetes-shaded-*.jar . && \
    cp -r /app/flink-kubernetes-operator/target/plugins ./plugins && \
    cd ../ && ./collect_license_files.sh ./jars ./licenses-output

# stage
FROM openjdk:11-jre
ENV FLINK_HOME=/opt/flink
ENV OPERATOR_VERSION=1.0.0
ENV OPERATOR_JAR=flink-kubernetes-operator-$OPERATOR_VERSION-shaded.jar
ENV WEBHOOK_JAR=flink-kubernetes-webhook-$OPERATOR_VERSION.jar
ENV FLINK_KUBERNETES_SHADED_JAR=flink-kubernetes-shaded-$OPERATOR_VERSION.jar

WORKDIR /flink-kubernetes-operator
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink

COPY --from=build /app/flink-kubernetes-operator/target/$OPERATOR_JAR .
COPY --from=build /app/flink-kubernetes-webhook/target/$WEBHOOK_JAR .
COPY --from=build /app/flink-kubernetes-shaded/target/$FLINK_KUBERNETES_SHADED_JAR .
COPY --from=build /app/flink-kubernetes-operator/target/plugins $FLINK_HOME/plugins
COPY --from=build /app/tools/license/licenses-output/NOTICE .
COPY --from=build /app/tools/license/licenses-output/licenses ./licenses
COPY docker-entrypoint.sh /

RUN chown -R flink:flink $FLINK_HOME && \
    chown flink:flink $OPERATOR_JAR && \
    chown flink:flink $WEBHOOK_JAR && \
    chown flink:flink $FLINK_KUBERNETES_SHADED_JAR && \
    chown flink:flink /docker-entrypoint.sh

# Updating Debian
RUN apt-get update
RUN apt-get upgrade -y

USER flink
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
