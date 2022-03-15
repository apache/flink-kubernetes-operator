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

WORKDIR /app
ENV SHADED_DIR=flink-kubernetes-shaded
ENV OPERATOR_DIR=flink-kubernetes-operator
ENV WEBHOOK_DIR=flink-kubernetes-webhook

RUN mkdir $OPERATOR_DIR $WEBHOOK_DIR

COPY pom.xml .
COPY $SHADED_DIR/pom.xml ./$SHADED_DIR/
COPY $WEBHOOK_DIR/pom.xml ./$WEBHOOK_DIR/
COPY $OPERATOR_DIR/pom.xml ./$OPERATOR_DIR/

COPY $OPERATOR_DIR/src ./$OPERATOR_DIR/src
COPY $WEBHOOK_DIR/src ./$WEBHOOK_DIR/src

COPY tools ./tools

RUN --mount=type=cache,target=/root/.m2 mvn -ntp clean install

# stage
FROM openjdk:11-jre
ENV FLINK_HOME=/opt/flink
ENV OPERATOR_VERSION=1.0-SNAPSHOT
ENV OPERATOR_JAR=flink-kubernetes-operator-$OPERATOR_VERSION-shaded.jar
ENV WEBHOOK_JAR=flink-kubernetes-webhook-$OPERATOR_VERSION.jar
ENV FLINK_KUBERNETES_SHADED_JAR=flink-kubernetes-shaded-$OPERATOR_VERSION.jar

WORKDIR /
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink

COPY --from=build /app/flink-kubernetes-operator/target/$OPERATOR_JAR .
COPY --from=build /app/flink-kubernetes-webhook/target/$WEBHOOK_JAR .
COPY --from=build /app/flink-kubernetes-shaded/target/$FLINK_KUBERNETES_SHADED_JAR .
COPY --from=build /app/flink-kubernetes-operator/target/plugins $FLINK_HOME/plugins
COPY docker-entrypoint.sh /

RUN chown -R flink:flink $FLINK_HOME && \
    chown flink:flink $OPERATOR_JAR && \
    chown flink:flink $WEBHOOK_JAR && \
    chown flink:flink docker-entrypoint.sh

USER flink
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
