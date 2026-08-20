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

package org.apache.flink.autoscaler.utils;

/**
 * Parses Kafka/Pulsar source-partition metric names to extract a stable per-partition key.
 *
 * <p>Uses plain string splitting/indexing rather than a regex, since metric names are fully
 * job-controlled and this must stay linear-time regardless of input shape.
 *
 * <p>Parsing splits on the '.' delimiter and treats each name component as dot-free: Flink's metric
 * query service replaces '.' within a component with '_' before joining, so a '.' in the
 * identifiers returned by the REST metrics endpoint is always a real delimiter. Only correctness on
 * legitimate input relies on this assumption; safety does not, since an unexpected shape merely
 * yields null.
 */
public final class PartitionMetricNameParser {

    private static final String KAFKA_CLUSTER = "kafkaCluster";
    private static final String KAFKA_SOURCE_READER = "KafkaSourceReader";
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";
    private static final String CURRENT_OFFSET = "currentOffset";
    private static final String PULSAR_CONSUMER = "PulsarConsumer";
    private static final String NUM_MSGS_RECEIVED = "numMsgsReceived";
    private static final String PULSAR_PARTITION_INFIX = "-partition-";

    private PartitionMetricNameParser() {}

    /**
     * Returns {@code "<cluster>-<topic>-<id>"}, or {@code null} if this is not a Kafka
     * currentOffset partition metric. The cluster segment is the literal "null" when absent; only
     * distinct keys matter to callers.
     */
    public static String parseKafkaPartitionKey(String metricName) {
        String[] parts = metricName.split("\\.", -1);
        int n = parts.length;
        // tail: KafkaSourceReader.topic.<topic>.partition.<id>.currentOffset
        if (n < 6
                || !parts[n - 6].equals(KAFKA_SOURCE_READER)
                || !parts[n - 5].equals(TOPIC)
                || !parts[n - 3].equals(PARTITION)
                || !isAllDigits(parts[n - 2])
                || !parts[n - 1].equals(CURRENT_OFFSET)) {
            return null;
        }
        // optional ".kafkaCluster.<cluster>." precedes the tail; null when absent
        String cluster = (n >= 8 && parts[n - 8].equals(KAFKA_CLUSTER)) ? parts[n - 7] : null;
        return cluster + "-" + parts[n - 4] + "-" + parts[n - 2];
    }

    /**
     * Returns {@code "<topic>-<id>"}, or {@code null} if this is not a Pulsar numMsgsReceived
     * partition metric.
     */
    public static String parsePulsarPartitionKey(String metricName) {
        String[] parts = metricName.split("\\.", -1);
        int n = parts.length;
        // tail: PulsarConsumer.<topic>-partition-<id>.<consumerHash>.numMsgsReceived
        if (n < 3 || !parts[n - 1].equals(NUM_MSGS_RECEIVED)) {
            return null;
        }
        for (int i = 0; i + 1 < n - 1; i++) {
            if (!parts[i].equals(PULSAR_CONSUMER)) {
                continue;
            }
            String segment = parts[i + 1]; // "<topic>-partition-<id>"
            int idx = segment.lastIndexOf(PULSAR_PARTITION_INFIX);
            if (idx <= 0) {
                return null;
            }
            String id = segment.substring(idx + PULSAR_PARTITION_INFIX.length());
            return isAllDigits(id) ? segment.substring(0, idx) + "-" + id : null;
        }
        return null;
    }

    private static boolean isAllDigits(String s) {
        if (s.isEmpty()) {
            return false; // matches \d+, which requires at least one digit
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') { // ASCII only, matching regex \d
                return false;
            }
        }
        return true;
    }
}
