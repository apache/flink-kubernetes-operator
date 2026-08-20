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

package org.apache.flink.autoscaler.utils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** Test for {@link PartitionMetricNameParser}. */
public class PartitionMetricNameParserTest {

    /**
     * The original regex-based implementation that {@link PartitionMetricNameParser} replaced, kept
     * here so we can assert the new parser produces exactly the same result on realistic inputs
     * (see {@link #testEquivalentToLegacyRegex()}).
     */
    private static final Pattern LEGACY_REGEX =
            Pattern.compile(
                    "^.*?(\\.kafkaCluster\\.(?<kafkaCluster>.+))?\\.KafkaSourceReader\\.topic\\.(?<kafkaTopic>.+)\\.partition\\.(?<kafkaId>\\d+)\\.currentOffset$"
                            + "|^.*\\.PulsarConsumer\\.(?<pulsarTopic>.+)-partition-(?<pulsarId>\\d+)\\..*\\.numMsgsReceived$");

    private static String legacyPartitionKey(String metricName) {
        Matcher matcher = LEGACY_REGEX.matcher(metricName);
        if (matcher.matches()) {
            String kafkaTopic = matcher.group("kafkaTopic");
            String kafkaCluster = matcher.group("kafkaCluster");
            String kafkaId = matcher.group("kafkaId");
            String pulsarTopic = matcher.group("pulsarTopic");
            String pulsarId = matcher.group("pulsarId");
            return kafkaTopic != null
                    ? kafkaCluster + "-" + kafkaTopic + "-" + kafkaId
                    : pulsarTopic + "-" + pulsarId;
        }
        return null;
    }

    /** Mirrors how {@code ScalingMetricCollector} combines the two parser methods. */
    private static String newPartitionKey(String metricName) {
        String kafka = PartitionMetricNameParser.parseKafkaPartitionKey(metricName);
        return kafka != null
                ? kafka
                : PartitionMetricNameParser.parsePulsarPartitionKey(metricName);
    }

    @Test
    public void testKafkaWithoutCluster() {
        assertEquals(
                "null-testTopic-0",
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.0.currentOffset"));
        assertEquals(
                "null-anotherTopic-0",
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.anotherTopic.partition.0.currentOffset"));
    }

    @Test
    public void testKafkaWithCluster() {
        assertEquals(
                "my-cluster-1-testTopic-0",
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "1.Source__Kafka_Source_(testTopic).kafkaCluster.my-cluster-1.KafkaSourceReader.topic.testTopic.partition.0.currentOffset"));
        assertEquals(
                "my-cluster-2-testTopic-3",
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "1.Source__Kafka_Source_(testTopic).kafkaCluster.my-cluster-2.KafkaSourceReader.topic.testTopic.partition.3.currentOffset"));
    }

    @Test
    public void testKafkaNonMatchingMetric() {
        assertNull(
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.0.anotherMetric"));
    }

    @Test
    public void testKafkaMalformedShapes() {
        // partition id is not numeric
        assertNull(
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "x.KafkaSourceReader.topic.testTopic.partition.abc.currentOffset"));
        // non-ASCII digits do not count as \d
        assertNull(
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "x.KafkaSourceReader.topic.testTopic.partition.٥.currentOffset"));
        // empty partition id segment
        assertNull(
                PartitionMetricNameParser.parseKafkaPartitionKey(
                        "x.KafkaSourceReader.topic.testTopic.partition..currentOffset"));
        // too short to contain the required shape
        assertNull(PartitionMetricNameParser.parseKafkaPartitionKey("topic.testTopic.partition"));
        assertNull(PartitionMetricNameParser.parseKafkaPartitionKey(""));
    }

    @Test
    public void testPulsar() {
        assertEquals(
                "persistent_//public/default/testTopic-1",
                PartitionMetricNameParser.parsePulsarPartitionKey(
                        "0.Source__pulsar_source[1].PulsarConsumer.persistent_//public/default/testTopic-partition-1.d842f.numMsgsReceived"));
        // Same topic/partition, different (irrelevant) consumer-hash segment -> same key.
        assertEquals(
                "persistent_//public/default/testTopic-1",
                PartitionMetricNameParser.parsePulsarPartitionKey(
                        "0.Source__pulsar_source[1].PulsarConsumer.persistent_//public/default/testTopic-partition-1.660d2.numMsgsReceived"));
        assertEquals(
                "persistent_//public/default/otherTopic-2",
                PartitionMetricNameParser.parsePulsarPartitionKey(
                        "0.Source__pulsar_source[1].PulsarConsumer.persistent_//public/default/otherTopic-partition-2.m953d.numMsgsReceived"));
    }

    @Test
    public void testPulsarNonMatchingMetric() {
        assertNull(
                PartitionMetricNameParser.parsePulsarPartitionKey(
                        "0.Source__pulsar_source[1].PulsarConsumer.persistent_//public/default/testTopic-partition-1.d842f.someOtherMetric"));
    }

    @Test
    public void testPulsarMalformedShapes() {
        // partition id is not numeric
        assertNull(
                PartitionMetricNameParser.parsePulsarPartitionKey(
                        "0.PulsarConsumer.testTopic-partition-abc.numMsgsReceived"));
        // no "-partition-" infix at all
        assertNull(
                PartitionMetricNameParser.parsePulsarPartitionKey(
                        "0.PulsarConsumer.testTopic.numMsgsReceived"));
        // too short to contain the required shape
        assertNull(PartitionMetricNameParser.parsePulsarPartitionKey("PulsarConsumer"));
        assertNull(PartitionMetricNameParser.parsePulsarPartitionKey(""));
    }

    @Test
    public void testPulsarFirstMarkerWinsWithoutException() {
        // only the first "PulsarConsumer" segment is considered
        assertNull(
                PartitionMetricNameParser.parsePulsarPartitionKey(
                        "0.PulsarConsumer.noPartitionHere.PulsarConsumer.testTopic-partition-1.numMsgsReceived"));
    }

    /**
     * Asserts the new parser produces exactly the same key as the original regex on the captured
     * real-world metric names plus a large set of generated realistic ones. Inputs are kept within
     * the shape actually emitted by Flink's metric query service (each metric-name component is
     * dot-free, since '.' is the delimiter), which is the space the operator ever sees in practice.
     */
    @Test
    public void testEquivalentToLegacyRegex() {
        List<String> inputs = new ArrayList<>();

        // Captured real-world fixtures (mirroring MetricsCollectionAndEvaluationTest).
        inputs.add(
                "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.0.anotherMetric");
        inputs.add(
                "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.anotherTopic.partition.0.currentOffset");
        inputs.add(
                "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.3.currentOffset");
        inputs.add(
                "1.Source__Kafka_Source_(testTopic).kafkaCluster.my-cluster-2.KafkaSourceReader.topic.testTopic.partition.1.currentOffset");
        inputs.add(
                "0.Source__pulsar_source[1].PulsarConsumer.persistent_//public/default/testTopic-partition-1.d842f.numMsgsReceived");
        inputs.add(
                "0.Source__pulsar_source[1].PulsarConsumer.persistent_//public/default/otherTopic-partition-2.m953d.numMsgsReceived");

        // Generated realistic names (deterministic seed for reproducibility). Kafka and Pulsar use
        // connector-specific source-operator prefixes and topic shapes, mirroring what each emits.
        Random rnd = new Random(42);
        String[] kafkaTopics = {"testTopic", "orders_eu_v1", "anotherTopic", "my-topic", "a"};
        String[] pulsarTopics = {
            "persistent_//public/default/testTopic",
            "persistent_//public/default/orders_eu_v1",
            "persistent_//tenant/ns/my-topic",
            "non-persistent_//public/default/events"
        };
        String[] clusters = {"my-cluster-1", "cluster_2", "c"};
        for (int i = 0; i < 100; i++) {
            int id = rnd.nextInt(1000);
            String hash = Integer.toHexString(rnd.nextInt(0xfffff));

            String kafkaTopic = kafkaTopics[rnd.nextInt(kafkaTopics.length)];
            String kafkaPrefix = rnd.nextInt(9) + ".Source__Kafka_Source_(" + kafkaTopic + ")";
            // Kafka without cluster.
            inputs.add(
                    kafkaPrefix
                            + ".KafkaSourceReader.topic."
                            + kafkaTopic
                            + ".partition."
                            + id
                            + ".currentOffset");
            // Kafka with cluster.
            inputs.add(
                    kafkaPrefix
                            + ".kafkaCluster."
                            + clusters[rnd.nextInt(clusters.length)]
                            + ".KafkaSourceReader.topic."
                            + kafkaTopic
                            + ".partition."
                            + id
                            + ".currentOffset");
            // Kafka near-miss negative (wrong suffix).
            inputs.add(
                    kafkaPrefix
                            + ".KafkaSourceReader.topic."
                            + kafkaTopic
                            + ".partition."
                            + id
                            + ".someOtherMetric");

            String pulsarTopic = pulsarTopics[rnd.nextInt(pulsarTopics.length)];
            String pulsarPrefix = rnd.nextInt(9) + ".Source__pulsar_source[" + rnd.nextInt(4) + "]";
            // Pulsar.
            inputs.add(
                    pulsarPrefix
                            + ".PulsarConsumer."
                            + pulsarTopic
                            + "-partition-"
                            + id
                            + "."
                            + hash
                            + ".numMsgsReceived");
            // Pulsar near-miss negative (non-numeric id).
            inputs.add(
                    pulsarPrefix
                            + ".PulsarConsumer."
                            + pulsarTopic
                            + "-partition-x."
                            + hash
                            + ".numMsgsReceived");
        }

        for (String v : inputs) {
            assertEquals(legacyPartitionKey(v), newPartitionKey(v), "mismatch for: " + v);
        }
    }

    @Test
    public void testAdversarialPayloadDoesNotHang() {
        StringBuilder evil = new StringBuilder("0.Source__pulsar_source[1]");
        for (int i = 0; i < 2000; i++) {
            evil.append(".PulsarConsumer.X-partition-1.a.numMsgsReceivedZ");
        }
        evil.append(".end");
        String payload = evil.toString();

        assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> {
                    assertNull(PartitionMetricNameParser.parseKafkaPartitionKey(payload));
                    assertNull(PartitionMetricNameParser.parsePulsarPartitionKey(payload));
                });
    }
}
