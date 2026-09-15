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

package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.pulsar.PulsarContainer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.autoscaler.MiniClusterJobDriver.JOB_ID_MARKER;
import static org.apache.flink.autoscaler.MiniClusterJobDriver.KAFKA_SOURCE_NAME;
import static org.apache.flink.autoscaler.MiniClusterJobDriver.PULSAR_SOURCE_NAME;
import static org.apache.flink.autoscaler.MiniClusterJobDriver.REST_ADDRESS_MARKER;
import static org.apache.flink.autoscaler.MiniClusterJobDriver.SEQUENCE_SOURCE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that {@link ScalingMetricCollector#getJobTopology} derives the correct {@code
 * numSourcePartitions} against real Kafka and Pulsar brokers: multi-topic summing, dotted and
 * hyphenated topic names, source parallelism &gt; 1, multiple source vertices in one job, and a
 * non-Kafka/Pulsar source that must not report a partition count.
 */
class KafkaPulsarPartitionMetricsITCase {

    private static final String KAFKA_TOPIC_DOTTED = "orders.eu.v1";
    private static final int KAFKA_TOPIC_DOTTED_PARTITIONS = 3;
    private static final String KAFKA_TOPIC_HYPHEN = "my-topic";
    private static final int KAFKA_TOPIC_HYPHEN_PARTITIONS = 2;
    private static final String KAFKA_TOPIC_MULTI_SUBTASK = "high-parallel-topic";
    private static final int KAFKA_TOPIC_MULTI_SUBTASK_PARTITIONS = 6;
    private static final int EXPECTED_KAFKA_PARTITIONS =
            KAFKA_TOPIC_DOTTED_PARTITIONS
                    + KAFKA_TOPIC_HYPHEN_PARTITIONS
                    + KAFKA_TOPIC_MULTI_SUBTASK_PARTITIONS;

    private static final String PULSAR_TOPIC = "orders-v2";
    private static final int PULSAR_TOPIC_PARTITIONS = 4;

    private static KafkaContainer kafka;
    private static PulsarContainer pulsar;
    private static Process driverProcess;
    private static Path driverResultFile;
    private static String restAddress;
    private static JobID jobId;
    private static Path driverLogFile;

    @BeforeAll
    static void setup() throws Exception {
        kafka = new KafkaContainer("apache/kafka:3.8.0");
        kafka.start();
        createKafkaTopicsAndProduce();

        pulsar = new PulsarContainer("apachepulsar/pulsar:3.2.3");
        pulsar.start();
        createPulsarTopicAndProduce();

        driverResultFile = Files.createTempFile("autoscaler-it-driver-result", ".txt");
        Files.delete(driverResultFile);
        driverLogFile = Files.createTempFile("autoscaler-it-driver", ".log");
        driverProcess =
                startDriverProcess(
                        kafka.getBootstrapServers(),
                        pulsar.getPulsarBrokerUrl(),
                        pulsar.getHttpServiceUrl(),
                        driverResultFile,
                        driverLogFile);
        awaitDriverReady(driverProcess, driverResultFile);
    }

    @AfterAll
    static void teardown() throws Exception {
        if (driverProcess != null) {
            driverProcess.destroy();
            if (!driverProcess.waitFor(10, TimeUnit.SECONDS)) {
                driverProcess.destroyForcibly();
            }
        }
        if (driverResultFile != null) {
            Files.deleteIfExists(driverResultFile);
        }
        if (driverLogFile != null) {
            Files.deleteIfExists(driverLogFile);
        }
        if (pulsar != null) {
            pulsar.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    @Test
    @Timeout(120)
    void detectsPartitionCountsAcrossKafkaAndPulsarSources() throws Exception {
        var collector =
                new RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>>(
                        new InMemoryAutoScalerStateStore<>());
        // ScalingMetricCollector closes the REST client after each use, so the supplier must
        // create a fresh one every time rather than reuse a captured instance.
        var context =
                new JobAutoScalerContext<>(
                        jobId,
                        jobId,
                        JobStatus.RUNNING,
                        new Configuration(),
                        new UnregisteredMetricsGroup(),
                        () ->
                                new RestClusterClient<>(
                                        new Configuration(),
                                        "test-cluster",
                                        (c, e) -> new StandaloneClientHAServices(restAddress)));

        await().atMost(Duration.ofSeconds(90))
                .untilAsserted(
                        () -> {
                            var jobDetailsInfo =
                                    collector.getJobDetailsInfo(context, Duration.ofSeconds(30));
                            var topology = collector.getJobTopology(context, jobDetailsInfo);

                            assertThat(
                                            numSourcePartitionsOf(
                                                    jobDetailsInfo, topology, KAFKA_SOURCE_NAME))
                                    .as(
                                            "Kafka source: multi-topic summing, dotted/hyphen"
                                                    + " names, parallelism > 1")
                                    .isEqualTo(EXPECTED_KAFKA_PARTITIONS);

                            assertThat(
                                            numSourcePartitionsOf(
                                                    jobDetailsInfo, topology, PULSAR_SOURCE_NAME))
                                    .as("Pulsar source: independent per-vertex partition count")
                                    .isEqualTo(PULSAR_TOPIC_PARTITIONS);

                            assertThat(
                                            numSourcePartitionsOf(
                                                    jobDetailsInfo, topology, SEQUENCE_SOURCE_NAME))
                                    .as("Non-Kafka/Pulsar source must not report a partition count")
                                    .isEqualTo(0);
                        });
    }

    private static int numSourcePartitionsOf(
            JobDetailsInfo jobDetailsInfo, JobTopology topology, String vertexName) {
        var vertexId =
                jobDetailsInfo.getJobVertexInfos().stream()
                        .filter(v -> v.getName().contains(vertexName))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "No vertex found with name containing "
                                                        + vertexName))
                        .getJobVertexID();
        return topology.get(vertexId).getNumSourcePartitions();
    }

    /**
     * Starts {@link MiniClusterJobDriver} in its own JVM; see its javadoc and {@link
     * #reorderClasspathForDriver} for why.
     */
    private static Process startDriverProcess(
            String kafkaBootstrapServers,
            String pulsarServiceUrl,
            String pulsarAdminUrl,
            Path resultFile,
            Path logFile)
            throws IOException {
        String javaBin =
                System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = reorderClasspathForDriver(System.getProperty("java.class.path"));

        ProcessBuilder processBuilder =
                new ProcessBuilder(
                        javaBin,
                        "-cp",
                        classpath,
                        MiniClusterJobDriver.class.getName(),
                        kafkaBootstrapServers,
                        pulsarServiceUrl,
                        pulsarAdminUrl,
                        resultFile.toString());
        // A file, not inherited/piped: writing into this surefire-forked JVM's own stdout
        // corrupts surefire's binary result-reporting protocol.
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(logFile.toFile());
        return processBuilder.start();
    }

    /**
     * Moves this module's own main output (where the {@code JobDetailsInfo} compatibility shim
     * lives, see {@link MiniClusterJobDriver}'s javadoc) to the end of the classpath, so the real
     * Flink jars resolve that class name instead. That output is the exploded {@code
     * target/classes} directory under surefire (runs before {@code package}) but the packaged
     * {@code flink-autoscaler-*.jar} under failsafe (runs after it); both are matched, and neither
     * the test-jar nor {@code target/test-classes} is touched, since moving the latter would let
     * {@code flink-runtime}'s bundled, log-silencing {@code log4j2-test.properties} shadow this
     * module's.
     */
    private static String reorderClasspathForDriver(String currentClasspath) {
        List<String> entries = new ArrayList<>(List.of(currentClasspath.split(File.pathSeparator)));
        List<String> ownMainOutput = new ArrayList<>();
        entries.removeIf(
                entry -> {
                    String fileName = Path.of(entry).getFileName().toString();
                    boolean isOwnMainOutput =
                            entry.endsWith(
                                            File.separator
                                                    + "flink-autoscaler"
                                                    + File.separator
                                                    + "target"
                                                    + File.separator
                                                    + "classes")
                                    || (fileName.startsWith("flink-autoscaler-")
                                            && fileName.endsWith(".jar")
                                            && !fileName.contains("-tests"));
                    if (isOwnMainOutput) {
                        ownMainOutput.add(entry);
                    }
                    return isOwnMainOutput;
                });
        entries.addAll(ownMainOutput);
        return String.join(File.pathSeparator, entries);
    }

    /**
     * Polls {@code resultFile} for the two lines {@link MiniClusterJobDriver} writes once ready.
     */
    private static void awaitDriverReady(Process process, Path resultFile) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        List<String> resultLines = List.of();
        while (resultLines.size() < 2 && System.currentTimeMillis() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException(
                        "Driver process exited early with code " + process.exitValue());
            }
            if (Files.exists(resultFile)) {
                resultLines = Files.readAllLines(resultFile, StandardCharsets.UTF_8);
            }
            if (resultLines.size() < 2) {
                Thread.sleep(500);
            }
        }
        if (resultLines.size() < 2) {
            throw new IllegalStateException("Driver process did not become ready in time");
        }
        for (String line : resultLines) {
            if (line.startsWith(REST_ADDRESS_MARKER)) {
                restAddress = line.substring(REST_ADDRESS_MARKER.length());
            } else if (line.startsWith(JOB_ID_MARKER)) {
                jobId = JobID.fromHexString(line.substring(JOB_ID_MARKER.length()));
            }
        }
        if (restAddress == null || jobId == null) {
            throw new IllegalStateException("Driver result file did not contain expected markers");
        }
    }

    private static void createKafkaTopicsAndProduce() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (Admin admin = Admin.create(adminProps)) {
            admin.createTopics(
                            List.of(
                                    new NewTopic(
                                            KAFKA_TOPIC_DOTTED,
                                            KAFKA_TOPIC_DOTTED_PARTITIONS,
                                            (short) 1),
                                    new NewTopic(
                                            KAFKA_TOPIC_HYPHEN,
                                            KAFKA_TOPIC_HYPHEN_PARTITIONS,
                                            (short) 1),
                                    new NewTopic(
                                            KAFKA_TOPIC_MULTI_SUBTASK,
                                            KAFKA_TOPIC_MULTI_SUBTASK_PARTITIONS,
                                            (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        }

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            produceToEveryPartition(producer, KAFKA_TOPIC_DOTTED, KAFKA_TOPIC_DOTTED_PARTITIONS);
            produceToEveryPartition(producer, KAFKA_TOPIC_HYPHEN, KAFKA_TOPIC_HYPHEN_PARTITIONS);
            produceToEveryPartition(
                    producer, KAFKA_TOPIC_MULTI_SUBTASK, KAFKA_TOPIC_MULTI_SUBTASK_PARTITIONS);
            producer.flush();
        }
    }

    private static void produceToEveryPartition(
            KafkaProducer<String, String> producer, String topic, int partitions) throws Exception {
        for (int p = 0; p < partitions; p++) {
            producer.send(new ProducerRecord<>(topic, p, "key-" + p, "value-" + p))
                    .get(30, TimeUnit.SECONDS);
        }
    }

    private static void createPulsarTopicAndProduce() throws Exception {
        try (PulsarAdmin admin =
                PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build()) {
            admin.topics().createPartitionedTopic(PULSAR_TOPIC, PULSAR_TOPIC_PARTITIONS);
        }

        try (PulsarClient client =
                PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build()) {
            for (int p = 0; p < PULSAR_TOPIC_PARTITIONS; p++) {
                String partitionTopic = PULSAR_TOPIC + "-partition-" + p;
                try (var producer =
                        client.newProducer(Schema.STRING).topic(partitionTopic).create()) {
                    producer.send("value-" + p);
                }
            }
        }
    }
}
