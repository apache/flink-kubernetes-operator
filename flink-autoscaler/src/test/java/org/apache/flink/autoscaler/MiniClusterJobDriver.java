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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Standalone driver, run in its own JVM by {@link KafkaPulsarPartitionMetricsITCase} rather than
 * in-process, that starts a {@link MiniCluster}, submits the Kafka/Pulsar test job to it, then
 * writes the cluster's REST address and the job ID to a result file and blocks.
 *
 * <p>Kept out-of-process (and free of {@code org.apache.flink.autoscaler.*} imports) because this
 * module vendors a compatibility copy of {@code
 * org.apache.flink.runtime.rest.messages.job.JobDetailsInfo} under {@code src/main/java} that
 * shadows the real Flink class on this module's own classpath. Harmless in production, where the
 * autoscaler and JobManager run in separate processes, but a MiniCluster sharing this module's
 * classpath breaks the JobManager's own {@code JobDetailsInfo} construction.
 */
public final class MiniClusterJobDriver {

    private static final String KAFKA_TOPIC_DOTTED = "orders.eu.v1";
    private static final int KAFKA_TOPIC_DOTTED_PARTITIONS = 3;
    private static final String KAFKA_TOPIC_HYPHEN = "my-topic";
    private static final int KAFKA_TOPIC_HYPHEN_PARTITIONS = 2;
    private static final String KAFKA_TOPIC_MULTI_SUBTASK = "high-parallel-topic";
    private static final int KAFKA_TOPIC_MULTI_SUBTASK_PARTITIONS = 6;
    private static final int KAFKA_SOURCE_PARALLELISM = 3;

    private static final String PULSAR_TOPIC = "orders-v2";
    private static final int PULSAR_SOURCE_PARALLELISM = 2;

    static final String KAFKA_SOURCE_NAME = "kafka-source";
    static final String PULSAR_SOURCE_NAME = "pulsar-source";
    static final String SEQUENCE_SOURCE_NAME = "sequence-source";

    static final String REST_ADDRESS_MARKER = "REST_ADDRESS:";
    static final String JOB_ID_MARKER = "JOB_ID:";

    private MiniClusterJobDriver() {}

    public static void main(String[] args) throws Exception {
        String kafkaBootstrapServers = args[0];
        String pulsarServiceUrl = args[1];
        String pulsarAdminUrl = args[2];
        Path resultFile = Path.of(args[3]);

        JobGraph jobGraph = buildJobGraph(kafkaBootstrapServers, pulsarServiceUrl, pulsarAdminUrl);

        MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(8)
                                .build());
        Runtime.getRuntime().addShutdownHook(new Thread(miniCluster::closeAsync));
        miniCluster.start();
        miniCluster.submitJob(jobGraph).get(60, TimeUnit.SECONDS);

        JobID jobId = jobGraph.getJobID();
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        while (miniCluster.getJobStatus(jobId).get() != JobStatus.RUNNING) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Job did not reach RUNNING in time");
            }
            Thread.sleep(200);
        }

        Files.writeString(
                resultFile,
                REST_ADDRESS_MARKER
                        + miniCluster.getRestAddress().get()
                        + System.lineSeparator()
                        + JOB_ID_MARKER
                        + jobId.toHexString()
                        + System.lineSeparator());

        new CountDownLatch(1).await();
    }

    private static JobGraph buildJobGraph(
            String kafkaBootstrapServers, String pulsarServiceUrl, String pulsarAdminUrl) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Sinks don't set their own parallelism, so without this they'd inherit the machine's
        // CPU-count-based default and starve the MiniCluster's fixed slot count.
        env.setParallelism(1);

        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaBootstrapServers)
                        .setTopics(
                                KAFKA_TOPIC_DOTTED, KAFKA_TOPIC_HYPHEN, KAFKA_TOPIC_MULTI_SUBTASK)
                        .setGroupId("autoscaler-it")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), KAFKA_SOURCE_NAME)
                .setParallelism(KAFKA_SOURCE_PARALLELISM)
                .sinkTo(new DiscardingSink<>());

        PulsarSource<String> pulsarSource =
                PulsarSource.<String>builder()
                        .setServiceUrl(pulsarServiceUrl)
                        .setAdminUrl(pulsarAdminUrl)
                        .setSubscriptionName("autoscaler-it")
                        .setTopics(PULSAR_TOPIC)
                        .setStartCursor(StartCursor.earliest())
                        .setDeserializationSchema(new SimpleStringSchema())
                        .setConfig(PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS, 1L)
                        .build();
        env.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), PULSAR_SOURCE_NAME)
                .setParallelism(PULSAR_SOURCE_PARALLELISM)
                .sinkTo(new DiscardingSink<>());

        env.fromSequence(0, Long.MAX_VALUE)
                .name(SEQUENCE_SOURCE_NAME)
                .setParallelism(1)
                .sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }
}
