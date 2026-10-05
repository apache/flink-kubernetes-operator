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

package org.apache.flink.examples.bluegreen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * A job for the ADVANCED Blue/Green transition mode, whose output shows how a transition split the
 * records between the two deployments.
 *
 * <p>The records carry a sequence id, and their event time follows the id: most are in order, every
 * 10th is out of order within the watermark bound, and every 100th is late, older than the
 * watermark by far. The file sink writes one line per record, {@code <color>,<id>,<eventTime>},
 * where the color is the deployment that wrote it, and commits on checkpoints. The gate is injected
 * in front of the sink by the operator, reading the event time through {@code
 * bluegreen.gate.watermark.field-path: eventTime}.
 */
public class BlueGreenAdvancedExample {

    /** The event time of record 0, fixed so that every run produces the same records. */
    static final long BASE_TIME_MS = 1_700_000_000_000L;

    /** The event time between two consecutive record ids. */
    static final long STEP_MS = 100;

    /** The watermark bound. */
    static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(2);

    /** How far behind the sequence the out of order records are, within the watermark bound. */
    static final long OUT_OF_ORDER_MS = 1_000;

    /** How far behind the sequence the late records are, past the watermark bound. */
    static final long LATE_MS = 120_000;

    /** Set by the operator on each deployment: BLUE or GREEN. */
    static final ConfigOption<String> ACTIVE_DEPLOYMENT_TYPE =
            ConfigOptions.key("bluegreen.active-deployment-type").stringType().defaultValue("NONE");

    public static void main(String[] args) throws Exception {
        String output = arg(args, "--output", "/opt/flink/volume/output");
        long recordsPerSecond = Long.parseLong(arg(args, "--records-per-second", "50"));

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        String color = env.getConfiguration().get(ACTIVE_DEPLOYMENT_TYPE);

        DataGeneratorSource<Event> source =
                new DataGeneratorSource<>(
                        Event::of,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(recordsPerSecond),
                        TypeInformation.of(Event.class));

        WatermarkStrategy<Event> watermarks =
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                        .withTimestampAssigner((event, timestamp) -> event.eventTime);

        FileSink<Event> sink =
                FileSink.forRowFormat(new Path(output), new LineEncoder(color))
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .build();

        // One source subtask keeps the ids a single sequence; the rebalance spreads them over
        // several gate subtasks, all of which must hand over before the old deployment goes.
        env.fromSource(source, watermarks, "Events")
                .setParallelism(1)
                .uid("events")
                .rebalance()
                .sinkTo(sink)
                .uid("output");

        env.execute("Blue/Green Advanced Example");
    }

    private static String arg(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    /** A record: a sequence id and its event time. */
    public static class Event {
        public long id;
        public long eventTime;

        public Event() {}

        static Event of(long id) {
            Event event = new Event();
            event.id = id;
            event.eventTime = BASE_TIME_MS + id * STEP_MS;
            if (id % 100 == 50) {
                event.eventTime -= LATE_MS;
            } else if (id % 10 == 5) {
                event.eventTime -= OUT_OF_ORDER_MS;
            }
            return event;
        }
    }

    /** Writes {@code <color>,<id>,<eventTime>}. */
    static class LineEncoder implements Encoder<Event> {
        private final String color;

        LineEncoder(String color) {
            this.color = color;
        }

        @Override
        public void encode(Event event, OutputStream stream) throws IOException {
            stream.write(
                    (color + "," + event.id + "," + event.eventTime + "\n")
                            .getBytes(StandardCharsets.UTF_8));
        }
    }
}
