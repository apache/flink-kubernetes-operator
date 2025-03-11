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

package autoscaling;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Example pipeline which simulates fluctuating load from zero to a defined max, and vice-versa. The
 * goal is to simulate a traffic pattern which traverses all possible stages between zero load and
 * peak load. The load curve is computed using a sine function.
 *
 * <p>The pipeline has defaults but can be parameterized as follows:
 *
 * <pre>
 *  repeatsAfterMs:  The period length after which the initial load will be reached again.
 *  maxLoadPerTask:  Each task's max load is presented by a double which is similar to the Unix CPU load
 *                     in the sense that at least maxLoad amount of subtasks are needed to sustain the load.
 *                     For example, a max load of 1 represents 100% load on a single subtask, 50% load on two subtasks.
 *                     Similarly, a max load of 2 represents 100% load on two tasks, 50% load on 4 subtasks.
 *
 *                     Multiple tasks and branches can be defined to test Flink Autoscaling. The format is as follows:
 *                       maxLoadTask1Branch1[;maxLoadTask2Branch1...[\n maxLoadTask1Branch2[;maxLoadTask2Branch2...]...]
 *
 *                    A concrete example: "1;2;4\n4;2;1"
 *                      Two branches are created with three tasks each. On the first branch, the tasks have
 *                      a load of 1, 2, and 3 respectively. On the second branch, the tasks have the load reversed.
 *                      This means, that at peak Flink Autoscaling at target utilization of 0.5, the parallelisms of
 *                      the tasks will be 2, 4, 8 for branch one and vise-versa for branch two.
 * </pre>
 */
public class LoadSimulationPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(LoadSimulationPipeline.class);

    // Number of impulses (records) emitted per sampling interval.
    // This value determines how many records should be generated within each `samplingIntervalMs`
    // period.
    private static final int IMPULSES_PER_SAMPLING_INTERVAL = 10;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        var arguments = ParameterTool.fromArgs(args);
        String maxLoadPerTask =
                arguments.get("maxLoadPerTask", "1;2;4;8;16;\n16;8;4;2;1\n8;4;16;1;2");
        long repeatsAfterMs =
                Duration.ofMinutes(arguments.getLong("repeatsAfterMinutes", 60)).toMillis();
        int samplingIntervalMs = arguments.getInt("samplingIntervalMs", 1_000);

        for (String branch : maxLoadPerTask.split("\n")) {
            String[] taskLoads = branch.split(";");

            /*
             * Creates an unbounded stream that continuously emits the constant value 42L.
             * Flink's DataGeneratorSource with RateLimiterStrategy is used to control the emission rate.
             *
             * Emission Rate Logic:
             * - The goal is to generate a fixed number of impulses per sampling interval.
             * - `samplingIntervalMs` defines the duration of one sampling interval in milliseconds.
             * - We define `IMPULSES_PER_SAMPLING_INTERVAL = 10`, meaning that for every sampling interval,
             *   exactly 10 impulses should be generated.
             *
             * To calculate the total number of records emitted per second:
             * 1. Determine how many sampling intervals fit within one second:
             *      samplingIntervalsPerSecond = 1000 / samplingIntervalMs;
             * 2. Multiply this by the number of impulses per interval to get the total rate:
             *      impulsesPerSecond = IMPULSES_PER_SAMPLING_INTERVAL * samplingIntervalsPerSecond;
             *
             * Example:
             * - If `samplingIntervalMs = 500 ms` and `IMPULSES_PER_SAMPLING_INTERVAL = 10`:
             *      impulsesPerSecond = (1000 / 500) * 10 = 2 * 10 = 20 records per second.
             */
            DataStream<Long> stream =
                    env.fromSource(
                            new DataGeneratorSource<>(
                                    (GeneratorFunction<Long, Long>)
                                            (index) -> 42L, // Emits constant value 42
                                    Long.MAX_VALUE, // Unbounded stream
                                    RateLimiterStrategy.perSecond(
                                            (1000.0 / samplingIntervalMs)
                                                    * IMPULSES_PER_SAMPLING_INTERVAL), // Controls
                                    // rate
                                    Types.LONG),
                            WatermarkStrategy.noWatermarks(),
                            "ImpulseSource");

            for (String load : taskLoads) {
                double maxLoad = Double.parseDouble(load);
                stream =
                        stream.shuffle()
                                .flatMap(
                                        new LoadSimulationFn(
                                                maxLoad, repeatsAfterMs, samplingIntervalMs))
                                .name("MaxLoad: " + maxLoad)
                                .broadcast();
            }

            stream.addSink(new DiscardingSink<>());
        }

        env.execute(
                "Load Simulation (repeats after "
                        + Duration.of(repeatsAfterMs, ChronoUnit.MILLIS)
                        + ")");
    }

    private static class LoadSimulationFn extends RichFlatMapFunction<Long, Long> {

        private final double maxLoad;
        private final long repeatsAfterMs;
        private final int samplingIntervalMs;
        long lastEpoch;

        public LoadSimulationFn(double maxLoad, long repeatsAfterMs, int samplingIntervalMs) {
            this.maxLoad = maxLoad;
            this.repeatsAfterMs = repeatsAfterMs;
            this.samplingIntervalMs = samplingIntervalMs;
        }

        @Override
        public void flatMap(Long record, Collector<Long> out) throws Exception {
            long timeMillis = System.currentTimeMillis();
            long currentEpoch = timeMillis % repeatsAfterMs / samplingIntervalMs;

            out.collect(record);

            if (currentEpoch == lastEpoch) {
                return;
            }
            lastEpoch = currentEpoch;

            double amplitude = getAmplitude(currentEpoch);

            double loadPerSubTask = maxLoad / getRuntimeContext().getNumberOfParallelSubtasks();

            long busyTimeMs = (long) (loadPerSubTask * samplingIntervalMs * amplitude);
            long remainingTimeMs =
                    (timeMillis / samplingIntervalMs + 1) * samplingIntervalMs - timeMillis;
            long sleepTime = Math.min(busyTimeMs, remainingTimeMs);
            LOG.info(
                    "{}> epoch: {} busyTime: {} remainingTime: {} sleepTime: {} amplitude: {}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    currentEpoch,
                    busyTimeMs,
                    remainingTimeMs,
                    sleepTime,
                    amplitude);

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
        }

        private double getAmplitude(long currentEpoch) {
            // A sine function which moves between 0 and 1 for the duration of repeatsAfterMs.
            double amplitude =
                    Math.sin(
                                            currentEpoch
                                                    / ((double) repeatsAfterMs / samplingIntervalMs)
                                                    * 2
                                                    * Math.PI)
                                    * 0.5
                            + 0.5;
            amplitude = Math.max(0, amplitude);
            return amplitude;
        }
    }
}
