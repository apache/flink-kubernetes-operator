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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/** Autoscaling Example. */
public class AutoscalingExample {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        long numIterations = Long.parseLong(args[0]);
        DataStream<Long> stream =
                env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE).filter(i -> System.nanoTime() > 1);
        stream =
                stream.shuffle()
                        .map(
                                new RichMapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long i) throws Exception {
                                        long end = 0;
                                        for (int j = 0; j < numIterations; j++) {
                                            end = System.nanoTime();
                                        }
                                        return end;
                                    }
                                });
        stream.addSink(
                new SinkFunction<Long>() {
                    @Override
                    public void invoke(Long value, Context context) throws Exception {
                        // Do nothing
                    }
                });
        env.execute("Autoscaling Example");
    }
}
