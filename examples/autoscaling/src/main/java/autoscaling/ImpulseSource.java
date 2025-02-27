/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package autoscaling;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ImpulseSource is a custom Flink Source that generates a continuous stream of constant values.
 * (42L)
 */
public class ImpulseSource implements Source<Long, SourceSplit, Void> {
    private final int samplingInterval;

    public ImpulseSource(int samplingInterval) {
        this.samplingInterval = samplingInterval;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Creates a new SourceReader instance that continuously emits values.
     *
     * @param context The reader context.
     * @return A new SourceReader that emits constant values.
     */
    @Override
    public SourceReader<Long, SourceSplit> createReader(SourceReaderContext context) {
        return new SourceReader<>() {
            private final int maxSleepTimeMs = samplingInterval / 10;
            private final AtomicBoolean running = new AtomicBoolean(true);

            @Override
            public void start() {}

            /**
             * Reads and emits the next record.
             *
             * @param output The output collector to emit records.
             * @return InputStatus.MORE_AVAILABLE to indicate that more data is available.
             */
            @Override
            public InputStatus pollNext(ReaderOutput<Long> output) {
                if (!running.get()) {
                    return InputStatus.END_OF_INPUT;
                }

                output.collect(42L);

                try {
                    Thread.sleep(maxSleepTimeMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return InputStatus.END_OF_INPUT;
                }

                return InputStatus.MORE_AVAILABLE;
            }

            @Override
            public List<SourceSplit> snapshotState(long l) {
                return Collections.emptyList();
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void addSplits(List<SourceSplit> list) {}

            @Override
            public void notifyNoMoreSplits() {}

            @Override
            public void close() {
                running.set(false);
            }
        };
    }

    /**
     * Creates a SplitEnumerator that assigns a single split to one reader. Since this source does
     * not use multiple splits, the split assignment is simple.
     */
    @Override
    public SplitEnumerator<SourceSplit, Void> createEnumerator(
            SplitEnumeratorContext<SourceSplit> context) {
        return new SplitEnumerator<>() {
            private boolean assigned = false;

            @Override
            public void start() {}

            @Override
            public void handleSplitRequest(int subtaskId, String requesterHostname) {
                if (!assigned) {
                    context.assignSplit(
                            new SourceSplit() {
                                @Override
                                public String splitId() {
                                    return "ImpulseSourceSplit";
                                }
                            },
                            subtaskId);
                    assigned = true;
                }
            }

            @Override
            public void addSplitsBack(java.util.List<SourceSplit> splits, int subtaskId) {}

            @Override
            public void addReader(int subtaskId) {}

            @Override
            public Void snapshotState(long checkpointId) {
                return null;
            }

            @Override
            public void close() {}
        };
    }

    /** Since this source has no state, it simply creates a new enumerator. */
    @Override
    public SplitEnumerator<SourceSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<SourceSplit> context, Void checkpoint) {
        return createEnumerator(context);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(SourceSplit split) {
                return new byte[0];
            }

            @Override
            public SourceSplit deserialize(int version, byte[] serialized) {
                return () -> "ImpulseSourceSplit";
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Void obj) {
                return new byte[0];
            }

            @Override
            public Void deserialize(int version, byte[] serialized) {
                return null;
            }
        };
    }
}
