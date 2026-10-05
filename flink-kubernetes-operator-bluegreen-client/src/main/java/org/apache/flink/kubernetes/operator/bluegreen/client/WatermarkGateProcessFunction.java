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

package org.apache.flink.kubernetes.operator.bluegreen.client;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContext;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateMetrics;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

/** Watermark based GateProcessFunction (streaming). */
public class WatermarkGateProcessFunction<I> extends GateProcessFunction<I>
        implements Serializable {

    // A standby subtask without a record for this long since it learned the toggle owes nothing to
    // the hand-over: there is nothing it could still emit before its watermark passes the toggle.
    @VisibleForTesting static final long IDLE_DONE_MS = 30_000L;

    private final Function<I, Long> watermarkExtractor;

    private WatermarkGateContext currentWatermarkGateContext;

    // Hand-over progress, read by the HAND_OVER_DONE gauge from the metric reporter's thread
    private transient volatile boolean pastToggle;
    private transient volatile long toggleKnownSince = -1L;
    private transient volatile long lastRecordAt = -1L;

    WatermarkGateProcessFunction(
            BlueGreenDeploymentType blueGreenDeploymentType,
            String namespace,
            String configMapName,
            Function<I, Long> watermarkExtractor) {
        super(blueGreenDeploymentType, namespace, configMapName);

        Preconditions.checkNotNull(watermarkExtractor);

        this.watermarkExtractor = watermarkExtractor;
    }

    public static <I> WatermarkGateProcessFunction<I> create(
            Map<String, String> flinkConfig, Function<I, Long> watermarkExtractor) {
        requireConfigKey(flinkConfig, "bluegreen.active-deployment-type");
        requireConfigKey(flinkConfig, "kubernetes.namespace");
        requireConfigKey(flinkConfig, "bluegreen.configmap.name");
        return new WatermarkGateProcessFunction<I>(
                BlueGreenDeploymentType.valueOf(
                        flinkConfig.get("bluegreen.active-deployment-type")),
                flinkConfig.get("kubernetes.namespace"),
                flinkConfig.get("bluegreen.configmap.name"),
                watermarkExtractor);
    }

    private static void requireConfigKey(Map<String, String> config, String key) {
        if (config.get(key) == null) {
            throw new IllegalArgumentException(
                    "WatermarkGateProcessFunction requires config key '"
                            + key
                            + "' to be set. If using automatic injection this is set by the"
                            + " operator; if instantiating directly, provide it in the"
                            + " flinkConfig map.");
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Deserialization does not run field initializers: set the progress before super.open()
        // reads the ConfigMap for the first time
        pastToggle = false;
        toggleKnownSince = -1L;
        lastRecordAt = -1L;
        super.open(openContext);
        getRuntimeContext()
                .getMetricGroup()
                .gauge(
                        GateMetrics.HAND_OVER_DONE,
                        (Gauge<Integer>) () -> isHandOverDone(System.currentTimeMillis()) ? 1 : 0);
    }

    /**
     * Whether this subtask, as the standby, has nothing left to emit for the hand-over: its
     * watermark has passed the toggle, or it has had no record for {@link #IDLE_DONE_MS} since it
     * learned the toggle. A subtask that receives no records never sees its watermark pass the
     * toggle, but it owes nothing either.
     */
    @VisibleForTesting
    boolean isHandOverDone(long now) {
        if (pastToggle) {
            return true;
        }
        long since = toggleKnownSince;
        return since >= 0 && now - Math.max(since, lastRecordAt) >= IDLE_DONE_MS;
    }

    @Override
    protected void onContextUpdate(GateContext baseContext, Map<String, String> data) {
        var fetchedWatermarkContext = WatermarkGateContext.create(baseContext, data);
        logInfo("Refreshing WatermarkGateContext with data: " + data);

        if (currentWatermarkGateContext == null) {
            logInfo("currentWatermarkGateContext INITIALIZED: " + fetchedWatermarkContext);
            currentWatermarkGateContext = fetchedWatermarkContext;
        } else if (!currentWatermarkGateContext.equals(fetchedWatermarkContext)) {
            logInfo("currentWatermarkGateContext UPDATED: " + fetchedWatermarkContext);
            currentWatermarkGateContext = fetchedWatermarkContext;
        }
        trackToggle(currentWatermarkGateContext.getWatermarkToggleValue());
    }

    /** A new transition starts without a toggle, which resets the hand-over progress. */
    private void trackToggle(Long toggle) {
        if (toggle == null) {
            pastToggle = false;
            toggleKnownSince = -1L;
        } else if (toggleKnownSince < 0) {
            toggleKnownSince = System.currentTimeMillis();
        }
    }

    @Override
    protected void processElementActive(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException {
        Long wmToggleValue = currentWatermarkGateContext.getWatermarkToggleValue();
        if (wmToggleValue != null) {
            if (isFullyOpen(ctx.timerService(), wmToggleValue)) {
                out.collect(value);
                return;
            }
            long extractedWatermark = timestampOf(value);
            if (wmToggleValue <= extractedWatermark) {
                // Normal
                out.collect(value);
            } else {
                // Waiting for WM
                logInfo(
                        " -- Waiting to Reach WM: "
                                + (wmToggleValue - extractedWatermark)
                                + " ms - ");
            }
        } else {
            // Transitioning to Active
            var currentGateStage = currentWatermarkGateContext.getBaseContext().getGateStage();
            if (currentGateStage == TransitionStage.TRANSITIONING) {
                logInfo(" -- Waiting for WM to be set - ");
                notifyWaitingForWatermark(ctx);
            } else {
                logInfo("Waiting for the TRANSITIONING state, current: " + currentGateStage);
            }
        }
    }

    @Override
    protected void processElementStandby(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException {
        lastRecordAt = System.currentTimeMillis();
        if (currentWatermarkGateContext.getWatermarkToggleValue() != null) {
            var watermarkToggleValue = currentWatermarkGateContext.getWatermarkToggleValue();

            if (ctx.timerService().currentWatermark() <= watermarkToggleValue) {
                if (watermarkToggleValue > timestampOf(value)) {
                    // Should still output the element
                    out.collect(value);
                } else {
                    // Went past the Watermark toggle value: BLOCK ELEMENT
                    logInfo(" -- Past WM -- ");
                }
            } else {
                // Went past the Watermark Boundary: BLOCK ELEMENT
                logInfo(" -- Past WM Boundary -- ");
                pastToggle = true;
                notifyClearToTeardown(ctx);
            }
        } else {
            // This ACTIVE job is transitioning to STANDBY, output elements
            out.collect(value);
            // Set the watermark when the other new job is ready
            updateWatermarkInConfigMap(ctx);
        }
    }

    /**
     * The active gate passes every record when there is no standby deployment to share the stream
     * with (a first deployment, or the deployment kept after an abort), or once this subtask's own
     * watermark has passed the toggle. The standby's gate stops at that same point, so it never
     * emits the records that arrive after it, including late ones older than the toggle. Only the
     * record-time watermark counts: until the first one arrives, currentWatermark() is
     * Long.MIN_VALUE and records are compared with the toggle one by one.
     */
    private boolean isFullyOpen(TimerService timerService, long wmToggleValue) {
        return currentWatermarkGateContext.getBaseContext().isFirstDeployment()
                || timerService.currentWatermark() > wmToggleValue;
    }

    /**
     * The record's timestamp from the job's extractor. A record without one (null) counts as older
     * than any toggle, like a null field-path field or field-index column: the standby emits it
     * until it stops, the active once it is fully open.
     */
    private long timestampOf(I value) {
        Long timestamp = watermarkExtractor.apply(value);
        return timestamp == null ? Long.MIN_VALUE : timestamp;
    }

    /**
     * The toggle the standby proposes: its own record-time watermark plus the deletion delay, or
     * null while no watermark has reached this subtask. It is never taken from the wall clock.
     */
    @VisibleForTesting
    static Long nextWatermarkToggleValue(long currentWatermark, long deploymentTeardownDelayMs) {
        return currentWatermark > 0 ? currentWatermark + deploymentTeardownDelayMs : null;
    }

    protected void updateWatermarkInConfigMap(Context ctx) {
        scheduleWriteTimer(ctx);
    }

    protected void notifyWaitingForWatermark(Context ctx) {
        scheduleWriteTimer(ctx);
    }

    @Override
    protected boolean handleScheduledWrite(Context ctx) throws Exception {
        var wmCtx = currentWatermarkGateContext;

        // Standby job: Active job has signalled it's waiting — compute and write the WM toggle.
        if (wmCtx.getWatermarkGateStage() == WatermarkGateStage.WAITING_FOR_WATERMARK
                && wmCtx.getWatermarkToggleValue() == null) {
            Long nextWatermarkToggleValue =
                    nextWatermarkToggleValue(
                            ctx.timerService().currentWatermark(),
                            wmCtx.getBaseContext().getDeploymentTeardownDelayMs());
            if (nextWatermarkToggleValue == null) {
                // Retried on the next record, which reschedules this write. A job that never
                // produces watermarks never gets a toggle, and the transition hits the gate
                // timeout.
                logInfo("No watermark has reached this subtask yet, the toggle waits for one");
                return false;
            }
            // Every standby subtask may propose a toggle; the first one written wins and all of
            // them use the stored value, never their own proposal.
            var toggle =
                    compareAndSetCustomEntries(
                            WatermarkGateContext.WATERMARK_TOGGLE_VALUE,
                            null,
                            Map.of(
                                    WatermarkGateContext.WATERMARK_TOGGLE_VALUE,
                                            Long.toString(nextWatermarkToggleValue),
                                    WatermarkGateContext.WATERMARK_STAGE,
                                            WatermarkGateStage.WATERMARK_SET.toString()));
            toggle.ifPresent(
                    value ->
                            currentWatermarkGateContext.setWatermarkToggleValue(
                                    Long.parseLong(value)));
            trackToggle(currentWatermarkGateContext.getWatermarkToggleValue());
            logInfo("Watermark toggle value: " + toggle.orElse("none, another transition"));
            return true;
        }

        // Active job: signal that it is waiting for the Standby job to provide the WM toggle.
        if (wmCtx.getBaseContext().getGateStage() == TransitionStage.TRANSITIONING
                && wmCtx.getWatermarkToggleValue() == null
                && wmCtx.getWatermarkGateStage() != WatermarkGateStage.WAITING_FOR_WATERMARK) {
            // Only while no stage is set: a late subtask must not undo WATERMARK_SET
            var stage =
                    compareAndSetCustomEntries(
                            WatermarkGateContext.WATERMARK_STAGE,
                            null,
                            Map.of(
                                    WatermarkGateContext.WATERMARK_STAGE,
                                    WatermarkGateStage.WAITING_FOR_WATERMARK.toString()));
            logInfo("Watermark stage is " + stage.orElse("unset"));
            return true;
        }

        return false; // fall through to base-class CLEAR_TO_TEARDOWN handling
    }
}
