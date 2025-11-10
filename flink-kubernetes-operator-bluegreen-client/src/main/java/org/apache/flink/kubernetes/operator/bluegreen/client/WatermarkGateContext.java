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

import org.apache.flink.kubernetes.operator.api.bluegreen.GateContext;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/** Watermark based functionality of the GateContext. */
@Getter
public class WatermarkGateContext implements Serializable {

    static final String WATERMARK_TOGGLE_VALUE = "watermark-toggle-value";
    static final String WATERMARK_STAGE = "watermark-stage";

    private final GateContext baseContext;
    private final WatermarkGateStage watermarkGateStage;

    @Setter private Long watermarkToggleValue;

    public WatermarkGateContext(
            GateContext baseContext,
            WatermarkGateStage watermarkGateStage,
            Long watermarkToggleValue) {
        this.baseContext = baseContext;
        this.watermarkGateStage = watermarkGateStage;
        this.watermarkToggleValue = watermarkToggleValue;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WatermarkGateContext) {
            WatermarkGateContext wm = (WatermarkGateContext) o;
            return this.baseContext.equals(wm.getBaseContext())
                    && this.watermarkGateStage.equals(wm.getWatermarkGateStage())
                    && this.watermarkToggleValue.equals(wm.getWatermarkToggleValue());
        }
        return false;
    }

    public static WatermarkGateContext create(GateContext baseContext, Map<String, String> data) {
        // Possible values:
        //  null -> indetermined/to be calculated by the active job
        //  0 -> For first deployments (undetermined)
        //  Positive -> For valid transitions
        Long watermarkToggleValue = null;

        if (baseContext.isFirstDeployment()) {
            watermarkToggleValue = 0L;
        } else if (data.containsKey(WATERMARK_TOGGLE_VALUE)) {
            watermarkToggleValue = Long.parseLong(data.get(WATERMARK_TOGGLE_VALUE));
        }

        var watermarkState =
                data.getOrDefault(WATERMARK_STAGE, WatermarkGateStage.WATERMARK_NOT_SET.toString());

        return new WatermarkGateContext(
                baseContext, WatermarkGateStage.valueOf(watermarkState), watermarkToggleValue);
    }

    @Override
    public String toString() {
        return "WatermarkGateContext: {"
                + ", watermarkToggleValue: "
                + watermarkToggleValue
                + ", watermarkGateStage: "
                + watermarkGateStage
                + ", gateStage:"
                + baseContext.getGateStage()
                + ", deploymentTeardownDelaySec: "
                + baseContext.getDeploymentTeardownDelayMs()
                + ", outputMode: "
                + baseContext.getOutputMode()
                + ", activeDeploymentType: "
                + baseContext.getActiveBlueGreenDeploymentType()
                + "}";
    }
}
