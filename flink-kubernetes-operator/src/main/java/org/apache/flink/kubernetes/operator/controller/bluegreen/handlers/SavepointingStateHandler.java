package org.apache.flink.kubernetes.operator.controller.bluegreen.handlers;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService.patchStatusUpdateControl;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getReconciliationReschedInterval;

public class SavepointingStateHandler extends AbstractBlueGreenStateHandler {

    public SavepointingStateHandler(
            FlinkBlueGreenDeploymentState supportedState,
            BlueGreenDeploymentService deploymentService) {
        super(supportedState, deploymentService);
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context) {
        DeploymentType currentType = getCurrentDeploymentType();
        var isSavepointReady = deploymentService.monitorSavepoint(context, currentType);

        // Savepoint complete, continue with the transition
        if (isSavepointReady) {
            var nextState = getSupportedState() == FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE ?
                    FlinkBlueGreenDeploymentState.ACTIVE_BLUE :
                    FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
            return patchStatusUpdateControl(context, nextState, null)
                    .rescheduleAfter(500);
        }

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate()
                .rescheduleAfter(getReconciliationReschedInterval(context));
    }

    private DeploymentType getCurrentDeploymentType() {
        return getSupportedState() == FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE
                ? DeploymentType.BLUE
                : DeploymentType.GREEN;
    }
}
