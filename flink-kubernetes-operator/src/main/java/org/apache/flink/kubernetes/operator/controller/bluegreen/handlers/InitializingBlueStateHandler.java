package org.apache.flink.kubernetes.operator.controller.bluegreen.handlers;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

/** State handler for the INITIALIZING_BLUE state. */
public class InitializingBlueStateHandler extends AbstractBlueGreenStateHandler {

    public InitializingBlueStateHandler(BlueGreenDeploymentService deploymentService) {
        super(FlinkBlueGreenDeploymentState.INITIALIZING_BLUE, deploymentService);
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context) {
        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();

        // We only allow a deployment if it's indeed the first (null last spec)
        // or if we're recovering (failing status) and the spec has changed
        if (deploymentStatus.getLastReconciledSpec() == null
                || (deploymentStatus.getJobStatus().getState().equals(JobStatus.FAILING)
                        && BlueGreenSpecUtils.hasSpecChanged(context))) {

            BlueGreenSpecUtils.setLastReconciledSpec(context);
            return deploymentService.initiateDeployment(
                    context,
                    DeploymentType.BLUE,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    null,
                    true);
        } else {
            getLogger()
                    .warn(
                            "Ignoring initial deployment. Last Reconciled Spec null: {}. BG Status: {}.",
                            deploymentStatus.getLastReconciledSpec() == null,
                            deploymentStatus.getJobStatus().getState());
            return UpdateControl.noUpdate();
        }
    }
}
