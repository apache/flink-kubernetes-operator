package org.apache.flink.kubernetes.operator.api.status;

/** Condition Status of the Flink JobManager Kubernetes deployment. */
public enum JobManagerDeploymentConditionStatus {
    READY("True", "JobManagerReady", "JobManager is running and ready to receive REST API calls"),
    MISSING("False", "JobManagerDeploymentMissing", "JobManager deployment not found"),
    DEPLOYING("False", "JobManagerIsDeploying", "JobManager process is starting up"),
    DEPLOYED_NOT_READY(
            "False",
            "DeployedNotReady",
            "JobManager is running but not ready yet to receive REST API calls"),
    ERROR("False", "Error", "JobManager deployment failed");

    private String status;
    private String reason;
    private String message;

    JobManagerDeploymentConditionStatus(String status, String reason, String message) {
        this.status = status;
        this.reason = reason;
        this.message = message;
    }

    public String getReason() {
        return reason;
    }

    public String getMessage() {
        return message;
    }

    public String getStatus() {
        return status;
    }
}
