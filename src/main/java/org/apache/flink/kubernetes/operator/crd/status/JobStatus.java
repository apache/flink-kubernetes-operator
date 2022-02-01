package org.apache.flink.kubernetes.operator.crd.status;

import lombok.Data;
import lombok.NoArgsConstructor;

/** Status of an individual job within the Flink deployment. */
@Data
@NoArgsConstructor
public class JobStatus {
    private String jobName;
    private String jobId;
    private String state;
    private String updateTime;
    private String savepointLocation;
}
