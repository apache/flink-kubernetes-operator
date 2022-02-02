package org.apache.flink.kubernetes.operator.crd.spec;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/** Spec that describes a Flink application deployment. */
@Data
@NoArgsConstructor
public class FlinkDeploymentSpec {
    private String image;
    private String imagePullPolicy;
    private String flinkVersion;
    private String ingressDomain;
    private Map<String, String> flinkConfiguration;
    private Pod podTemplate;
    private JobManagerSpec jobManager;
    private TaskManagerSpec taskManager;
    private JobSpec job;
    private Map<String, String> logging;
}
