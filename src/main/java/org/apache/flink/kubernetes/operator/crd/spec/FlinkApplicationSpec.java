package org.apache.flink.kubernetes.operator.crd.spec;

import io.fabric8.kubernetes.api.model.KubernetesResource;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
public class FlinkApplicationSpec {
    private String imageName;
    private String imagePullPolicy;

    private String jarURI;
    private String[] mainArgs = new String[0];
    private String entryClass;

    private int parallelism;

    private Resource jobManagerResource;
    private Resource taskManagerResource;

    private String fromSavepoint;
    private boolean allowNonRestoredState = false;
    private String savepointsDir;
    private int savepointGeneration;

    private Map<String, String> flinkConfig;
}
