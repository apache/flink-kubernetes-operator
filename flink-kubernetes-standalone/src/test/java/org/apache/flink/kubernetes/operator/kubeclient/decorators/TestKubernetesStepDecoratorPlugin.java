package org.apache.flink.kubernetes.operator.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.operator.decorators.KubernetesStepDecoratorPlugin;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** KubernetesStepDecoratorPlugin mock for tests. */
public class TestKubernetesStepDecoratorPlugin implements KubernetesStepDecoratorPlugin {
    @Override
    public DecoratorComponent getDecoratorComponent() {
        return DecoratorComponent.JOB_MANAGER;
    }

    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        return flinkPod;
    }

    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        return Collections.emptyList();
    }
}
