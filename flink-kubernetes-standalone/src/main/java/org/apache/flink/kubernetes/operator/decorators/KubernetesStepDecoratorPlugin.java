package org.apache.flink.kubernetes.operator.decorators;

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;

/** KubernetesStepDecorator Plugin Interface. */
public interface KubernetesStepDecoratorPlugin extends KubernetesStepDecorator, Plugin {

    /** Flink Component to be decorated. */
    enum DecoratorComponent {
        JOB_MANAGER,
        TASK_MANAGER
    }

    default DecoratorComponent getDecoratorComponent() {
        return DecoratorComponent.JOB_MANAGER;
    }
}
