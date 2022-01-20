package org.apache.flink.kubernetes.operator.Utils;

import org.apache.flink.client.deployment.StandaloneClientFactory;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkApplicationSpec;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.util.StringUtils;

import java.net.URI;
import java.util.Collections;

public class FlinkUtils {

	public static Configuration getEffectiveConfig(String namespace, String clusterId, FlinkApplicationSpec spec) throws Exception {
		final String flinkConfDir = System.getenv().get(ConfigConstants.ENV_FLINK_CONF_DIR);
		final Configuration effectiveConfig;
		if (flinkConfDir != null) {
			effectiveConfig = GlobalConfiguration.loadConfiguration(flinkConfDir);
		} else {
			effectiveConfig = new Configuration();
		}

		// Basic config options
		final URI uri = new URI(spec.getJarURI());
		effectiveConfig.setString(KubernetesConfigOptions.NAMESPACE, namespace);
		effectiveConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
		effectiveConfig.set(DeploymentOptions.TARGET, Constants.KUBERNETES_APP_TARGET);
		// Set rest service exposed type to clusterIP since we will use ingress to access the webui
		effectiveConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, KubernetesConfigOptions.ServiceExposedType.ClusterIP);

		// Image
		if (!StringUtils.isNullOrWhitespaceOnly(spec.getImageName())) {
			effectiveConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, spec.getImageName());
		}
		if (!StringUtils.isNullOrWhitespaceOnly(spec.getImagePullPolicy())) {
			effectiveConfig.set(
				KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
				KubernetesConfigOptions.ImagePullPolicy.valueOf(spec.getImagePullPolicy()));
		}

		// Jars
		effectiveConfig.set(PipelineOptions.JARS, Collections.singletonList(uri.toString()));

		// Parallelism and Resource
		if (spec.getParallelism() > 0) {
			effectiveConfig.set(CoreOptions.DEFAULT_PARALLELISM, spec.getParallelism());
		}
		if (spec.getJobManagerResource() != null) {
			effectiveConfig.setString(JobManagerOptions.TOTAL_PROCESS_MEMORY.key(), spec.getJobManagerResource().getMem());
			effectiveConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, spec.getJobManagerResource().getCpu());
		}
		if (spec.getTaskManagerResource() != null) {
			effectiveConfig.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), spec.getTaskManagerResource().getMem());
			effectiveConfig.set(KubernetesConfigOptions.TASK_MANAGER_CPU, spec.getTaskManagerResource().getCpu());
		}

		// Savepoint
		if (!StringUtils.isNullOrWhitespaceOnly(spec.getFromSavepoint())) {
			effectiveConfig.setString(SavepointConfigOptions.SAVEPOINT_PATH, spec.getFromSavepoint());
			effectiveConfig.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, spec.isAllowNonRestoredState());
		}
		if (!StringUtils.isNullOrWhitespaceOnly(spec.getSavepointsDir())) {
			effectiveConfig.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, spec.getSavepointsDir());
		}

		// Dynamic configuration
		if (spec.getFlinkConfig() != null && !spec.getFlinkConfig().isEmpty()) {
			spec.getFlinkConfig().forEach(effectiveConfig::setString);
		}

		return effectiveConfig;
	}


	public static ClusterClient<String> getRestClusterClient(Configuration config) throws Exception {
		final String clusterId = config.get(KubernetesConfigOptions.CLUSTER_ID);
		final String namespace = config.get(KubernetesConfigOptions.NAMESPACE);
		final int port = config.getInteger(RestOptions.PORT);
		final String restServerAddress = String.format("http://%s-rest.%s:%s", clusterId, namespace, port);
		return new RestClusterClient<>(
			config,
			clusterId,
			(c,e) -> new StandaloneClientHAServices(restServerAddress));
	}
}
