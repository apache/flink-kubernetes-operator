package org.apache.flink.autoscaler.standalone;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneAutoscalerValidatorTest {
    @Test
    public void testAutoScalerWithInvalidConfig() {
        var jobList = new ArrayList<JobAutoScalerContext<JobID>>();
        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();

        Configuration correctConfiguration = new Configuration();
        correctConfiguration.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        Configuration invalidConfiguration = new Configuration();
        invalidConfiguration.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        invalidConfiguration.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, -1.);

        var correctConfigurationJob = createJobAutoScalerContextWithConf(correctConfiguration);
        var illegalConfigurationJob = createJobAutoScalerContextWithConf(invalidConfiguration);
        var scaleCounter = new ConcurrentHashMap<JobID, Integer>();

        try (var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        new Configuration(),
                        baseConf -> jobList,
                        eventCollector,
                        new JobAutoScaler<>() {
                            @Override
                            public void scale(JobAutoScalerContext<JobID> context) {
                                scaleCounter.put(
                                        context.getJobKey(),
                                        scaleCounter.getOrDefault(context.getJobKey(), 0) + 1);
                            }

                            @Override
                            public void cleanup(JobAutoScalerContext<JobID> context) {
                                // do nothing
                            }
                        })) {
            jobList.add(correctConfigurationJob);
            jobList.add(illegalConfigurationJob);
            List<CompletableFuture<Void>> scaledFutures = autoscalerExecutor.scaling();

            assertThat(scaledFutures).hasSize(2);
            assertThat(scaleCounter).size().isEqualTo(1);

            assertThat(eventCollector.events).size().isEqualTo(1);
            assertThat(eventCollector.events)
                    .allMatch(event -> event.getContext().equals(illegalConfigurationJob));
        }
    }

    private static JobAutoScalerContext<JobID> createJobAutoScalerContextWithConf(
            Configuration configuration) {
        var jobID = new JobID();
        return new JobAutoScalerContext<>(
                jobID, jobID, JobStatus.RUNNING, configuration, null, null);
    }
}
