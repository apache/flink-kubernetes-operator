package org.apache.flink.autoscaler.standalone;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_INTERVAL;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_PARALLELISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandaloneAutoscalerEntrypointTest {
    @Test
    public void testLoadConfiguration() {
        // Test for loading configuration from file.
        String confOverrideDir = "src/test/resources";
        Configuration conf =
                StandaloneAutoscalerEntrypoint.loadConfiguration(
                        Optional.of(confOverrideDir), new String[0]);
        assertNotNull(conf);
        assertEquals(Duration.ofMinutes(1), conf.get(CONTROL_LOOP_INTERVAL));
        assertEquals(20, conf.get(CONTROL_LOOP_PARALLELISM));
        // Test for args override
        String[] args =
                new String[] {
                    "--autoscaler.standalone.control-loop.interval",
                    "2min",
                    "--autoscaler" + ".standalone.control-loop.parallelism",
                    "10"
                };
        Configuration confOverride =
                StandaloneAutoscalerEntrypoint.loadConfiguration(
                        Optional.of(confOverrideDir), args);
        assertNotNull(confOverride);
        assertEquals(Duration.ofMinutes(2), confOverride.get(CONTROL_LOOP_INTERVAL));
        assertEquals(10, confOverride.get(CONTROL_LOOP_PARALLELISM));
    }
}
