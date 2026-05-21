package org.apache.flink.runtime.rest.messages.job.metrics;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;

class AggregatedMetricTest {

    @Test
    void testToStringContainsMinFieldName() {
        final AggregatedMetric aggregatedMetric = new AggregatedMetric("id", 1.0, 2.0, 1.5, 4.0, 0.0);
        assertThat(aggregatedMetric.toString()).contains("min=");
    }

}