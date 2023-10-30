// package org.apache.flink.kubernetes.operator.autoscaler;
//
// import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetricHistory;
// import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetrics;
// import org.apache.flink.kubernetes.operator.autoscaler.metrics.Edge;
// import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
// import org.apache.flink.kubernetes.operator.autoscaler.topology.VertexInfo;
// import org.apache.flink.runtime.jobgraph.JobVertexID;
//
// import org.junit.jupiter.api.Test;
//
// import java.io.IOException;
// import java.net.HttpURLConnection;
// import java.net.URL;
// import java.time.Instant;
// import java.util.Collections;
// import java.util.Map;
// import java.util.Set;
// import java.util.TreeMap;
//
// import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.LAG;
// import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.LOAD;
// import static
// org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.SOURCE_DATA_RATE;
// import static
// org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;
// import static org.mockito.Mockito.mock;
// import static org.mockito.Mockito.verify;
// import static org.mockito.Mockito.when;
//
// public class HttpPostMockitoTest {
//
//    @Test
//    public void testSendMetricsAsJson() throws IOException {
//        var source = new JobVertexID();
//        var sink = new JobVertexID();
//        var topology =
//                new JobTopology(
//                        new VertexInfo(source, Collections.emptySet(), 1, 1),
//                        new VertexInfo(sink, Set.of(source), 1, 1));
//
//        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
//
//        metricHistory.put(
//                Instant.now(),
//                new CollectedMetrics(
//                        Map.of(
//                                source,
//                                Map.of(
//                                        SOURCE_DATA_RATE,
//                                        100.,
//                                        LAG,
//                                        0.,
//                                        TRUE_PROCESSING_RATE,
//                                        200.,
//                                        LOAD,
//                                        .8),
//                                sink,
//                                Map.of(TRUE_PROCESSING_RATE, 2000., LOAD, .4)),
//                        Map.of(new Edge(source, sink), 2.)));
//
//        // Mock the URL and HttpURLConnection classes
//        URL url = mock(URL.class);
//        HttpURLConnection connection = mock(HttpURLConnection.class);
//
//        // Create a CollectedMetricHistory object
//        CollectedMetricHistory collectedMetricHistory =
//                new CollectedMetricHistory(topology, metricHistory);
//
//        // Mock the HttpURLConnection's methods
//        when(connection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
//
//        // Call the sendMetricsAsJson() function
//        ScalingMetricJsonSender.sendMetricsAsJson(collectedMetricHistory);
//
//        // Verify that the HttpURLConnection's responseCode is HttpURLConnection.HTTP_OK
//        verify(connection).getResponseCode();
//
//        // Verify that the sendMetricsAsJson() function logs a success message
//        //        verify(LOG).info("Metrics sent successfully to {}", url);
//    }
// }
