package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetricHistory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The `ScalingMetricJsonSender` class is responsible for converting collected metrics to JSON
 * format and sending them as an HTTP POST request to a specified endpoint. It uses the Jackson
 * library for JSON serialization and the SLF4J framework for logging.
 */
public class ScalingMetricJsonSender {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetricJsonSender.class);

    private static String endpoint = "http://hello-python-service.default.svc.cluster.local:6000";
    private static String endpointForGettingParalelisms =
            "http://hello-python-service.default.svc.cluster.local:6000/paralelisms\n";

    /**
     * Constructs a `ScalingMetricJsonSender` instance with a specified endpoint.
     *
     * @param endpoint The endpoint to which metrics will be sent.
     */
    public ScalingMetricJsonSender(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Converts a `CollectedMetricHistory` object to JSON format. For testing purposes, this method
     * is made public and can be called statically.
     *
     * @param collectedMetricHistory The collected metrics to be converted to JSON.
     * @return A JSON representation of the collected metrics.
     */
    public static String convertMetricsToJson(CollectedMetricHistory collectedMetricHistory) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            // Create a map to hold the data for serialization
            Map<String, Object> jsonData = new HashMap<>();
            jsonData.put("jobTopology", collectedMetricHistory.getJobTopology());
            jsonData.put("metricHistory", collectedMetricHistory.getMetricHistory());

            // Serialize the map to JSON
            return objectMapper.writeValueAsString(jsonData);
        } catch (Exception e) {
            LOG.error("Error converting metrics to JSON", e);
            return "{}";
        }
    }

    /**
     * Sends metrics in JSON format as an HTTP POST request to the specified endpoint. For testing
     * purposes, this method is made public and can be called statically.
     *
     * @param collectedMetricHistory The collected metrics to be converted to JSON.
     */
    public static void sendMetricsAsJson(CollectedMetricHistory collectedMetricHistory) {
        try {
            URL url = new URL(endpoint);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input =
                        convertMetricsToJson(collectedMetricHistory)
                                .getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                // Successfully sent metrics
                LOG.info("Metrics sent successfully to {}", endpoint);
            } else {
                // Handle error response
                try (InputStream errorStream = connection.getErrorStream()) {
                    if (errorStream != null) {
                        String errorResponse =
                                new BufferedReader(new InputStreamReader(errorStream))
                                        .lines()
                                        .collect(Collectors.joining("\n"));
                        LOG.error(
                                "Failed to send metrics. HTTP Response Code: {}. Error response: {}",
                                responseCode,
                                errorResponse);
                    } else {
                        LOG.error("Failed to send metrics. HTTP Response Code: {}", responseCode);
                    }
                }
            }
        } catch (IOException e) {
            // Handle exception
            LOG.error("Error while sending metrics: {}", e.getMessage());
        }
    }

    /**
     * Retrieves data from the specified endpoint using the HTTP GET method and populates a HashMap
     * with the results.
     *
     * @return A HashMap containing key-value pairs representing the retrieved data.
     * @apiNote The function assumes that the data returned from the endpoint is in JSON format. The
     *     JSON response is parsed, and its contents are used to populate the HashMap. Adjust the
     *     parsing logic based on the actual format of the data returned by the endpoint.
     * @implNote This function uses the java.net.HttpURLConnection class to establish a connection
     *     to the specified endpoint. It handles both successful responses (HTTP 200 OK) and error
     *     responses.
     * @throws IOException If an error occurs while connecting to the endpoint or processing the
     *     response.
     */
    public static HashMap<String, String> getDataFromEndpoint() {
        HashMap<String, String> dataMap = new HashMap<>();

        try {
            URL url = new URL(endpointForGettingParalelisms);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                // Successfully retrieved data
                try (InputStream inputStream = connection.getInputStream()) {
                    String responseData =
                            new BufferedReader(new InputStreamReader(inputStream))
                                    .lines()
                                    .collect(Collectors.joining("\n"));
                }
            } else {
                // Handle error response
                try (InputStream errorStream = connection.getErrorStream()) {
                    if (errorStream != null) {
                        String errorResponse =
                                new BufferedReader(new InputStreamReader(errorStream))
                                        .lines()
                                        .collect(Collectors.joining("\n"));
                        System.err.println(
                                "Failed to retrieve data. HTTP Response Code: "
                                        + responseCode
                                        + ". Error response: "
                                        + errorResponse);
                    } else {
                        System.err.println(
                                "Failed to retrieve data. HTTP Response Code: " + responseCode);
                    }
                }
            }
        } catch (IOException e) {
            // Handle exception
            System.err.println("Error while retrieving data: " + e.getMessage());
        }

        return dataMap;
    }
}
