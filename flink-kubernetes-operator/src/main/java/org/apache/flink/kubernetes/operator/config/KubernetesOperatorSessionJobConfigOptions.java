package org.apache.flink.kubernetes.operator.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** List supported session job specific configurations */
public enum KubernetesOperatorSessionJobConfigOptions {

    SESSION_JOB_HTTP_JAR_HEADERS("kubernetes.operator.user.artifacts.http.header");

    private final String sessionJobConfigurationKey;

    KubernetesOperatorSessionJobConfigOptions(String sessionJobConfigurationKey) {
        this.sessionJobConfigurationKey = sessionJobConfigurationKey;
    }

    public String getSessionJobConfigKey() {
        return sessionJobConfigurationKey;
    }

    public static KubernetesOperatorSessionJobConfigOptions validateSessionConfig(String sessionJobConfigKey) {
        return Arrays.stream(KubernetesOperatorSessionJobConfigOptions.values())
                .filter(sessionJobSupportedConfigs -> sessionJobConfigKey
                        .equalsIgnoreCase(sessionJobSupportedConfigs.getSessionJobConfigKey()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid Session Job Configuration: " + sessionJobConfigKey));
    }

    /** Parse key value k1:v1,k2,v2 comma separated key/value string into Map. */
    public static Map<String, String> parseKeyValuePair(String keyValuePairs) {
        Map<String, String> map = new HashMap<>();
        String[] keyValuePairArray = keyValuePairs.trim().split(",");
        for (int i = 0; i < keyValuePairArray.length; i++) {
            String[] keyValuePair = keyValuePairArray[i].split(":");
            if (keyValuePair.length == 2) {
                map.put(keyValuePair[0].trim(), keyValuePair[1].trim());
            } else {
                throw new IllegalArgumentException("Invalid KV pairs: " + keyValuePairs + ". Expect format: k1:v1,k2:v2 .");
            }
        }
        return map;
    }
}
