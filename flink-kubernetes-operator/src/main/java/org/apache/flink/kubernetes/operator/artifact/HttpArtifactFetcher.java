/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.artifact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/** Download the jar from the http resource. */
public class HttpArtifactFetcher implements ArtifactFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(HttpArtifactFetcher.class);
    public static final HttpArtifactFetcher INSTANCE = new HttpArtifactFetcher();

    @Override
    public File fetch(
            String uri,
            Configuration clusterLevelConfiguration,
            Map<String, String> flinkConfiguration,
            File targetDir)
            throws Exception {
        var start = System.currentTimeMillis();
        URL url = new URL(uri);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        Map<String, String> clusterLevelHeader =
                clusterLevelConfiguration.get(
                        KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER);
        Map<String, String> jobLevelHeader =
                parseKeyValuePair(
                        flinkConfiguration.get(
                                KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER.key()));

        // jobLevelHeader will take precedence
        if (jobLevelHeader.size() > 0) {
            jobLevelHeader.forEach(conn::setRequestProperty);
        } else if (clusterLevelHeader != null && clusterLevelHeader.size() > 0) {
            clusterLevelHeader.forEach(conn::setRequestProperty);
        }

        conn.setRequestMethod("GET");

        String fileName = FilenameUtils.getName(url.getFile());
        File targetFile = new File(targetDir, fileName);
        try (var inputStream = conn.getInputStream()) {
            FileUtils.copyToFile(inputStream, targetFile);
        }
        LOG.debug(
                "Copied file from {} to {}, cost {} ms",
                uri,
                targetFile,
                System.currentTimeMillis() - start);
        return targetFile;
    }

    /** Parse key value k1:v1,k2,v2 comma separated key/value string into Map. */
    private Map<String, String> parseKeyValuePair(String keyValuePairs) {
        Map<String, String> map = new HashMap<>();

        if (keyValuePairs == null || keyValuePairs.isEmpty()) {
            return map;
        }

        String[] keyValuePairArray = keyValuePairs.trim().split(",");
        for (String s : keyValuePairArray) {
            String[] keyValuePair = s.split(":");
            if (keyValuePair.length == 2) {
                map.put(keyValuePair[0].trim(), keyValuePair[1].trim());
            } else {
                throw new IllegalArgumentException(
                        "Invalid KV pairs: " + keyValuePairs + ". Expect format: k1:v1,k2:v2 .");
            }
        }
        return map;
    }
}
