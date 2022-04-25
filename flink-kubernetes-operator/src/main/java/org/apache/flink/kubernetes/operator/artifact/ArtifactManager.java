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

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

/** Manage the user artifacts. */
public class ArtifactManager {

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactManager.class);
    private final FlinkConfigManager configManager;

    public ArtifactManager(FlinkConfigManager configManager) {
        this.configManager = configManager;
    }

    private synchronized void createIfNotExists(File targetDir) {
        if (!targetDir.exists()) {
            try {
                FileUtils.forceMkdirParent(targetDir);
                LOG.info("Created dir: {}", targetDir);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format("Failed to create the dir: %s", targetDir), e);
            }
        }
    }

    public File fetch(String jarURI, String targetDirStr) throws Exception {
        File targetDir = new File(targetDirStr);
        createIfNotExists(targetDir);
        URI uri = new URI(jarURI);
        if ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) {
            return HttpArtifactFetcher.INSTANCE.fetch(jarURI, targetDir);
        } else {
            return FileSystemBasedArtifactFetcher.INSTANCE.fetch(jarURI, targetDir);
        }
    }

    public String generateJarDir(FlinkSessionJob sessionJob) {
        return String.join(
                File.separator,
                new String[] {
                    new File(configManager.getOperatorConfiguration().getArtifactsBaseDir())
                            .getAbsolutePath(),
                    sessionJob.getMetadata().getNamespace(),
                    sessionJob.getSpec().getDeploymentName(),
                    sessionJob.getMetadata().getName()
                });
    }
}
