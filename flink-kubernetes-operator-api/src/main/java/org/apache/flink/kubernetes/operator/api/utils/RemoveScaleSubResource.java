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

package org.apache.flink.kubernetes.operator.api.utils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

/**
 * Utility to remove scale subresource from CRD. Required by the {@link
 * org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment}.
 */
public class RemoveScaleSubResource {

    public static void main(String[] args) throws Exception {
        var path = Paths.get(args[0]);
        var filtered =
                Files.readAllLines(path).stream()
                        .filter(line -> !line.trim().equals("scale:"))
                        .filter(line -> !line.trim().startsWith("specReplicasPath:"))
                        .collect(Collectors.toList());

        // Write back (overwrites file)
        Files.write(path, filtered);
    }
}
