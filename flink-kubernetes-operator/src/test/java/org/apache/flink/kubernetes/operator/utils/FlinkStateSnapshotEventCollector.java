/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.listener.AuditUtils;

import io.fabric8.kubernetes.api.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.function.BiConsumer;

/** Simple consumer that collects triggered savepoint events for tests. */
public class FlinkStateSnapshotEventCollector implements BiConsumer<FlinkStateSnapshot, Event> {
    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkStateSnapshotEventCollector.class);
    public final LinkedList<Event> events = new LinkedList<>();

    @Override
    public void accept(FlinkStateSnapshot savepoint, Event event) {
        LOG.info(AuditUtils.format(event, "Snapshot"));
        events.add(event);
    }
}
