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

package org.apache.flink.autoscaler.event;

import org.apache.flink.autoscaler.JobAutoScalerContext;

import lombok.Getter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Testing {@link AutoScalerEventHandler} implementation. */
public class TestingEventCollector<KEY, Context extends JobAutoScalerContext<KEY>>
        implements AutoScalerEventHandler<KEY, Context> {

    public final LinkedList<Event<KEY, Context>> events = new LinkedList<>();

    public final Map<String, Event<KEY, Context>> eventMap = new ConcurrentHashMap<>();

    @Override
    public void handleEvent(
            Context context,
            Type type,
            String reason,
            String message,
            @Nullable String messageKey,
            @Nullable Duration interval) {
        String eventKey =
                generateEventKey(context, type, reason, messageKey != null ? messageKey : message);
        Event<KEY, Context> event = eventMap.get(eventKey);
        if (event == null) {
            Event<KEY, Context> newEvent = new Event<>(context, type, reason, message, messageKey);
            events.add(newEvent);
            eventMap.put(eventKey, newEvent);
            return;
        }
        if (Objects.equals(event.getMessage(), message)
                && interval != null
                && Instant.now()
                        .isBefore(event.getLastUpdateTimestamp().plusMillis(interval.toMillis()))) {
            // The event should be ignored.
            return;
        }
        event.incrementCount();
        events.add(event);
    }

    private String generateEventKey(Context context, Type type, String reason, String message) {
        return context.getJobID() + type.name() + reason + message;
    }

    /** The collected event. */
    public static class Event<KEY, Context extends JobAutoScalerContext<KEY>> {

        @Getter private Instant lastUpdateTimestamp;

        @Getter private final Context context;

        @Getter private final Type type;

        @Getter private final String reason;

        @Getter private final String message;

        @Getter @Nullable private final String messageKey;

        @Getter private int count;

        public Event(
                Context context,
                Type type,
                String reason,
                String message,
                @Nullable String messageKey) {
            this.lastUpdateTimestamp = Instant.now();
            this.context = context;
            this.type = type;
            this.reason = reason;
            this.message = message;
            this.messageKey = messageKey;
            this.count = 1;
        }

        private void incrementCount() {
            count++;
            lastUpdateTimestamp = Instant.now();
        }
    }
}
