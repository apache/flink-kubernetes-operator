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

package org.apache.flink.autoscaler.jdbc.state;

import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.autoscaler.jdbc.state.JobStateView.State.NEEDS_CREATE;
import static org.apache.flink.autoscaler.jdbc.state.JobStateView.State.NEEDS_DELETE;
import static org.apache.flink.autoscaler.jdbc.state.JobStateView.State.NEEDS_UPDATE;
import static org.apache.flink.autoscaler.jdbc.state.JobStateView.State.NOT_NEEDED;
import static org.apache.flink.autoscaler.jdbc.state.JobStateView.State.UP_TO_DATE;
import static org.apache.flink.util.Preconditions.checkState;

/** The view of job state. */
@NotThreadSafe
public class JobStateView {

    /**
     * The state of state type about the cache and database.
     *
     * <p>Note: {@link #inLocally} and {@link #inDatabase} are only for understand, we don't use
     * them.
     */
    @SuppressWarnings("unused")
    enum State {

        /** State doesn't exist at database, and it's not used so far, so it's not needed. */
        NOT_NEEDED(false, false, false),
        /** State is only stored locally, not created in JDBC database yet. */
        NEEDS_CREATE(true, false, true),
        /** State exists in JDBC database but there are newer local changes. */
        NEEDS_UPDATE(true, true, true),
        /** State is stored locally and in database, and they are same. */
        UP_TO_DATE(true, true, false),
        /** State is stored in database, but it's deleted in local. */
        NEEDS_DELETE(false, true, true);

        /** The data of this state type is stored in locally when it is true. */
        private final boolean inLocally;

        /** The data of this state type is stored in database when it is true. */
        private final boolean inDatabase;

        /** The data of this state type is stored in database when it is true. */
        private final boolean needFlush;

        State(boolean inLocally, boolean inDatabase, boolean needFlush) {
            this.inLocally = inLocally;
            this.inDatabase = inDatabase;
            this.needFlush = needFlush;
        }

        public boolean isNeedFlush() {
            return needFlush;
        }
    }

    /** Transition old state to the new state when some operations happen to cache or database. */
    private static class StateTransitioner {

        /** The transition when put data to cache. */
        @Nonnull
        public State putTransition(@Nonnull State oldState) {
            switch (oldState) {
                case NOT_NEEDED:
                case NEEDS_CREATE:
                    return NEEDS_CREATE;
                case NEEDS_UPDATE:
                case UP_TO_DATE:
                case NEEDS_DELETE:
                    return NEEDS_UPDATE;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unknown state : %s.", oldState));
            }
        }

        /** The transition when delete data from cache. */
        @Nonnull
        public State deleteTransition(@Nonnull State oldState) {
            switch (oldState) {
                case NOT_NEEDED:
                case NEEDS_CREATE:
                    return NOT_NEEDED;
                case NEEDS_UPDATE:
                case UP_TO_DATE:
                case NEEDS_DELETE:
                    return NEEDS_DELETE;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unknown state : %s.", oldState));
            }
        }

        /** The transition when flush data from cache to database. */
        @Nonnull
        public State flushTransition(@Nonnull State oldState) {
            switch (oldState) {
                case NOT_NEEDED:
                case NEEDS_DELETE:
                    return NOT_NEEDED;
                case NEEDS_CREATE:
                case NEEDS_UPDATE:
                case UP_TO_DATE:
                    return UP_TO_DATE;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unknown state : %s.", oldState));
            }
        }
    }

    private static final StateTransitioner STATE_TRANSITIONER = new StateTransitioner();

    private final JdbcStateInteractor jdbcStateInteractor;
    private final String jobKey;
    private final Map<StateType, String> data;

    /**
     * The state is maintained for each state type, which means that part of state types of current
     * job are stored in the database, but the rest of the state types may have been created in the
     * database.
     */
    private final Map<StateType, State> states;

    public JobStateView(JdbcStateInteractor jdbcStateInteractor, String jobKey) throws Exception {
        this.jdbcStateInteractor = jdbcStateInteractor;
        this.jobKey = jobKey;
        this.data = jdbcStateInteractor.queryData(jobKey);
        this.states = generateStates(this.data);
    }

    private Map<StateType, State> generateStates(Map<StateType, String> data) {
        final var states = new HashMap<StateType, State>();
        for (StateType stateType : StateType.values()) {
            if (data.containsKey(stateType)) {
                states.put(stateType, UP_TO_DATE);
            } else {
                states.put(stateType, NOT_NEEDED);
            }
        }
        return states;
    }

    public String get(StateType stateType) {
        return data.get(stateType);
    }

    public void put(StateType stateType, String value) {
        data.put(stateType, value);
        updateState(stateType, STATE_TRANSITIONER::putTransition);
    }

    public void remove(StateType stateType) {
        var oldKey = data.remove(stateType);
        if (oldKey == null) {
            return;
        }
        updateState(stateType, STATE_TRANSITIONER::deleteTransition);
    }

    public void clear() {
        if (data.isEmpty()) {
            return;
        }
        var iterator = data.keySet().iterator();
        while (iterator.hasNext()) {
            var stateType = iterator.next();
            iterator.remove();
            updateState(stateType, STATE_TRANSITIONER::deleteTransition);
        }
    }

    public void flush() throws Exception {
        if (states.values().stream().noneMatch(State::isNeedFlush)) {
            // No any state needs to be flushed.
            return;
        }

        // Build the data that need to be flushed.
        var flushData = new HashMap<State, List<StateType>>(3);
        for (Map.Entry<StateType, State> stateEntry : states.entrySet()) {
            State state = stateEntry.getValue();
            if (!state.isNeedFlush()) {
                continue;
            }
            StateType stateType = stateEntry.getKey();
            flushData.compute(
                    state,
                    (st, list) -> {
                        if (list == null) {
                            list = new LinkedList<>();
                        }
                        list.add(stateType);
                        return list;
                    });
        }

        for (var entry : flushData.entrySet()) {
            State state = entry.getKey();
            List<StateType> stateTypes = entry.getValue();
            switch (state) {
                case NEEDS_CREATE:
                    jdbcStateInteractor.createData(jobKey, stateTypes, data);
                    break;
                case NEEDS_DELETE:
                    jdbcStateInteractor.deleteData(jobKey, stateTypes);
                    break;
                case NEEDS_UPDATE:
                    jdbcStateInteractor.updateData(jobKey, stateTypes, data);
                    break;
                default:
                    throw new IllegalStateException(String.format("Unknown state : %s", state));
            }
            for (var stateType : stateTypes) {
                updateState(stateType, STATE_TRANSITIONER::flushTransition);
            }
        }
    }

    private void updateState(StateType stateType, Function<State, State> stateTransitioner) {
        states.compute(
                stateType,
                (type, oldState) -> {
                    checkState(
                            oldState != null,
                            "The state of each state type should be maintained in states. "
                                    + "It may be a bug, please raise a JIRA to Flink Community.");
                    return stateTransitioner.apply(oldState);
                });
    }

    @VisibleForTesting
    public Map<StateType, String> getDataReadOnly() {
        return Collections.unmodifiableMap(data);
    }
}
