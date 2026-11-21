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

package org.apache.flink.kubernetes.operator.bluegreen.agent;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.kubernetes.operator.bluegreen.client.GateInjectorExecutor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.MethodDelegation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Submits a job through each way a job can be submitted, and checks that the graph the executor
 * receives went through the gate injector exactly once.
 *
 * <p>The agent rebases {@link GateInjectionAgent#INSTRUMENTED_TYPE} itself, which needs an {@code
 * Instrumentation}. Here a subclass of each environment overrides the same method with the same
 * interceptor instead: that is equivalent as long as {@code StreamContextEnvironment}'s override
 * calls the base method, which {@link FlinkContractTest} checks.
 */
public class GateInjectionAgentTest {

    private final CapturingExecutorServiceLoader executor = new CapturingExecutorServiceLoader();

    @BeforeEach
    public void resetStub() {
        GateInjectorExecutor.INJECTED_GRAPHS.clear();
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    }

    @ParameterizedTest(name = "{0} on {1}")
    @MethodSource("submissions")
    public void everySubmissionInjectsTheGatesOnce(
            String submissionName, String environmentName, Submission submission) throws Exception {
        StreamExecutionEnvironment env =
                StreamContextEnvironment.class.getSimpleName().equals(environmentName)
                        ? instrumented(StreamContextEnvironment.class)
                                .getConstructor(
                                        PipelineExecutorServiceLoader.class,
                                        Configuration.class,
                                        ClassLoader.class,
                                        boolean.class,
                                        boolean.class)
                                .newInstance(
                                        executor,
                                        configuration(),
                                        getClass().getClassLoader(),
                                        false,
                                        true)
                        : instrumented(StreamExecutionEnvironment.class)
                                .getConstructor(
                                        PipelineExecutorServiceLoader.class,
                                        Configuration.class,
                                        ClassLoader.class)
                                .newInstance(
                                        executor, configuration(), getClass().getClassLoader());
        env.fromSequence(0, 9).sinkTo(new DiscardingSink<>());

        // The executor fails every submission once it has the graph, so that no job runs
        assertThrows(Exception.class, () -> submission.submit(env));

        assertEquals(1, executor.submitted.size());
        assertEquals(1, GateInjectorExecutor.INJECTED_GRAPHS.size());
        assertSame(executor.submitted.get(0), GateInjectorExecutor.INJECTED_GRAPHS.get(0));
    }

    static Stream<Arguments> submissions() {
        List<Arguments> submissions = new ArrayList<>();
        for (String environment :
                List.of(
                        StreamExecutionEnvironment.class.getSimpleName(),
                        StreamContextEnvironment.class.getSimpleName())) {
            submissions.add(
                    Arguments.of("execute()", environment, (Submission) env -> env.execute()));
            submissions.add(
                    Arguments.of(
                            "execute(String)",
                            environment,
                            (Submission) env -> env.execute("bluegreen-job")));
            submissions.add(
                    Arguments.of(
                            "executeAsync()", environment, (Submission) env -> env.executeAsync()));
            submissions.add(
                    Arguments.of(
                            "executeAsync(String)",
                            environment,
                            (Submission) env -> env.executeAsync("bluegreen-job")));
        }
        return submissions.stream();
    }

    private static <T extends StreamExecutionEnvironment> Class<? extends T> instrumented(
            Class<T> environment) {
        return new ByteBuddy()
                .subclass(environment)
                .method(GateInjectionAgent.interceptedMethod())
                .intercept(MethodDelegation.to(GateInjectionInterceptor.class))
                .make()
                .load(GateInjectionAgentTest.class.getClassLoader())
                .getLoaded();
    }

    private static Configuration configuration() {
        Configuration configuration = new Configuration();
        configuration.setString("bluegreen.gate.injection.enabled", "true");
        configuration.set(DeploymentOptions.TARGET, CapturingExecutorServiceLoader.NAME);
        return configuration;
    }

    /** One way of submitting the job of an environment. */
    interface Submission {
        void submit(StreamExecutionEnvironment env) throws Exception;
    }

    /** Provides an executor that records the graph of each submission, then fails it. */
    private static class CapturingExecutorServiceLoader
            implements PipelineExecutorServiceLoader, PipelineExecutorFactory {

        static final String NAME = "capturing";

        final List<Pipeline> submitted = new ArrayList<>();

        @Override
        public PipelineExecutorFactory getExecutorFactory(Configuration configuration) {
            return this;
        }

        @Override
        public Stream<String> getExecutorNames() {
            return Stream.of(NAME);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return true;
        }

        @Override
        public PipelineExecutor getExecutor(Configuration configuration) {
            return (pipeline, executionConfiguration, userCodeClassloader) -> {
                submitted.add(pipeline);
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException("The test does not run jobs"));
            };
        }
    }
}
