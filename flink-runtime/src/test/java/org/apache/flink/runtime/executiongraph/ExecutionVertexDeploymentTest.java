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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExecutionVertexDeploymentTest extends TestLogger {

    private static final String ERROR_MESSAGE = "test_failure_error_message";

    @Test
    public void testDeployCall() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

            assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
            vertex.deployToSlot(slot);
            assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertFalse(vertex.getFailureInfo().isPresent());

            assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeployWithSynchronousAnswer() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

            assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

            vertex.deployToSlot(slot);

            assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertFalse(vertex.getFailureInfo().isPresent());

            assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) == 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeployWithAsynchronousAnswer() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

            assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

            vertex.deployToSlot(slot);

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) == 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeployFailedSynchronous() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new SubmitFailingSimpleAckingTaskManagerGateway())
                            .createTestingLogicalSlot();

            assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

            vertex.deployToSlot(slot);

            assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
            assertTrue(vertex.getFailureInfo().isPresent());
            assertThat(
                    vertex.getFailureInfo().map(ErrorInfo::getExceptionAsString).get(),
                    containsString(ERROR_MESSAGE));

            assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeployFailedAsynchronously() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new SubmitFailingSimpleAckingTaskManagerGateway())
                            .createTestingLogicalSlot();

            assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

            vertex.deployToSlot(slot);

            // wait until the state transition must be done
            for (int i = 0; i < 100; i++) {
                if (vertex.getExecutionState() == ExecutionState.FAILED
                        && vertex.getFailureInfo().isPresent()) {
                    break;
                } else {
                    Thread.sleep(10);
                }
            }

            assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
            assertTrue(vertex.getFailureInfo().isPresent());
            assertThat(
                    vertex.getFailureInfo().map(ErrorInfo::getExceptionAsString).get(),
                    containsString(ERROR_MESSAGE));

            assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFailExternallyDuringDeploy() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            TestingLogicalSlot testingLogicalSlot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new SubmitBlockingSimpleAckingTaskManagerGateway())
                            .createTestingLogicalSlot();

            assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
            vertex.deployToSlot(testingLogicalSlot);
            assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

            Exception testError = new Exception("test error");
            vertex.fail(testError);

            assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
            assertThat(
                    vertex.getFailureInfo()
                            .map(ErrorInfo::getException)
                            .get()
                            .deserializeError(ClassLoader.getSystemClassLoader()),
                    is(testError));

            assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    public static class SubmitFailingSimpleAckingTaskManagerGateway
            extends SimpleAckingTaskManagerGateway {
        @Override
        public CompletableFuture<Acknowledge> submitTask(
                TaskDeploymentDescriptor tdd, Time timeout) {
            CompletableFuture<Acknowledge> future = new CompletableFuture<>();
            future.completeExceptionally(new Exception(ERROR_MESSAGE));
            return future;
        }
    }

    private static class SubmitBlockingSimpleAckingTaskManagerGateway
            extends SimpleAckingTaskManagerGateway {
        @Override
        public CompletableFuture<Acknowledge> submitTask(
                TaskDeploymentDescriptor tdd, Time timeout) {
            return new CompletableFuture<>();
        }
    }

    private static final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor();

    private static final TestingComponentMainThreadExecutor mainThreadExecutor =
            new TestingComponentMainThreadExecutor(
                    ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                            scheduledExecutorService));

    @Test
    public void testTddCreationPerformance() throws Exception {
        final int parallelism = 4000;

        final JobID jobId = new JobID();

        final JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(parallelism);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(parallelism);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        final JobGraph jobGraph = new JobGraph(jobId, "Test Job", source, sink);
        jobGraph.setJobType(JobType.STREAMING);
        final SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(
                        jobGraph, mainThreadExecutor.getMainThreadExecutor());

        final long start = System.currentTimeMillis();

        deploy(source.getID(), parallelism, scheduler);
        deploy(sink.getID(), parallelism, scheduler);

        final long end = System.currentTimeMillis();
        System.out.println("Time elapsed on scheduling: " + (end - start));
    }

    private void deploy(JobVertexID jobVertexId, int parallelism, SchedulerBase scheduler) {
        for (int i = 0; i < parallelism; i++) {
            final ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertexId, i);
            final ExecutionVertex vertex = scheduler.getExecutionVertex(executionVertexId);
            mainThreadExecutor.execute(
                    () -> {
                        final LogicalSlot slot =
                                new TestingLogicalSlotBuilder().createTestingLogicalSlot();
                        vertex.getCurrentExecutionAttempt()
                                .registerProducedPartitions(slot.getTaskManagerLocation(), false);
                        vertex.tryAssignResource(slot);
                        vertex.deploy();
                    });
        }
    }
}
