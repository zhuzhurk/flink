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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

            assertNull(vertex.getFailureCause());

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

            assertNull(vertex.getFailureCause());

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
            assertNotNull(vertex.getFailureCause());
            assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));

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
                        && vertex.getFailureCause() != null) {
                    break;
                } else {
                    Thread.sleep(10);
                }
            }

            assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
            assertNotNull(vertex.getFailureCause());
            assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));

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
            assertEquals(testError, vertex.getFailureCause());

            assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
            assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private static class SubmitFailingSimpleAckingTaskManagerGateway
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

    /**
     * Tests that the lazy scheduling flag is correctly forwarded to the produced partition
     * descriptors.
     */
    @Test
    public void testTddProducedPartitionsLazyScheduling() throws Exception {
        for (ScheduleMode scheduleMode : ScheduleMode.values()) {
            JobVertex jobVertex = new JobVertex("v1");
            jobVertex.setInvokableClass(NoOpInvokable.class);
            jobVertex.createAndAddResultDataSet(ResultPartitionType.PIPELINED);
            ExecutionJobVertex ejv =
                    ExecutionGraphTestUtils.getExecutionJobVertex(
                            jobVertex, new DirectScheduledExecutorService(), scheduleMode);

            IntermediateResult result = ejv.getProducedDataSets()[0];

            ExecutionAttemptID attemptID = new ExecutionAttemptID();
            ExecutionVertex vertex = ejv.getTaskVertices()[0];
            TaskDeploymentDescriptorFactory tddFactory =
                    TaskDeploymentDescriptorFactory.fromExecutionVertex(vertex, 1);

            ExecutionEdge mockEdge = createMockExecutionEdge(1);

            result.getPartitions()[0].addConsumerGroup();
            result.getPartitions()[0].addConsumer(mockEdge, 0);

            TaskManagerLocation location =
                    new TaskManagerLocation(
                            ResourceID.generate(), InetAddress.getLoopbackAddress(), 1);

            TaskDeploymentDescriptor tdd =
                    tddFactory.createDeploymentDescriptor(
                            new AllocationID(),
                            0,
                            null,
                            Execution.registerProducedPartitions(
                                            vertex,
                                            location,
                                            attemptID,
                                            scheduleMode.allowLazyDeployment())
                                    .get()
                                    .values());

            Collection<ResultPartitionDeploymentDescriptor> producedPartitions =
                    tdd.getProducedPartitions();

            assertEquals(1, producedPartitions.size());
            ResultPartitionDeploymentDescriptor desc = producedPartitions.iterator().next();
            assertEquals(scheduleMode.allowLazyDeployment(), desc.notifyPartitionDataAvailable());
        }
    }

    private ExecutionEdge createMockExecutionEdge(int maxParallelism) {
        ExecutionVertex targetVertex = mock(ExecutionVertex.class);
        ExecutionJobVertex targetJobVertex = mock(ExecutionJobVertex.class);

        when(targetVertex.getJobVertex()).thenReturn(targetJobVertex);
        when(targetJobVertex.getMaxParallelism()).thenReturn(maxParallelism);

        ExecutionEdge edge = mock(ExecutionEdge.class);
        when(edge.getTarget()).thenReturn(targetVertex);
        return edge;
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

        final SlotProvider slotProvider = new SimpleSlotProvider(parallelism * 2);

        final JobGraph jobGraph = new JobGraph(jobId, "Test Job", source, sink);
        jobGraph.setScheduleMode(ScheduleMode.EAGER);
        final SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(jobGraph, slotProvider);
        scheduler.initialize(mainThreadExecutor.getMainThreadExecutor());

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
