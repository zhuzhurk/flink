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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImplTest.setupScheduler;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImplTest.setupSlotPool;
import static org.junit.Assert.assertEquals;

/**
 * Tests input locality works as expected when using slot sharing.
 */
public class TaskInputLocalityOnSlotSharingTest extends TestLogger {

	private static final JobID JOB_ID = new JobID();

	private static final JobVertexID VERTEX_ID_1 = new JobVertexID();

	private static final JobVertexID VERTEX_ID_2 = new JobVertexID();

	private static final ComponentMainThreadExecutor MAIN_THREAD_EXECUTOR =
		ComponentMainThreadExecutorServiceAdapter.forMainThread();

	private final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

	@Test
	public void testTaskInputLocalityIsRespectedOnSlotSharing() throws Exception {
		try (SlotPoolImpl slotPool = new TestingSlotPoolImpl(JOB_ID)) {
			setupSlotPool(slotPool, resourceManagerGateway, MAIN_THREAD_EXECUTOR);

			final Scheduler scheduler = setupScheduler(slotPool, MAIN_THREAD_EXECUTOR);

			// The input locality may be expected by chance even if the input locality is not respected.
			// The chance decreases with larger parallelism. With a parallelism 200, the chance should be small enough.
			final int parallelism = 200;
			final ExecutionGraph eg = createExecutionGraph(parallelism, scheduler);
			eg.scheduleForExecution();

			final Set<AllocationID> allocationIds = new HashSet<>(slotPool.getPendingRequests().keySetB());
			for (AllocationID allocationID : allocationIds) {
				final SlotOffer slotOffer = new SlotOffer(
					allocationID,
					0,
					DEFAULT_TESTING_PROFILE);

				final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(
					ResourceID.generate(),
					InetAddress.getLoopbackAddress(),
					42);
				slotPool.registerTaskManager(taskManagerLocation.getResourceID());
				slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer);
			}

			final ExecutionJobVertex ejv1 = eg.getJobVertex(VERTEX_ID_1);
			final ExecutionJobVertex ejv2 = eg.getJobVertex(VERTEX_ID_2);
			for (int i = 0; i < parallelism; i++) {
				final ExecutionVertex ev1 = ejv1.getTaskVertices()[i];
				final ExecutionVertex ev2 = ejv2.getTaskVertices()[i];

				assertEquals(
					ev1.getCurrentAssignedResource().getTaskManagerLocation(),
					ev2.getCurrentAssignedResource().getTaskManagerLocation());
			}
		}
	}

	private static JobGraph createJobGraph(final int parallelism) {
		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		final JobVertex v1 = new JobVertex("source", VERTEX_ID_1);
		v1.setParallelism(parallelism);
		v1.setInvokableClass(NoOpInvokable.class);
		v1.setSlotSharingGroup(slotSharingGroup);

		final JobVertex v2 = new JobVertex("sink", VERTEX_ID_2);
		v2.setParallelism(parallelism);
		v2.setInvokableClass(NoOpInvokable.class);
		v2.setSlotSharingGroup(slotSharingGroup);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(JOB_ID, "Test Job", v1, v2);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		return jobGraph;
	}

	private static ExecutionGraph createExecutionGraph(
			final int parallelism,
			final SlotProvider slotProvider) throws Exception {

		final ExecutionGraph eg = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(createJobGraph(parallelism))
			.setSlotProvider(slotProvider)
			.build();

		eg.start(MAIN_THREAD_EXECUTOR);

		return eg;
	}
}
