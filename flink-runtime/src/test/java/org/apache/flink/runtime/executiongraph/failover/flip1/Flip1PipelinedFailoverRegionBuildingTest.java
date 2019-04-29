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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests that make sure that the building of pipelined connected failover regions works
 * correctly.
 */
public class Flip1PipelinedFailoverRegionBuildingTest extends TestLogger {

	/**
	 * Tests that validates that a graph with single unconnected vertices works correctly.
	 *
	 * <pre>
	 *     (v1)
	 *
	 *     (v2)
	 *
	 *     (v3)
	 *
	 *     ...
	 * </pre>
	 */
	@Test
	public void testIndividualVertices() throws Exception {
		final JobVertex source1 = new JobVertex("source1");
		source1.setInvokableClass(NoOpInvokable.class);
		source1.setParallelism(2);

		final JobVertex source2 = new JobVertex("source2");
		source2.setInvokableClass(NoOpInvokable.class);
		source2.setParallelism(2);

		final JobGraph jobGraph = new JobGraph("test job", source1, source2);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);
		FailoverRegion sourceRegion11 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source1.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion sourceRegion12 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source1.getID()).getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion targetRegion21 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source2.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion targetRegion22 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source2.getID()).getTaskVertices()[1].getExecutionVertexID());

		assertTrue(sourceRegion11 != sourceRegion12);
		assertTrue(sourceRegion12 != targetRegion21);
		assertTrue(targetRegion21 != targetRegion22);
	}

	/**
	 * Tests that validates that embarrassingly parallel chains of vertices work correctly.
	 *
	 * <pre>
	 *     (a1) --> (b1)
	 *
	 *     (a2) --> (b2)
	 *
	 *     (a3) --> (b3)
	 *
	 *     ...
	 * </pre>
	 */
	@Test
	public void testEmbarrassinglyParallelCase() throws Exception {
		int parallelism = 10000;
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(parallelism);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(parallelism);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(parallelism);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);
		FailoverRegion preRegion1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion preRegion2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion preRegion3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0].getExecutionVertexID());

		assertTrue(preRegion1 == preRegion2);
		assertTrue(preRegion2 == preRegion3);

		for (int i = 1; i < parallelism; ++i) {
			FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[i].getExecutionVertexID());
			FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[i].getExecutionVertexID());
			FailoverRegion region3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[i].getExecutionVertexID());

			assertTrue(region1 == region2);
			assertTrue(region2 == region3);

			assertTrue(preRegion1 != region1);
		}
	}

	/**
	 * Tests that validates that a single pipelined component via a sequence of all-to-all
	 * connections works correctly.
	 *
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1)
	 *           X         X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *           X         X
	 *     (a3) -+-> (b3) -+-> (c3)
	 *
	 *     ...
	 * </pre>
	 */
	@Test
	public void testOneComponentViaTwoExchanges() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(3);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(5);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[4].getExecutionVertexID());
		FailoverRegion region3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0].getExecutionVertexID());

		assertTrue(region1 == region2);
		assertTrue(region2 == region3);
	}

	/**
	 * Tests that validates that a single pipelined component via a cascade of joins
	 * works correctly.
	 *
	 * <p>Non-parallelized view:
	 * <pre>
	 *     (1)--+
	 *          +--(5)-+
	 *     (2)--+      |
	 *                 +--(7)
	 *     (3)--+      |
	 *          +--(6)-+
	 *     (4)--+
	 *     ...
	 * </pre>
	 */
	@Test
	public void testOneComponentViaCascadeOfJoins() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		final JobVertex vertex5 = new JobVertex("vertex5");
		vertex5.setInvokableClass(NoOpInvokable.class);
		vertex5.setParallelism(4);

		final JobVertex vertex6 = new JobVertex("vertex6");
		vertex6.setInvokableClass(NoOpInvokable.class);
		vertex6.setParallelism(4);

		final JobVertex vertex7 = new JobVertex("vertex7");
		vertex7.setInvokableClass(NoOpInvokable.class);
		vertex7.setParallelism(2);

		vertex5.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex5.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex6.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex6.connectNewDataSetAsInput(vertex4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex7.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex7.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3, vertex4, vertex5, vertex6, vertex7);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);

		Iterator<ExecutionVertex> evs = eg.getAllExecutionVertices().iterator();

		FailoverRegion preRegion = failoverStrategy.getFailoverRegion(evs.next().getExecutionVertexID());

		while (evs.hasNext()) {
			FailoverRegion region = failoverStrategy.getFailoverRegion(evs.next().getExecutionVertexID());
			assertTrue(preRegion == region);
		}
	}

	/**
	 * Tests that validates that a single pipelined component instance from one source
	 * works correctly.
	 *
	 * <p>Non-parallelized view:
	 * <pre>
	 *                 +--(1)
	 *          +--(5)-+
	 *          |      +--(2)
	 *     (7)--+
	 *          |      +--(3)
	 *          +--(6)-+
	 *                 +--(4)
	 *     ...
	 * </pre>
	 */
	@Test
	public void testOneComponentInstanceFromOneSource() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		final JobVertex vertex5 = new JobVertex("vertex5");
		vertex5.setInvokableClass(NoOpInvokable.class);
		vertex5.setParallelism(4);

		final JobVertex vertex6 = new JobVertex("vertex6");
		vertex6.setInvokableClass(NoOpInvokable.class);
		vertex6.setParallelism(4);

		final JobVertex vertex7 = new JobVertex("vertex7");
		vertex7.setInvokableClass(NoOpInvokable.class);
		vertex7.setParallelism(2);

		vertex1.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex2.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex3.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex4.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex5.connectNewDataSetAsInput(vertex7, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex6.connectNewDataSetAsInput(vertex7, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex7, vertex5, vertex6, vertex1, vertex2, vertex3, vertex4);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);

		Iterator<ExecutionVertex> evs = eg.getAllExecutionVertices().iterator();

		FailoverRegion preRegion = failoverStrategy.getFailoverRegion(evs.next().getExecutionVertexID());

		while (evs.hasNext()) {
			FailoverRegion region = failoverStrategy.getFailoverRegion(evs.next().getExecutionVertexID());
			assertTrue(preRegion == region);
		}
	}

	/**
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1)
	 *           X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *           X
	 *     (a3) -+-> (b3) -+-> (c3)
	 *
	 *           ^         ^
	 *           |         |
	 *     (pipelined) (blocking)
	 *
	 * </pre>
	 */
	@Test
	public void testTwoComponentsViaBlockingExchange() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(3);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(2);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region31 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region32 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[1].getExecutionVertexID());

		assertTrue(region1 == region2);
		assertTrue(region2 != region31);
		assertTrue(region32 != region31);
	}

	/**
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1)
	 *           X         X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *           X         X
	 *     (a3) -+-> (b3) -+-> (c3)
	 *
	 *           ^         ^
	 *           |         |
	 *     (pipelined) (blocking)
	 * </pre>
	 */
	@Test
	public void testTwoComponentsViaBlockingExchange2() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(3);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(2);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region31 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region32 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[1].getExecutionVertexID());

		assertTrue(region1 == region2);
		assertTrue(region2 != region31);
		assertTrue(region32 != region31);
	}

	/**
	 * Cascades of joins with partially blocking, partially pipelined exchanges:
	 * <pre>
	 *     (1)--+
	 *          +--(5)-+
	 *     (2)--+      |
	 *              (block)
	 *                 |
	 *                 +--(7)
	 *                 |
	 *              (block)
	 *     (3)--+      |
	 *          +--(6)-+
	 *     (4)--+
	 *     ...
	 * </pre>
	 *
	 * Component 1: 1, 2, 5; component 2: 3,4,6; component 3: 7
	 */
	@Test
	public void testMultipleComponentsViaCascadeOfJoins() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		final JobVertex vertex5 = new JobVertex("vertex5");
		vertex5.setInvokableClass(NoOpInvokable.class);
		vertex5.setParallelism(4);

		final JobVertex vertex6 = new JobVertex("vertex6");
		vertex6.setInvokableClass(NoOpInvokable.class);
		vertex6.setParallelism(4);

		final JobVertex vertex7 = new JobVertex("vertex7");
		vertex7.setInvokableClass(NoOpInvokable.class);
		vertex7.setParallelism(2);

		vertex5.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex5.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex6.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex6.connectNewDataSetAsInput(vertex4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex7.connectNewDataSetAsInput(vertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		vertex7.connectNewDataSetAsInput(vertex6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3, vertex4, vertex5, vertex6, vertex7);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);

		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex1.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex2.getID()).getTaskVertices()[5].getExecutionVertexID());
		FailoverRegion region5 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex5.getID()).getTaskVertices()[2].getExecutionVertexID());

		assertTrue(region1 == region2);
		assertTrue(region1 == region5);

		FailoverRegion region3 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex3.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region4 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex4.getID()).getTaskVertices()[5].getExecutionVertexID());
		FailoverRegion region6 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex6.getID()).getTaskVertices()[2].getExecutionVertexID());

		assertTrue(region3 == region4);
		assertTrue(region3 == region6);

		FailoverRegion region71 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex7.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region72 = failoverStrategy.getFailoverRegion(eg.getJobVertex(vertex7.getID()).getTaskVertices()[1].getExecutionVertexID());

		assertTrue(region71 != region72);
		assertTrue(region1 != region71);
		assertTrue(region1 != region72);
		assertTrue(region3 != region71);
		assertTrue(region3 != region72);
	}

	@Test
	public void testDiamondWithMixedPipelinedAndBlockingExchanges() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(8);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(8);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(8);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(8);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		vertex3.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertex4.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex4.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3, vertex4);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);

		Iterator<ExecutionVertex> evs = eg.getAllExecutionVertices().iterator();

		FailoverRegion preRegion = failoverStrategy.getFailoverRegion(evs.next().getExecutionVertexID());

		while (evs.hasNext()) {
			FailoverRegion region = failoverStrategy.getFailoverRegion(evs.next().getExecutionVertexID());
			assertTrue(preRegion == region);
		}
	}

	/**
	 * This test checks that are strictly co-located vertices are in the same failover region,
	 * even through they are connected via a blocking pattern.
	 * This is currently an assumption / limitation of the scheduler.
	 */
	@Test
	public void testBlockingAllToAllTopologyWithCoLocation() throws Exception {
		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(10);

		final JobVertex target = new JobVertex("target");
		target.setInvokableClass(NoOpInvokable.class);
		target.setParallelism(13);

		target.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final SlotSharingGroup sharingGroup = new SlotSharingGroup();
		source.setSlotSharingGroup(sharingGroup);
		target.setSlotSharingGroup(sharingGroup);

		source.setStrictlyCoLocatedWith(target);

		final JobGraph jobGraph = new JobGraph("test job", source, target);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);
		FailoverRegion region1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(target.getID()).getTaskVertices()[0].getExecutionVertexID());

		// we use 'assertTrue' here rather than 'assertEquals' because we want to test
		// for referential equality, to be on the safe side
		assertTrue(region1 == region2);
	}

	/**
	 * This test checks that are strictly co-located vertices are in the same failover region,
	 * even through they are connected via a blocking pattern.
	 * This is currently an assumption / limitation of the scheduler.
	 */
	@Test
	public void testPipelinedOneToOneTopologyWithCoLocation() throws Exception {
		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(10);

		final JobVertex target = new JobVertex("target");
		target.setInvokableClass(NoOpInvokable.class);
		target.setParallelism(10);

		target.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final SlotSharingGroup sharingGroup = new SlotSharingGroup();
		source.setSlotSharingGroup(sharingGroup);
		target.setSlotSharingGroup(sharingGroup);

		source.setStrictlyCoLocatedWith(target);

		final JobGraph jobGraph = new JobGraph("test job", source, target);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);
		FailoverRegion sourceRegion1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion sourceRegion2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(source.getID()).getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion targetRegion1 = failoverStrategy.getFailoverRegion(eg.getJobVertex(target.getID()).getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion targetRegion2 = failoverStrategy.getFailoverRegion(eg.getJobVertex(target.getID()).getTaskVertices()[1].getExecutionVertexID());

		// we use 'assertTrue' here rather than 'assertEquals' because we want to test
		// for referential equality, to be on the safe side
		assertTrue(sourceRegion1 == sourceRegion2);
		assertTrue(sourceRegion2 == targetRegion1);
		assertTrue(targetRegion1 == targetRegion2);
	}
	/**
	 * Creates a JobGraph of the following form:
	 *
	 * <pre>
	 *  v1--->v2-->\
	 *              \
	 *               v4 --->\
	 *        ----->/        \
	 *  v3-->/                v5
	 *       \               /
	 *        ------------->/
	 * </pre>
	 */
	@Test
	public void testSimpleFailoverRegion() throws Exception {

		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");
		JobVertex v5 = new JobVertex("vertex5");

		v1.setParallelism(5);
		v2.setParallelism(7);
		v3.setParallelism(2);
		v4.setParallelism(11);
		v5.setParallelism(4);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v5.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, jobName, v1, v2, v3, v4, v5);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(eg);
		ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
		ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
		ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
		ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
		ExecutionJobVertex ejv5 = eg.getJobVertex(v5.getID());
		FailoverRegion region1 = strategy.getFailoverRegion(ejv1.getTaskVertices()[2].getExecutionVertexID());
		FailoverRegion region2 = strategy.getFailoverRegion(ejv2.getTaskVertices()[3].getExecutionVertexID());
		FailoverRegion region3 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[4].getExecutionVertexID());
		FailoverRegion region5 = strategy.getFailoverRegion(ejv5.getTaskVertices()[1].getExecutionVertexID());

		assertEquals(region1, region2);
		assertEquals(region3, region2);
		assertEquals(region4, region2);
		assertEquals(region5, region2);
	}

	/**
	 * Creates a JobGraph of the following form:
	 *
	 * <pre>
	 *  v2 ------->\
	 *              \
	 *  v1---------> v4 --->|\
	 *                        \
	 *                        v5
	 *                       /
	 *  v3--------------->|/
	 * </pre>
	 */
	@Test
	public void testMultipleFailoverRegions() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");
		JobVertex v5 = new JobVertex("vertex5");

		v1.setParallelism(3);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(5);
		v5.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v5.setInvokableClass(AbstractInvokable.class);

		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, jobName, v1, v2, v3, v4, v5);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		// All in one failover region
		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(eg);
		ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
		ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
		ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
		ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
		ExecutionJobVertex ejv5 = eg.getJobVertex(v5.getID());
		FailoverRegion region1 = strategy.getFailoverRegion(ejv1.getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region2 = strategy.getFailoverRegion(ejv2.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[3].getExecutionVertexID());
		FailoverRegion region31 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region32 = strategy.getFailoverRegion(ejv3.getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region51 = strategy.getFailoverRegion(ejv5.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region52 = strategy.getFailoverRegion(ejv5.getTaskVertices()[1].getExecutionVertexID());

		//There should be 5 failover regions. v1 v2 v4 in one, v3 has two, v5 has two
		assertEquals(region1, region2);
		assertEquals(region2, region4);
		assertFalse(region31.equals(region32));
		assertFalse(region51.equals(region52));
	}

	/**
	 * Creates a JobGraph of the following form:
	 *
	 * <pre>
	 *  v1--->v2-->\
	 *              \
	 *               v4 --->|\
	 *        ----->/        \
	 *  v3-->/                v5
	 *       \               /
	 *        ------------->/
	 * </pre>
	 */
	@Test
	public void testSingleRegionWithMixedInput() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");
		JobVertex v5 = new JobVertex("vertex5");

		v1.setParallelism(3);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(5);
		v5.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v5.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, jobName, v1, v2, v3, v4, v5);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		// All in one failover region
		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(eg);
		ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
		ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
		ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
		ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
		ExecutionJobVertex ejv5 = eg.getJobVertex(v5.getID());
		FailoverRegion region1 = strategy.getFailoverRegion(ejv1.getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region2 = strategy.getFailoverRegion(ejv2.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[3].getExecutionVertexID());
		FailoverRegion region3 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region5 = strategy.getFailoverRegion(ejv5.getTaskVertices()[1].getExecutionVertexID());

		assertEquals(region1, region2);
		assertEquals(region2, region4);
		assertEquals(region3, region2);
		assertEquals(region1, region5);
	}

	/**
	 * Creates a JobGraph of the following form:
	 *
	 * <pre>
	 *  v1-->v2-->|\
	 *              \
	 *               v4
	 *             /
	 *  v3------>/
	 * </pre>
	 */
	@Test
	public void testMultiRegionNotAllToAll() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");
		JobVertex v5 = new JobVertex("vertex5");

		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(5);
		v4.setParallelism(5);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, jobName, v1, v2, v3, v4);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		// All in one failover region
		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(eg);
		ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
		ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
		ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
		ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
		FailoverRegion region11 = strategy.getFailoverRegion(ejv1.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region12 = strategy.getFailoverRegion(ejv1.getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region21 = strategy.getFailoverRegion(ejv2.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region22 = strategy.getFailoverRegion(ejv2.getTaskVertices()[1].getExecutionVertexID());
		FailoverRegion region3 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0].getExecutionVertexID());
		FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[3].getExecutionVertexID());

		//There should be 3 failover regions. v1 v2 in two, v3 and v4 in one
		assertEquals(region11, region21);
		assertEquals(region12, region22);
		assertFalse(region11.equals(region12));
		assertFalse(region3.equals(region4));
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph) throws JobException, JobExecutionException {
		// configure the pipelined failover strategy
		final Configuration jobManagerConfig = new Configuration();

		final Time timeout = Time.seconds(10L);
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobManagerConfig,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			mock(SlotProvider.class),
			Flip1PipelinedFailoverRegionBuildingTest.class.getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			timeout,
			log);
	}
}
