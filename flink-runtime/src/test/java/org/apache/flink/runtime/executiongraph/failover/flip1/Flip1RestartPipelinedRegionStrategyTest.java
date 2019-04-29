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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class Flip1RestartPipelinedRegionStrategyTest extends TestLogger {

	/**
	 * Tests for scenes that a task fails for its own error, in which case only the
	 * region containing the failed task should be restarted.
	 */
	@Test
	public void testRegionFailoverForTaskInternalErrors() throws Exception {
		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(2);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(2);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph("test job", vertex1, vertex2, vertex3);
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		ExecutionVertex ev11 = eg.getJobVertex(vertex1.getID()).getTaskVertices()[0];
		ExecutionVertex ev12 = eg.getJobVertex(vertex1.getID()).getTaskVertices()[1];
		ExecutionVertex ev21 = eg.getJobVertex(vertex2.getID()).getTaskVertices()[0];
		ExecutionVertex ev22 = eg.getJobVertex(vertex2.getID()).getTaskVertices()[1];
		ExecutionVertex ev31 = eg.getJobVertex(vertex3.getID()).getTaskVertices()[0];
		ExecutionVertex ev32 = eg.getJobVertex(vertex3.getID()).getTaskVertices()[1];

		RestartPipelinedRegionStrategy failoverStrategy = new RestartPipelinedRegionStrategy(eg);

		assertEquals(failoverStrategy.getFailoverRegion(ev11.getExecutionVertexID()).getAllExecutionVertexIDs(),
			failoverStrategy.getTasksNeedingRestart(ev11.getExecutionVertexID(), new Exception("Test failure")));
		assertEquals(failoverStrategy.getFailoverRegion(ev12.getExecutionVertexID()).getAllExecutionVertexIDs(),
			failoverStrategy.getTasksNeedingRestart(ev12.getExecutionVertexID(), new Exception("Test failure")));
		assertEquals(failoverStrategy.getFailoverRegion(ev21.getExecutionVertexID()).getAllExecutionVertexIDs(),
			failoverStrategy.getTasksNeedingRestart(ev21.getExecutionVertexID(), new Exception("Test failure")));
		assertEquals(failoverStrategy.getFailoverRegion(ev22.getExecutionVertexID()).getAllExecutionVertexIDs(),
			failoverStrategy.getTasksNeedingRestart(ev22.getExecutionVertexID(), new Exception("Test failure")));
		assertEquals(failoverStrategy.getFailoverRegion(ev31.getExecutionVertexID()).getAllExecutionVertexIDs(),
			failoverStrategy.getTasksNeedingRestart(ev31.getExecutionVertexID(), new Exception("Test failure")));
		assertEquals(failoverStrategy.getFailoverRegion(ev32.getExecutionVertexID()).getAllExecutionVertexIDs(),
			failoverStrategy.getTasksNeedingRestart(ev32.getExecutionVertexID(), new Exception("Test failure")));
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
