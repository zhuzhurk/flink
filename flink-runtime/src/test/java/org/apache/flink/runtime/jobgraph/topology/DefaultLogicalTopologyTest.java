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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link DefaultLogicalTopology}.
 */
public class DefaultLogicalTopologyTest extends TestLogger {

	private JobGraph jobGraph;

	private DefaultLogicalTopology logicalTopology;

	@Before
	public void setUp() throws Exception {
		jobGraph = createJobGraph(false);
		logicalTopology = new DefaultLogicalTopology(jobGraph);
	}

	@Test
	public void testGetVertices() {
		// vertices from getVertices() should be topologically sorted
		Iterator<JobVertex> jobVertexIterator = jobGraph.getVerticesSortedTopologicallyFromSources().iterator();
		Iterator<DefaultLogicalVertex> logicalVertexIterator = logicalTopology.getVertices().iterator();
		while (jobVertexIterator.hasNext()) {
			assertTrue(logicalVertexIterator.hasNext());
			assertVertexEquals(jobVertexIterator.next(), logicalVertexIterator.next());
		}
		assertFalse(logicalVertexIterator.hasNext());
	}

	@Test
	public void testGetVertex() {
		for (JobVertex jobVertex : jobGraph.getVertices()) {
			DefaultLogicalVertex logicalVertex = logicalTopology.getVertex(jobVertex.getID());

			assertVertexEquals(jobVertex, logicalVertex);
		}
	}

	@Test
	public void testGetVertexNotExist() {
		try {
			logicalTopology.getVertex(new JobVertexID());
			fail("should throw an exception");
		} catch (IllegalArgumentException exception) {
			// expected
		}
	}

	@Test
	public void testGetResult() {
		for (JobVertex jobVertex : jobGraph.getVertices()) {
			for (IntermediateDataSet intermediateDataSet : jobVertex.getProducedDataSets()) {
				DefaultLogicalResult logicalResult = logicalTopology.getResult(intermediateDataSet.getId());

				assertResultEquals(intermediateDataSet, logicalResult);
			}
		}
	}

	@Test
	public void testGetResultNotExist() {
		try {
			logicalTopology.getResult(new IntermediateDataSetID());
			fail("should throw an exception");
		} catch (IllegalArgumentException exception) {
			// expected
		}
	}

	@Test
	public void testWithCoLocationConstraints() {
		final DefaultLogicalTopology topology = new DefaultLogicalTopology(createJobGraph(true));
		assertTrue(topology.containsCoLocationConstraints());
	}

	@Test
	public void testWithoutCoLocationConstraints() {
		assertFalse(logicalTopology.containsCoLocationConstraints());
	}

	@Test
	public void testGetLogicalPipelinedRegions_OneRegion() {
		final Set<Set<DefaultLogicalVertex>> regions = logicalTopology.getLogicalPipelinedRegions();
		assertEquals(1, regions.size());
	}

	@Test
	public void testGetLogicalPipelinedRegions_MultipleRegionsWithBlockingEdges() {
		JobVertex[] jobVertices = new JobVertex[2];
		int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, BLOCKING);
		final JobGraph jobGraph = new JobGraph(jobVertices);

		final DefaultLogicalTopology topology = new DefaultLogicalTopology(jobGraph);

		final Set<Set<DefaultLogicalVertex>> regions = topology.getLogicalPipelinedRegions();
		assertEquals(2, regions.size());
	}

	@Test
	public void testGetLogicalPipelinedRegions_MultipleRegionsWithNoConnection() {
		JobVertex[] jobVertices = new JobVertex[2];
		int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		final JobGraph jobGraph = new JobGraph(jobVertices);

		final DefaultLogicalTopology topology = new DefaultLogicalTopology(jobGraph);

		final Set<Set<DefaultLogicalVertex>> regions = topology.getLogicalPipelinedRegions();
		assertEquals(2, regions.size());
	}

	private JobGraph createJobGraph(final boolean containsCoLocationConstraint) {
		final JobVertex[] jobVertices = new JobVertex[2];
		final int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);

		if (containsCoLocationConstraint) {
			final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
			jobVertices[0].setSlotSharingGroup(slotSharingGroup);
			jobVertices[1].setSlotSharingGroup(slotSharingGroup);

			final CoLocationGroup coLocationGroup = new CoLocationGroup();
			coLocationGroup.addVertex(jobVertices[0]);
			coLocationGroup.addVertex(jobVertices[1]);
			jobVertices[0].updateCoLocationGroup(coLocationGroup);
			jobVertices[1].updateCoLocationGroup(coLocationGroup);
		}

		return new JobGraph(jobVertices);
	}

	private static void assertVertexEquals(
			final JobVertex jobVertex,
			final DefaultLogicalVertex logicalVertex) {

		assertVertexInfoEquals(jobVertex, logicalVertex);
		// TODO: verify logicalVertex.getConsumedResults() and logicalVertex.getProducedResults()
	}

	private static void assertResultEquals(
			final IntermediateDataSet intermediateDataSet,
			final DefaultLogicalResult logicalResult) {

		assertResultInfoEquals(intermediateDataSet, logicalResult);
		//TODO: verify logicalResult.getProducer() and logicalResult.getProducer()
	}

	private static void assertVertexInfoEquals(
			final JobVertex jobVertex,
			final DefaultLogicalVertex logicalVertex) {

		assertEquals(jobVertex.getID(), logicalVertex.getId());
	}

	private static void assertResultInfoEquals(
			final IntermediateDataSet intermediateDataSet,
			final DefaultLogicalResult logicalResult) {

		assertEquals(intermediateDataSet.getId(), logicalResult.getId());
		assertEquals(intermediateDataSet.getResultType(), logicalResult.getResultType());
	}
}
