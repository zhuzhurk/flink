/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.util.AbstractID;

import java.util.Set;

/**
 * TODO: add javadoc.
 */
public class RestartPipelinedRegionStrategy implements FailoverStrategy {

	public RestartPipelinedRegionStrategy(FailoverTopology topology) {
		// TODO: iterate over all vertices via FailoverTopology#getVertices
		// TODO: calculate failover regions and likely a Map<ID, FailoverRegion>
	}

	@Override
	public Set<AbstractID> getTasksNeedingRestart(AbstractID executionVertexId, Throwable cause) {
		/*
		 * if DataConsumptionException, extract ResultPartitionID, iterate over FailoverVertex#getInputs and use
		 * FailoverEdge#getResultPartitionID to compare IDs, then use FailoverEdge#getSource to get producer vertex
		 *
		 * FailoverVertex#getInputs() -> FailoverEdge#getSource gives you the partition producers
		 * FailoverVertex#getOutputs() -> FailoverEdge#getTarget gives you the partition consumers
		 *
		 * FailoverEdge#getResultPartitionType can be used to check whether the partition is pipelined or not
		 *
		 * convert vertex set to ID set via FailoverVertex#getExecutionVertexID
		 */
		// TODO: implement  failover logic
		return null;
	}
}
