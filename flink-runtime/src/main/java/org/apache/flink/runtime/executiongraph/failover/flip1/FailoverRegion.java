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

import org.apache.flink.util.AbstractID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * FailoverRegion is a subset of all the vertices in the jbo topology.
 */
public class FailoverRegion {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(FailoverRegion.class);

	private final Set<AbstractID> executionVertexIDs;

	public FailoverRegion(Collection<? extends FailoverVertex> executionVertices) {
		checkNotNull(executionVertices);
		this.executionVertexIDs = executionVertices.stream().map(
			v -> v.getExecutionVertexID()).collect(Collectors.toSet());

		LOG.debug("Created a failover region with {} vertices.", executionVertices.size());
	}

	/**
	 * get IDs of all execution vertex IDs in this region
	 */
	public Set<AbstractID> getAllExecutionVertexIDs() {
		return executionVertexIDs;
	}
}
