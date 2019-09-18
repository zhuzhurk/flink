/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

class DeploymentHandle {

	private final ExecutionVertexVersion requiredVertexVersion;

	private final ExecutionVertexDeploymentOption executionVertexDeploymentOption;

	private final SlotExecutionVertexAssignment slotExecutionVertexAssignment;

	public DeploymentHandle(
		final ExecutionVertexVersion requiredVertexVersion,
		final ExecutionVertexDeploymentOption executionVertexDeploymentOption,
		final SlotExecutionVertexAssignment slotExecutionVertexAssignment) {

		this.requiredVertexVersion = Preconditions.checkNotNull(requiredVertexVersion);
		this.executionVertexDeploymentOption = Preconditions.checkNotNull(executionVertexDeploymentOption);
		this.slotExecutionVertexAssignment = Preconditions.checkNotNull(slotExecutionVertexAssignment);
	}

	public ExecutionVertexID getExecutionVertexId() {
		return requiredVertexVersion.getExecutionVertexId();
	}

	public ExecutionVertexVersion getRequiredVertexVersion() {
		return requiredVertexVersion;
	}

	public DeploymentOption getDeploymentOption() {
		return executionVertexDeploymentOption.getDeploymentOption();
	}

	public SlotExecutionVertexAssignment getSlotExecutionVertexAssignment() {
		return slotExecutionVertexAssignment;
	}

	public LogicalSlot getLogicalSlot() {
		final LogicalSlot logicalSlot = slotExecutionVertexAssignment.getLogicalSlotFuture().getNow(null);
		Preconditions.checkState(logicalSlot != null, "method can only be called after slot is assigned");
		return logicalSlot;
	}
}
