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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * This class contains scheduling logic similar to that in ExecutionGraph.
 * It is used by legacy failover strategy as the strategy will do the re-scheduling work by itself.
 * We extract it from ExecutionGraph to avoid affect existing scheduling logic.
 */
public class AdaptedSchedulingUtils {

	static CompletableFuture<Void> scheduleLazy(
		final Set<ExecutionVertex> vertices,
		final ExecutionGraph executionGraph) {

		final Set<AllocationID> previousAllocations = getAllPriorAllocationIds(vertices);

		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>(vertices.size());
		for (ExecutionVertex executionVertex : vertices) {
			// only schedule vertex when its input constraint is satisfied
			if (executionVertex.getJobVertex().getJobVertex().isInputVertex() ||
				executionVertex.checkInputDependencyConstraints()) {

				final CompletableFuture<Void> schedulingVertexFuture = executionVertex.scheduleForExecution(
					executionGraph.getSlotProvider(),
					executionGraph.isQueuedSchedulingAllowed(),
					LocationPreferenceConstraint.ANY,
					previousAllocations);

				schedulingFutures.add(schedulingVertexFuture);
			}
		}

		return FutureUtils.waitForAll(schedulingFutures);
	}

	static CompletableFuture<Void> scheduleEager(
		final Set<ExecutionVertex> vertices,
		final ExecutionGraph executionGraph) {

		// Important: reserve all the space we need up front.
		// that way we do not have any operation that can fail between allocating the slots
		// and adding them to the list. If we had a failure in between there, that would
		// cause the slots to get lost
		final boolean queued = executionGraph.isQueuedSchedulingAllowed();

		// collecting all the slots may resize and fail in that operation without slots getting lost
		final ArrayList<CompletableFuture<Execution>> allAllocationFutures = new ArrayList<>(vertices.size());

		final Set<AllocationID> allPreviousAllocationIds = Collections.unmodifiableSet(getAllPriorAllocationIds(vertices));

		// allocate the slots (obtain all their futures
		for (ExecutionVertex ev : vertices) {
			// these calls are not blocking, they only return futures
			CompletableFuture<Execution> allocationFuture = ev.getCurrentExecutionAttempt().allocateResourcesForExecution(
				executionGraph.getSlotProvider(),
				queued,
				LocationPreferenceConstraint.ALL,
				allPreviousAllocationIds,
				executionGraph.getAllocationTimeout());

			allAllocationFutures.add(allocationFuture);
		}

		// this future is complete once all slot futures are complete.
		// the future fails once one slot future fails.
		final FutureUtils.ConjunctFuture<Collection<Execution>> allAllocationsFuture = FutureUtils.combineAll(allAllocationFutures);

		return allAllocationsFuture.thenAccept(
			(Collection<Execution> executionsToDeploy) -> {
				for (Execution execution : executionsToDeploy) {
					try {
						execution.deploy();
					} catch (Throwable t) {
						throw new CompletionException(
							new FlinkException(
								String.format("Could not deploy execution %s.", execution),
								t));
					}
				}
			})
			// Generate a more specific failure message for the eager scheduling
			.exceptionally(
				(Throwable throwable) -> {
					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
					final Throwable resultThrowable;
					if (strippedThrowable instanceof TimeoutException) {
						int numTotal = allAllocationsFuture.getNumFuturesTotal();
						int numComplete = allAllocationsFuture.getNumFuturesCompleted();

						String message = "Could not allocate all requires slots within timeout of "
							+ executionGraph.getAllocationTimeout() + ". Slots required: "
							+ numTotal + ", slots allocated: " + numComplete
							+ ", previous allocation IDs: " + allPreviousAllocationIds;

						StringBuilder executionMessageBuilder = new StringBuilder();

						for (int i = 0; i < allAllocationFutures.size(); i++) {
							CompletableFuture<Execution> executionFuture = allAllocationFutures.get(i);

							try {
								Execution execution = executionFuture.getNow(null);
								if (execution != null) {
									executionMessageBuilder.append("completed: " + execution);
								} else {
									executionMessageBuilder.append("incomplete: " + executionFuture);
								}
							} catch (CompletionException completionException) {
								executionMessageBuilder.append("completed exceptionally: "
									+ completionException + "/" + executionFuture);
							}

							if (i < allAllocationFutures.size() - 1) {
								executionMessageBuilder.append(", ");
							}
						}

						message += ", execution status: " + executionMessageBuilder.toString();

						resultThrowable = new NoResourceAvailableException(message);
					} else {
						resultThrowable = strippedThrowable;
					}

					throw new CompletionException(resultThrowable);
				});
	}

	/**
	 * Computes and returns a set with the prior allocation ids from all execution vertices in the graph.
	 */
	static Set<AllocationID> getAllPriorAllocationIds(final Set<ExecutionVertex> vertices) {
		HashSet<AllocationID> allPreviousAllocationIds = new HashSet<>(vertices.size());
		for (ExecutionVertex executionVertex : vertices) {
			AllocationID latestPriorAllocation = executionVertex.getLatestPriorAllocation();
			if (latestPriorAllocation != null) {
				allPreviousAllocationIds.add(latestPriorAllocation);
			}
		}
		return allPreviousAllocationIds;
	}
}
