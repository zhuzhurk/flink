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

package org.apache.flink.runtime.deployment;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Factory of {@link TaskDeploymentDescriptor} to deploy {@link
 * org.apache.flink.runtime.taskmanager.Task} from {@link Execution}.
 */
public class TaskDeploymentDescriptorFactory {
    private final ExecutionAttemptID executionId;
    private final int attemptNumber;
    private final MaybeOffloaded<JobInformation> serializedJobInformation;
    private final MaybeOffloaded<TaskInformation> taskInfo;
    private final JobID jobID;
    private final PartitionLocationConstraint partitionDeploymentConstraint;
    private final int subtaskIndex;
    private final List<ConsumedPartitionGroup> consumedPartitionGroups;
    private final Function<IntermediateResultPartitionID, IntermediateResultPartition>
            partitionRetriever;

    private TaskDeploymentDescriptorFactory(
            ExecutionAttemptID executionId,
            int attemptNumber,
            MaybeOffloaded<JobInformation> serializedJobInformation,
            MaybeOffloaded<TaskInformation> taskInfo,
            JobID jobID,
            PartitionLocationConstraint partitionDeploymentConstraint,
            int subtaskIndex,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Function<IntermediateResultPartitionID, IntermediateResultPartition>
                    partitionRetriever) {
        this.executionId = executionId;
        this.attemptNumber = attemptNumber;
        this.serializedJobInformation = serializedJobInformation;
        this.taskInfo = taskInfo;
        this.jobID = jobID;
        this.partitionDeploymentConstraint = partitionDeploymentConstraint;
        this.subtaskIndex = subtaskIndex;
        this.consumedPartitionGroups = consumedPartitionGroups;
        this.partitionRetriever = partitionRetriever;
    }

    public TaskDeploymentDescriptor createDeploymentDescriptor(
            AllocationID allocationID,
            @Nullable JobManagerTaskRestore taskRestore,
            Collection<ResultPartitionDeploymentDescriptor> producedPartitions) {
        return new TaskDeploymentDescriptor(
                jobID,
                serializedJobInformation,
                taskInfo,
                executionId,
                allocationID,
                subtaskIndex,
                attemptNumber,
                taskRestore,
                new ArrayList<>(producedPartitions),
                createInputGateDeploymentDescriptors());
    }

    private List<InputGateDeploymentDescriptor> createInputGateDeploymentDescriptors() {
        List<InputGateDeploymentDescriptor> inputGates =
                new ArrayList<>(consumedPartitionGroups.size());

        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroups) {
            IntermediateResultPartition resultPartition =
                    getIntermediateResultPartition(consumedPartitionGroup.getFirst());

            // All partitions in the same ConsumedPartitionGroup have the same consumers
            int numConsumers = resultPartition.getConsumers().get(0).size();

            int queueToRequest = subtaskIndex % numConsumers;
            IntermediateResult consumedIntermediateResult = resultPartition.getIntermediateResult();
            IntermediateDataSetID resultId = consumedIntermediateResult.getId();
            ResultPartitionType partitionType = consumedIntermediateResult.getResultType();

            inputGates.add(
                    new InputGateDeploymentDescriptor(
                            resultId,
                            partitionType,
                            queueToRequest,
                            getConsumedPartitionShuffleDescriptors(consumedPartitionGroup)));
        }

        return inputGates;
    }

    private ShuffleDescriptor[] getConsumedPartitionShuffleDescriptors(
            ConsumedPartitionGroup consumedPartitionGroup) {

        final IntermediateResult consumedIntermediateResult =
                getIntermediateResultPartition(consumedPartitionGroup.getFirst())
                        .getIntermediateResult();

        // currently one result can have exactly one consumer job vertex
        final int consumerIndex = 0;

        if (consumedIntermediateResult.isShuffleDescriptorsCacheEnabledFor(consumerIndex)) {
            final ShuffleDescriptor[] cachedShuffleDescriptors =
                    consumedIntermediateResult.getCachedShuffleDescriptors(consumerIndex);
            if (cachedShuffleDescriptors != null) {
                return cachedShuffleDescriptors;
            } else {
                final ShuffleDescriptor[] shuffleDescriptors =
                        buildConsumedPartitionShuffleDescriptors(consumedPartitionGroup);
                consumedIntermediateResult.cacheShuffleDescriptors(
                        consumerIndex, shuffleDescriptors);
                return shuffleDescriptors;
            }
        } else {
            return buildConsumedPartitionShuffleDescriptors(consumedPartitionGroup);
        }
    }

    private ShuffleDescriptor[] buildConsumedPartitionShuffleDescriptors(
            ConsumedPartitionGroup consumedPartitionGroup) {

        final ShuffleDescriptor[] shuffleDescriptors =
                new ShuffleDescriptor[consumedPartitionGroup.size()];

        int i = 0;
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            shuffleDescriptors[i++] =
                    getConsumedPartitionShuffleDescriptor(
                            getIntermediateResultPartition(partitionId),
                            partitionDeploymentConstraint);
        }

        return shuffleDescriptors;
    }

    private IntermediateResultPartition getIntermediateResultPartition(
            IntermediateResultPartitionID intermediateResultPartitionId) {
        return partitionRetriever.apply(intermediateResultPartitionId);
    }

    public static TaskDeploymentDescriptorFactory fromExecutionVertex(
            ExecutionVertex executionVertex, int attemptNumber) throws IOException {
        InternalExecutionGraphAccessor internalExecutionGraphAccessor =
                executionVertex.getExecutionGraphAccessor();

        return new TaskDeploymentDescriptorFactory(
                executionVertex.getCurrentExecutionAttempt().getAttemptId(),
                attemptNumber,
                getSerializedJobInformation(internalExecutionGraphAccessor),
                getSerializedTaskInformation(
                        executionVertex.getJobVertex().getTaskInformationOrBlobKey()),
                internalExecutionGraphAccessor.getJobID(),
                internalExecutionGraphAccessor.getPartitionLocationConstraint(),
                executionVertex.getParallelSubtaskIndex(),
                executionVertex.getAllConsumedPartitionGroups(),
                id -> internalExecutionGraphAccessor.getResultPartitionOrThrow(id));
    }

    private static MaybeOffloaded<JobInformation> getSerializedJobInformation(
            InternalExecutionGraphAccessor internalExecutionGraphAccessor) {
        Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey =
                internalExecutionGraphAccessor.getJobInformationOrBlobKey();
        if (jobInformationOrBlobKey.isLeft()) {
            return new TaskDeploymentDescriptor.NonOffloaded<>(jobInformationOrBlobKey.left());
        } else {
            return new TaskDeploymentDescriptor.Offloaded<>(jobInformationOrBlobKey.right());
        }
    }

    private static MaybeOffloaded<TaskInformation> getSerializedTaskInformation(
            Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInfo) {
        return taskInfo.isLeft()
                ? new TaskDeploymentDescriptor.NonOffloaded<>(taskInfo.left())
                : new TaskDeploymentDescriptor.Offloaded<>(taskInfo.right());
    }

    public static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            IntermediateResultPartition consumedPartition,
            PartitionLocationConstraint partitionDeploymentConstraint) {
        Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();

        ExecutionState producerState = producer.getState();
        Optional<ResultPartitionDeploymentDescriptor> consumedPartitionDescriptor =
                producer.getResultPartitionDeploymentDescriptor(consumedPartition.getPartitionId());

        ResultPartitionID consumedPartitionId =
                new ResultPartitionID(consumedPartition.getPartitionId(), producer.getAttemptId());

        return getConsumedPartitionShuffleDescriptor(
                consumedPartitionId,
                consumedPartition.getResultType(),
                consumedPartition.isConsumable(),
                producerState,
                partitionDeploymentConstraint,
                consumedPartitionDescriptor.orElse(null));
    }

    @VisibleForTesting
    static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            ResultPartitionID consumedPartitionId,
            ResultPartitionType resultPartitionType,
            boolean isConsumable,
            ExecutionState producerState,
            PartitionLocationConstraint partitionDeploymentConstraint,
            @Nullable ResultPartitionDeploymentDescriptor consumedPartitionDescriptor) {
        // The producing task needs to be RUNNING or already FINISHED
        if ((resultPartitionType.isPipelined() || isConsumable)
                && consumedPartitionDescriptor != null
                && isProducerAvailable(producerState)) {
            // partition is already registered
            return consumedPartitionDescriptor.getShuffleDescriptor();
        } else if (partitionDeploymentConstraint == PartitionLocationConstraint.CAN_BE_UNKNOWN) {
            // The producing task might not have registered the partition yet
            return new UnknownShuffleDescriptor(consumedPartitionId);
        } else {
            // throw respective exceptions
            throw handleConsumedPartitionShuffleDescriptorErrors(
                    consumedPartitionId, resultPartitionType, isConsumable, producerState);
        }
    }

    private static RuntimeException handleConsumedPartitionShuffleDescriptorErrors(
            ResultPartitionID consumedPartitionId,
            ResultPartitionType resultPartitionType,
            boolean isConsumable,
            ExecutionState producerState) {
        String msg;
        if (isProducerFailedOrCanceled(producerState)) {
            msg =
                    "Trying to consume an input partition whose producer has been canceled or failed. "
                            + "The producer is in state "
                            + producerState
                            + ".";
        } else {
            msg =
                    String.format(
                            "Trying to consume an input partition whose producer "
                                    + "is not ready (result type: %s, partition consumable: %s, producer state: %s, partition id: %s).",
                            resultPartitionType, isConsumable, producerState, consumedPartitionId);
        }
        return new IllegalStateException(msg);
    }

    private static boolean isProducerAvailable(ExecutionState producerState) {
        return producerState == ExecutionState.RUNNING
                || producerState == ExecutionState.FINISHED
                || producerState == ExecutionState.SCHEDULED
                || producerState == ExecutionState.DEPLOYING;
    }

    private static boolean isProducerFailedOrCanceled(ExecutionState producerState) {
        return producerState == ExecutionState.CANCELING
                || producerState == ExecutionState.CANCELED
                || producerState == ExecutionState.FAILED;
    }

    /**
     * Defines whether the partition's location must be known at deployment time or can be unknown
     * and, therefore, updated later.
     */
    public enum PartitionLocationConstraint {
        MUST_BE_KNOWN,
        CAN_BE_UNKNOWN;

        public static PartitionLocationConstraint fromJobType(JobType jobType) {
            switch (jobType) {
                case BATCH:
                    return CAN_BE_UNKNOWN;
                case STREAMING:
                    return MUST_BE_KNOWN;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unknown JobType %s. Cannot derive partition location constraint for it.",
                                    jobType));
            }
        }
    }
}
