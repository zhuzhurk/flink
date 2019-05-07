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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This exception is wrapped on downstream side during consuming producer's
 * partition data. And it is reported to job master as a hint to restart the
 * upstream to re-produce the data.
 */
@ThrowableAnnotation(ThrowableType.PartitionDataMissingError)
public class DataConsumptionException extends IOException {

	private static final long serialVersionUID = 0L;

	private final ResultPartitionID partitionId;

	public DataConsumptionException(ResultPartitionID partitionId, Throwable cause) {
		super(cause);

		this.partitionId = checkNotNull(partitionId);
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public String getMessage() {
		return "Partition " + partitionId + " consumption exception.";
	}
}
