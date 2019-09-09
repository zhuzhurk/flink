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

import java.util.function.BiFunction;

/**
 * @see java.util.concurrent.CompletableFuture#handle(BiFunction)
 */
class ConditionalFutureHandler<T> implements BiFunction<T, Throwable, Void> {

	private final ExecutionVertexVersioner executionVertexVersioner;

	private final ExecutionVertexVersion executionVertexVersion;

	private final BiFunction<T, Throwable, Void> delegate;

	public ConditionalFutureHandler(
		final ExecutionVertexVersioner executionVertexVersioner,
		final ExecutionVertexVersion executionVertexVersion,
		final BiFunction<T, Throwable, Void> delegate) {

		this.executionVertexVersioner = executionVertexVersioner;
		this.executionVertexVersion = executionVertexVersion;
		this.delegate = delegate;
	}

	@Override
	public Void apply(final T t, final Throwable throwable) {
		if (executionVertexVersioner.isModified(executionVertexVersion)) {
			return null;
		}
		return delegate.apply(t, throwable);
	}

}
