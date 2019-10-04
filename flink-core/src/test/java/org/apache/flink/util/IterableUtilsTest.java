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

package org.apache.flink.util;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link IterableUtils}.
 */
public class IterableUtilsTest extends TestLogger {

	private final Iterable<Integer> testIterable = Arrays.asList(1, 8, 5, 3, 8);

	@Test
	public void testToStream() {
		Deque<Integer> deque = new ArrayDeque<>();
		testIterable.forEach(deque::add);

		Stream<Integer> stream = IterableUtils.toStream(testIterable);
		stream.forEach(deque.poll()::equals);
	}

	@Test
	public void testContains() {
		assertTrue(IterableUtils.contains(testIterable, 1));
		assertTrue(IterableUtils.contains(testIterable, 8));
		assertTrue(IterableUtils.contains(testIterable, 5));
		assertTrue(IterableUtils.contains(testIterable, 3));

		assertFalse(IterableUtils.contains(testIterable, 6));
	}
}
