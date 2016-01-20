/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.trident.operation;

import java.util.List;


/**
 * Interface for publishing tuples to a stream and reporting exceptions (to be displayed in Storm UI).
 *
 * Trident components that have the ability to emit tuples to a stream are passed an instance of this
 * interface.
 *
 * For example, to emit a new tuple to a stream, you would do something like the following:
 *
 * ```java
 *      collector.emit(new Values("a", "b", "c"));
 * ```
 * @see org.apache.storm.trident.Stream
 * @see org.apache.storm.tuple.Values
 */
public interface TridentCollector {
    /**
     * Emits a tuple to a Stream
     * @param values a list of values of which the tuple will be composed
     */
    void emit(List<Object> values);

    /**
     * Reports an error. The corresponding stack trace will be visible in the Storm UI.
     *
     * Note that calling this method does not alter the processing of a batch. To explicitly fail a batch and trigger
     * a replay, components should throw {@link org.apache.storm.topology.FailedException}.
     * @param t The instance of the error (Throwable) being reported.
     */
    void reportError(Throwable t);
}
