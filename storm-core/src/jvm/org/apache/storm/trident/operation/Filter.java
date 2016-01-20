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

import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Filters take in a tuple as input and decide whether or not to keep that tuple or not.
 *
 * If the `isKeep()` method of a Filter returns `false` for a tuple, that tuple will be filtered out of the Stream
 *
 *
 * ### Configuration
 * If your `Filter` implementation has configuration requirements, you will typically want to extend
 * {@link org.apache.storm.trident.operation.BaseFilter} and override the
 * {@link org.apache.storm.trident.operation.Operation#prepare(Map, TridentOperationContext)} method to perform your custom
 * initialization.

 *
 * @see org.apache.storm.trident.Stream
 */
public interface Filter extends EachOperation {

    /**
     * Determines if a tuple should be filtered out of a stream
     *
     * @param tuple the tuple being evaluated
     * @return `false` to drop the tuple, `true` to keep the tuple
     */
    boolean isKeep(TridentTuple tuple);
}
