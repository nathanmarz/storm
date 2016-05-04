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
 * A function takes in a set of input fields and emits zero or more tuples as output. The fields of the output tuple
 * are appended to the original input tuple in the stream. If a function emits no tuples, the original input tuple is
 * filtered out. Otherwise, the input tuple is duplicated for each output tuple.
 *
 * For example, if you have the following function:
 *
 * ```java
 * public class MyFunction extends BaseFunction {
 *      public void execute(TridentTuple tuple, TridentCollector collector) {
 *      for(int i=0; i < tuple.getInteger(0); i++) {
 *          collector.emit(new Values(i));
 *      }
 *    }
 * }
 *
 * ```
 *
 * Now suppose you have a stream in the variable `mystream` with the fields `["a", "b", "c"]` with the following tuples:
 *
 * ```
 * [1, 2, 3]
 * [4, 1, 6]
 * [3, 0, 8]
 * ```
 * If you had the following code in your topology definition:
 *
 * ```java
 * mystream.each(new Fields("b"), new MyFunction(), new Fields("d")))
 * ```
 *
 * The resulting tuples would have the fields `["a", "b", "c", "d"]` and look like this:
 *
 * ```
 * [1, 2, 3, 0]
 * [1, 2, 3, 1]
 * [4, 1, 6, 0]
 * ```
 *
 * In this case, the parameter `new Fields("b")` tells Trident that you would like to select the field "b" as input
 * to the function, and that will be the only field in the Tuple passed to the `execute()` method. The value of "b" in
 * the first tuple (2) causes the for loop to execute twice, so 2 tuples are emitted. similarly the second tuple causes
 * one tuple to be emitted. For the third tuple, the value of 0 causes the `for` loop to be skipped, so nothing is
 * emitted and the incoming tuple is filtered out of the stream.
 *
 * ### Configuration
 * If your `Function` implementation has configuration requirements, you will typically want to extend
 * {@link org.apache.storm.trident.operation.BaseFunction} and override the
 * {@link org.apache.storm.trident.operation.Operation#prepare(Map, TridentOperationContext)} method to perform your custom
 * initialization.
 *
 * ### Performance Considerations
 * Because Trident Functions perform logic on individual tuples -- as opposed to batches -- it is advisable
 * to avoid expensive operations such as database operations in a Function, if possible. For data store interactions
 * it is better to use a {@link org.apache.storm.trident.state.State} or
 * {@link org.apache.storm.trident.state.QueryFunction} implementation since Trident states operate on batch partitions
 * and can perform bulk updates to a database.
 *
 *
 */
public interface Function extends EachOperation {
    /**
     * Performs the function logic on an individual tuple and emits 0 or more tuples.
     *
     * @param tuple The incoming tuple
     * @param collector A collector instance that can be used to emit tuples
     */
    void execute(TridentTuple tuple, TridentCollector collector);
}
