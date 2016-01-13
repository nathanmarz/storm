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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.executor.impl;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.cassandra.ExecutionResultHandler;
import org.apache.storm.cassandra.executor.AsyncResultHandler;
import org.apache.storm.cassandra.executor.ExecutionResultCollector;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


public class BatchAsyncResultHandler implements AsyncResultHandler<List<Tuple>> {

    private ConcurrentLinkedQueue<ExecutionResultCollector> completed;

    private ExecutionResultHandler handler;

    /**
     * Creates a new {@link BatchAsyncResultHandler} instance.
     * @param handler
     */
    public BatchAsyncResultHandler(ExecutionResultHandler handler) {
        this.handler = handler;
        this.completed = new ConcurrentLinkedQueue<>();
    }

    /**
     * This method is responsible for failing specified inputs.
     *
     * The default method does no-operation.
     */
    public void failure(Throwable t, List<Tuple> input) {
        completed.offer(new ExecutionResultCollector.FailedCollector(input, t));
    }
    /**
     * This method is responsible for acknowledging specified inputs.
     *
     * The default method does no-operation.
     */
    public void success(List<Tuple> input) {
        completed.offer(new ExecutionResultCollector.SucceedCollector(input));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush(final OutputCollector collector) {
        ExecutionResultCollector poll;
        while( (poll = completed.poll()) != null ) {
            poll.handle(collector, handler);
        }
    }
}
