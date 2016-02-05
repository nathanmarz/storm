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
package org.apache.storm.cassandra.executor;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import com.google.common.collect.Lists;
import org.apache.storm.cassandra.ExecutionResultHandler;

import java.util.List;

/**
 * This class is responsible to collect input tuples proceed.
 */
public interface ExecutionResultCollector {

    void handle(OutputCollector collector, ExecutionResultHandler handler);

    public static final class SucceedCollector implements ExecutionResultCollector {

        private final List<Tuple> inputs;

        /**
         * Creates a new {@link ExecutionResultCollector} instance.
         * @param input the input tuple.
         */
        public SucceedCollector(Tuple input) {
           this(Lists.newArrayList(input));
        }

        /**
         * Creates a new {@link ExecutionResultCollector} instance.
         * @param inputs the input tuple.
         */
        public SucceedCollector(List<Tuple> inputs) {
            this.inputs = inputs;
        }

        /**
         * Calls {@link ExecutionResultHandler#onQuerySuccess(org.apache.storm.task.OutputCollector, org.apache.storm.tuple.Tuple)} before
         * acknowledging an single input tuple.
         */
        @Override
        public void handle(OutputCollector collector, ExecutionResultHandler handler) {
            for(Tuple t : inputs) handler.onQuerySuccess(collector, t);
            for(Tuple t : inputs) collector.ack(t);
        }
    }

    public static final class FailedCollector implements ExecutionResultCollector {

        private final Throwable cause;
        private final List<Tuple> inputs;

        /**
         * Creates a new {@link FailedCollector} instance.
         * @param input the input tuple.
         * @param cause the cause of the error.
         */
        public FailedCollector(Tuple input, Throwable cause) {
            this(Lists.newArrayList(input), cause);
        }

        /**
         * Creates a new {@link FailedCollector} instance.
         * @param inputs the input tuple.
         * @param cause the cause of the error.
         */
        public FailedCollector(List<Tuple> inputs, Throwable cause) {
            this.inputs = inputs;
            this.cause = cause;
        }

        /**
         * Calls {@link ExecutionResultHandler#onThrowable(Throwable, org.apache.storm.task.OutputCollector, org.apache.storm.tuple.Tuple)} .
         */
        @Override
        public void handle(OutputCollector collector, ExecutionResultHandler handler) {
            handler.onThrowable(cause, collector, inputs);
        }
    }

}
