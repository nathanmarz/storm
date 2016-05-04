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
package org.apache.storm.cassandra;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import com.datastax.driver.core.exceptions.*;
import org.slf4j.LoggerFactory;

/**
 * Simple {@link ExecutionResultHandler} which fail the incoming tuple when an {@link com.datastax.driver.core.exceptions.DriverException} is thrown.
 * The exception is then automatically report to storm.
 *
 */
public class BaseExecutionResultHandler extends AbstractExecutionResultHandler {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(BaseExecutionResultHandler.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void onQueryValidationException(QueryValidationException e, OutputCollector collector, Tuple tuple) {
        onDriverException(e, collector, tuple);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void onReadTimeoutException(ReadTimeoutException e, OutputCollector collector, Tuple tuple) {
        onDriverException(e, collector, tuple);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void onWriteTimeoutException(WriteTimeoutException e, OutputCollector collector, Tuple tuple) {
        onDriverException(e, collector, tuple);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void onUnavailableException(UnavailableException e, OutputCollector collector, Tuple tuple) {
        onDriverException(e, collector, tuple);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void onQuerySuccess(OutputCollector collector, Tuple tuple) {

    }

    /**
     * This method is called when an one of the methods of the {@link BaseExecutionResultHandler} is not
     * overridden. It can be practical if you want to bundle some/all of the methods to a single method.
     *
     * @param e the exception throws
     * @param collector the output collector
     * @param tuple the tuple in failure
     */
    protected void onDriverException(DriverException e, OutputCollector collector, Tuple tuple) {
        LOG.error("An error occurred while executing cassandra statement", e);
        collector.fail(tuple);
        collector.reportError(e);
    }

}
