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
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

import java.io.Serializable;
import java.util.List;

/**
 * Default interface to define strategies to apply when a query is either succeed or failed.
 *
 */
public interface ExecutionResultHandler extends Serializable {

    /**
     * Invoked when a {@link com.datastax.driver.core.exceptions.QueryValidationException} is thrown.
     *
     * @param e the cassandra exception.
     * @param collector the storm collector.
     * @param tuple an input tuple.
     */
    void onQueryValidationException(QueryValidationException e, OutputCollector collector, Tuple tuple);
    /**
     * Invoked when a {@link com.datastax.driver.core.exceptions.ReadTimeoutException} is thrown.
     *
     * @param e the cassandra exception.
     * @param collector the storm collector.
     * @param tuple an input tuple.
     */
    void onReadTimeoutException(ReadTimeoutException e, OutputCollector collector, Tuple tuple);

    /**
     * Invoked when a {@link com.datastax.driver.core.exceptions.WriteTimeoutException} is thrown.
     *
     * @param e the cassandra exception.
     * @param collector the storm collector.
     * @param tuple an input tuple.
     */
    void onWriteTimeoutException(WriteTimeoutException e, OutputCollector collector, Tuple tuple);
    /**
     * Invoked when a {@link com.datastax.driver.core.exceptions.UnavailableException} is thrown.
     *
     *
     * @param e the cassandra exception.
     * @param collector the storm collector.
     * @param tuple an input tuple.
     */
    void onUnavailableException(UnavailableException e, OutputCollector collector, Tuple tuple);
    /**
     * Invoked when a query is executed with success.
     * This method is NOT responsible for acknowledging input tuple.
     *
     * @param collector the storm collector.
     * @param tuple an input tuple.
     */
    void onQuerySuccess(OutputCollector collector, Tuple tuple);

    /**
     * Default method used to handle any type of exception.
     *
     * @param t the thrown exception
     * @param collector the storm collector.
     * @param i an input tuple.
     */
    void onThrowable(Throwable t, OutputCollector collector, Tuple i);

    /**
     * Default method used to handle any type of exception.
     *
     * @param t the thrown exception
     * @param collector the storm collector.
     * @param tl a list of input tuple.
     */
    void onThrowable(Throwable t, OutputCollector collector, List<Tuple> tl);

}
