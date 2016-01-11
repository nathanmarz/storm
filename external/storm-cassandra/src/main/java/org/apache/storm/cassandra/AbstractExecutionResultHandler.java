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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Default interface to define strategies to apply when a query is either succeed or failed.
 *
 */
public abstract class AbstractExecutionResultHandler implements ExecutionResultHandler {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractExecutionResultHandler.class);

    @Override
    public void onThrowable(Throwable t, OutputCollector collector, Tuple i) {
        if( t instanceof QueryValidationException) {
            this.onQueryValidationException((QueryValidationException)t, collector, i);
        } else if (t instanceof ReadTimeoutException) {
            this.onReadTimeoutException((ReadTimeoutException)t, collector, i);
        } else if (t instanceof WriteTimeoutException) {
            this.onWriteTimeoutException((WriteTimeoutException) t, collector, i);
        } else if (t instanceof UnavailableException) {
            this.onUnavailableException((UnavailableException) t, collector, i);
        } else {
            collector.reportError(t);
            collector.fail(i);
        }
    }

    @Override
    public void onThrowable(Throwable t, OutputCollector collector, List<Tuple> tl) {
        for(Tuple i : tl) onThrowable(t, collector, i);
    }
}
