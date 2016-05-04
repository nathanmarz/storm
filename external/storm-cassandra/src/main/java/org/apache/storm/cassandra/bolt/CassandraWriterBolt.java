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
package org.apache.storm.cassandra.bolt;

import org.apache.storm.tuple.Tuple;
import com.datastax.driver.core.Statement;
import org.apache.storm.cassandra.executor.AsyncResultHandler;
import org.apache.storm.cassandra.executor.impl.SingleAsyncResultHandler;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;

import java.util.List;

public class CassandraWriterBolt extends BaseCassandraBolt<Tuple> {

    private AsyncResultHandler<Tuple> asyncResultHandler;

    /**
     * Creates a new {@link CassandraWriterBolt} instance.
     *
     * @param tupleMapper
     */
    public CassandraWriterBolt(CQLStatementTupleMapper tupleMapper) {
        super(tupleMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AsyncResultHandler<Tuple> getAsyncHandler() {
        if( asyncResultHandler == null) {
            asyncResultHandler = new SingleAsyncResultHandler(getResultHandler());
        }
        return asyncResultHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void process(Tuple input) {
        List<Statement> statements = getMapper().map(stormConfig, session, input);
        if (statements.size() == 1) getAsyncExecutor().execAsync(statements.get(0), input);
        else getAsyncExecutor().execAsync(statements, input);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void onTickTuple() {
        /** do nothing **/
    }
}



