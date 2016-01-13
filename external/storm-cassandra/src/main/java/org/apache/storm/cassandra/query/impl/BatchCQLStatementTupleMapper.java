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
package org.apache.storm.cassandra.query.impl;

import org.apache.storm.tuple.ITuple;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class BatchCQLStatementTupleMapper implements CQLStatementTupleMapper {

    private final List<CQLStatementTupleMapper> mappers;
    private final BatchStatement.Type type;

    /**
     * Creates a new {@link BatchCQLStatementTupleMapper} instance.
     * @param type
     * @param mappers
     */
    public BatchCQLStatementTupleMapper(BatchStatement.Type type, List<CQLStatementTupleMapper> mappers) {
        this.mappers = new ArrayList<>(mappers);
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Statement> map(Map conf, Session session, ITuple tuple) {
        final BatchStatement batch = new BatchStatement(this.type);
        for(CQLStatementTupleMapper m : mappers)
            batch.addAll(m.map(conf, session, tuple));
        return Arrays.asList((Statement)batch);
    }
}
