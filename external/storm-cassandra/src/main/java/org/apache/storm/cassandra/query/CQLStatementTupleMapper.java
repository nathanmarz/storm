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
package org.apache.storm.cassandra.query;

import backtype.storm.tuple.ITuple;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.base.Optional;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

/**
 * Default interface to map a {@link backtype.storm.tuple.ITuple} to a CQL {@link com.datastax.driver.core.Statement}.
 *
 */
public interface CQLStatementTupleMapper extends Serializable {

    public static final String FIELD_KEYSPACE =  "keyspace";
    public static final String FIELD_TABLE    =  "table";
    public static final String FIELD_VALUES   =  "value";

    /**
     * Maps a given tuple to one or multiple CQL statements.
     *
     * @param conf the storm configuration map.
     * @param session the cassandra session.
     * @param tuple the incoming tuple to map.
     * @return a list of {@link com.datastax.driver.core.Statement}.
     */
    List<Statement> map(Map conf, Session session, ITuple tuple);

    public static class InsertCQLStatementTupleMapper implements CQLStatementTupleMapper {
        @Override
        public List<Statement> map(Map conf, Session session, ITuple tuple) {
            Optional<String> ks = Optional.fromNullable(tuple.contains(FIELD_KEYSPACE) ? tuple.getStringByField(FIELD_KEYSPACE) : null);
            String table = tuple.getStringByField(FIELD_TABLE);
            Map<String, Object> values = (Map<String, Object>) tuple.getValueByField(FIELD_VALUES);

            final Insert stmt = (ks.isPresent()) ? insertInto(ks.get(), table) : insertInto(table);
            for(Map.Entry<String, Object> v : values.entrySet())
                stmt.value(v.getKey(), v.getValue());

            return Arrays.asList((Statement)stmt);
        }
    }

    public static class DynamicCQLStatementTupleMapper implements CQLStatementTupleMapper {
        private List<CQLStatementBuilder> builders;

        public DynamicCQLStatementTupleMapper(List<CQLStatementBuilder> builders) {
            this.builders = builders;
        }

        @Override
        public List<Statement> map(Map conf, Session session, ITuple tuple) {
            List<Statement> statements = new LinkedList<>();
            for(CQLStatementBuilder b : builders) {
                statements.addAll(b.build().map(conf, session, tuple));
            }
            return statements;
        }
    }
}
