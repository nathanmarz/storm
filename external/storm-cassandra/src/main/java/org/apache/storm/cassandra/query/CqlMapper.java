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

import org.apache.storm.tuple.ITuple;
import org.apache.storm.cassandra.query.selector.FieldSelector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Default interface to defines how a storm tuple maps to a list of columns representing a row in a database.
 */
public interface CqlMapper extends Serializable {

    /**
     * Maps the specified input tuple to a list of CQL columns.
     * @param tuple the input tuple.
     * @return the list of
     */
    List<Column> map(ITuple tuple);


    public static final class SelectableCqlMapper implements CqlMapper {

        private final List<FieldSelector> selectors;

        /**
         * Creates a new {@link org.apache.storm.cassandra.query.CqlMapper.DefaultCqlMapper} instance.
         * @param selectors list of selectors used to extract column values from tuple.
         */
        public SelectableCqlMapper(List<FieldSelector> selectors) {
            this.selectors = selectors;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<Column> map(ITuple tuple) {
            List<Column> columns = new ArrayList<>(selectors.size());
            for(FieldSelector selector : selectors)
                columns.add(selector.select(tuple));
            return columns;
        }
    }

    /**
     * Default {@link CqlMapper} to map all tuple values to column.
     */
    public static final class DefaultCqlMapper implements CqlMapper {

        /**
         * Creates a new {@link org.apache.storm.cassandra.query.CqlMapper.DefaultCqlMapper} instance.
         */
        public DefaultCqlMapper() {}

        /**
         * {@inheritDoc}
         */
        @Override
        public List<Column> map(ITuple tuple) {
            List<Column> columns = new ArrayList<>(tuple.size());
            for(String name  : tuple.getFields())
                columns.add(new Column(name, tuple.getValueByField(name)));
            return columns;
        }
    }
}
