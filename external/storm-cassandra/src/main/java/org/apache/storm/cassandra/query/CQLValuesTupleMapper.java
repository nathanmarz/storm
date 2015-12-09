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
import org.apache.storm.cassandra.query.selector.FieldSelector;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Default interface for mapping a {@link backtype.storm.tuple.ITuple} to Map of values of a CQL statement.
 *
 */
public interface CQLValuesTupleMapper extends Serializable {

    /**
     * Map the specified {@code tuple} to values of CQL statement(s).
     * @param tuple the incoming tuple to map.
     * @return values of CQL statement(s)
     */
    Map<String, Object> map(ITuple tuple);

    /**
     * Default {@link CQLValuesTupleMapper} implementation to get specifics tuple's fields.
     */
    public static class WithFieldTupleMapper implements CQLValuesTupleMapper {
        private List<FieldSelector> fields;

        public WithFieldTupleMapper(List<FieldSelector> fields) {
            this.fields = fields;
        }

        @Override
        public Map<String, Object> map(ITuple tuple) {
            Map<String, Object> ret = new LinkedHashMap<>();
            for(FieldSelector fs : fields)
                fs.selectAndPut(tuple, ret);
            return ret;
        }
    }

    /**
     * Default {@link CQLValuesTupleMapper} implementation to get all tuple's fields.
     */
    public static class AllTupleMapper implements CQLValuesTupleMapper {
        @Override
        public Map<String, Object> map(ITuple tuple) {
            Map<String, Object> ret = new LinkedHashMap<>();
            for(String name  : tuple.getFields())
                ret.put(name, tuple.getValueByField(name));
            return ret;
        }
    }

}
