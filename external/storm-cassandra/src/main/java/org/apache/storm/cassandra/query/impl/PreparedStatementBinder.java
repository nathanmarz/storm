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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import org.apache.storm.cassandra.query.Column;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
public interface PreparedStatementBinder extends Serializable {

    public BoundStatement apply(PreparedStatement statement, List<Column> columns);

    public static final class DefaultBinder implements PreparedStatementBinder {

        /**
         * {@inheritDoc}
         */
        @Override
        public BoundStatement apply(PreparedStatement statement, List<Column> columns) {
            Object[] values = Column.getVals(columns);
            return statement.bind(values);
        }
    }

    public static final class CQL3NamedSettersBinder implements PreparedStatementBinder {

        /**
         * {@inheritDoc}
         */
        @Override
        public BoundStatement apply(PreparedStatement statement, List<Column> columns) {
            Object[] values = Column.getVals(columns);

            BoundStatement boundStatement = statement.bind();
            for(Column col : columns) {
                // For native protocol V3 or below, all variables must be bound.
                // With native protocol V4 or above, variables can be left unset,
                // in which case they will be ignored server side (no tombstones will be generated).
                if(col.isNull()) {
                    boundStatement.setToNull(col.getColumnName());
                } else {
                    bind(boundStatement, col.getColumnName(), col.getVal());
                }
            }
            return statement.bind(values);
        }

        /**
         * This ugly method comes from {@link com.datastax.driver.core.TypeCodec#getDataTypeFor(Object)}.
         */
        static void bind(BoundStatement statement, String name, Object value) {
            // Starts with ByteBuffer, so that if already serialized value are provided, we don't have the
            // cost of testing a bunch of other types first
            if (value instanceof ByteBuffer)
                statement.setBytes(name, (ByteBuffer)value);

            if (value instanceof Number) {
                if (value instanceof Integer)
                    statement.setInt(name, (Integer)value);
                if (value instanceof Long)
                    statement.setLong(name, (Long) value);
                if (value instanceof Float)
                    statement.setFloat(name, (Float) value);
                if (value instanceof Double)
                    statement.setDouble(name, (Double)value);
                if (value instanceof BigDecimal)
                    statement.setDecimal(name, (BigDecimal)value);
                if (value instanceof BigInteger)
                    statement.setVarint(name, (BigInteger)value);
                throw new IllegalArgumentException(String.format("Value of type %s does not correspond to any CQL3 type", value.getClass()));
            }

            if (value instanceof String)
                statement.setString(name, (String)value);

            if (value instanceof Boolean)
                statement.setBool(name, (Boolean)value);

            if (value instanceof InetAddress)
                statement.setInet(name, (InetAddress)value);

            if (value instanceof Date)
                statement.setDate(name, (Date)value);

            if (value instanceof UUID)
                statement.setUUID(name, (UUID)value);

            if (value instanceof List) {
                statement.setList(name, (List)value);
            }

            if (value instanceof Set) {
                statement.setSet(name, (Set)value);
            }

            if (value instanceof Map) {
                statement.setMap(name, (Map) value);
            }

            if (value instanceof UDTValue) {
                statement.setUDTValue(name, (UDTValue)value);
            }

            if (value instanceof TupleValue) {
                statement.setTupleValue(name, (TupleValue) value);
            }

            throw new IllegalArgumentException(String.format("Value of type %s does not correspond to any CQL3 type", value.getClass()));
        }
    }
}
