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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.jdbc.mapper;

import org.apache.storm.tuple.ITuple;
import org.apache.commons.lang.Validate;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.common.Util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleJdbcMapper implements JdbcMapper {

    private List<Column> schemaColumns;

    public SimpleJdbcMapper(String tableName, ConnectionProvider connectionProvider) {
        Validate.notEmpty(tableName);
        Validate.notNull(connectionProvider);

        int queryTimeoutSecs = 30;
        connectionProvider.prepare();
        JdbcClient client = new JdbcClient(connectionProvider, queryTimeoutSecs);
        this.schemaColumns = client.getColumnSchema(tableName);
    }

    public SimpleJdbcMapper(List<Column> schemaColumns) {
        Validate.notEmpty(schemaColumns);
        this.schemaColumns = schemaColumns;
    }

    @Override
    public List<Column> getColumns(ITuple tuple) {
        List<Column> columns = new ArrayList<Column>();
        for(Column column : schemaColumns) {
            String columnName = column.getColumnName();
            Integer columnSqlType = column.getSqlType();

            if(Util.getJavaType(columnSqlType).equals(String.class)) {
                String value = tuple.getStringByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Short.class)) {
                Short value = tuple.getShortByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Integer.class)) {
                Integer value = tuple.getIntegerByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Long.class)) {
                Long value = tuple.getLongByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Double.class)) {
                Double value = tuple.getDoubleByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Float.class)) {
                Float value = tuple.getFloatByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Boolean.class)) {
                Boolean value = tuple.getBooleanByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(byte[].class)) {
                byte[] value = tuple.getBinaryByField(columnName);
                columns.add(new Column(columnName, value, columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Date.class)) {
                Long value = tuple.getLongByField(columnName);
                columns.add(new Column(columnName, new Date(value), columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Time.class)) {
                Long value = tuple.getLongByField(columnName);
                columns.add(new Column(columnName, new Time(value), columnSqlType));
            } else if(Util.getJavaType(columnSqlType).equals(Timestamp.class)) {
                Long value = tuple.getLongByField(columnName);
                columns.add(new Column(columnName, new Timestamp(value), columnSqlType));
            } else {
                throw new RuntimeException("Unsupported java type in tuple " + Util.getJavaType(columnSqlType));
            }
        }
        return columns;
    }
}
