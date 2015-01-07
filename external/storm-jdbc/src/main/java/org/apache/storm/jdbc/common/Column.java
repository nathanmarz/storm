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
package org.apache.storm.jdbc.common;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

public class Column<T> implements Serializable {

    private String columnName;
    private T val;
    private int sqlType;

    public Column(String columnName, T val, int sqlType) {
        this.columnName = columnName;
        this.val = val;
        this.sqlType = sqlType;
    }

    public Column(String columnName, int sqlType) {
        this.columnName = columnName;
        this.sqlType = sqlType;
    }

    public String getColumnName() {
        return columnName;
    }

    public T getVal() {
        return val;
    }

    public int getSqlType() {
        return sqlType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Column)) return false;

        Column column = (Column) o;

        if (sqlType != column.sqlType) return false;
        if (!columnName.equals(column.columnName)) return false;
        if (!val.equals(column.val)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + val.hashCode();
        result = 31 * result + sqlType;
        return result;
    }

    @Override
    public String toString() {
        return "Column{" +
                "columnName='" + columnName + '\'' +
                ", val=" + val +
                ", sqlType=" + sqlType +
                '}';
    }
}
