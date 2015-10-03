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

/**
 * A database table can be defined as a list of rows and each row can be defined as a list of columns where
 * each column instance has a name, a value and a type. This class represents an instance of a column in a database
 * row. For example if we have the following table named user:
 * <pre>
 *  ____________________________
 * |    UserId  |   UserName    |
 * |      1     |    Foo        |
 * |      2     |    Bar        |
 *  ----------------------------
 * </pre>
 *
 * The following class can be used to represent the data in the table as
 * <pre>
 * List<List<Column>> rows = new ArrayList<List<Column>>();
 * List<Column> row1 = Lists.newArrayList(new Column("UserId", 1, Types.INTEGER), new Column("UserName", "Foo", Types.VARCHAR))
 * List<Column> row1 = Lists.newArrayList(new Column("UserId", 2, Types.INTEGER), new Column("UserName", "Bar", Types.VARCHAR))
 *
 * rows.add(row1)
 * rows.add(row2)
 *
 * </pre>
 *
 * @param <T>
 */
public class Column<T> implements Serializable {

    private String columnName;
    private T val;

    /**
     * The sql type(e.g. varchar, date, int) Ideally we would have an enum but java's jdbc API uses integer.
     * See {@link java.sql.Types}
     */
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

        Column<?> column = (Column<?>) o;

        if (sqlType != column.sqlType) return false;
        if (!columnName.equals(column.columnName)) return false;
        return val != null ? val.equals(column.val) : column.val == null;

    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + (val != null ? val.hashCode() : 0);
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
