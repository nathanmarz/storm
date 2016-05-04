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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class Column<T> implements Serializable {

    private final String columnName;
    private final T val;

    public Column(String columnName, T val) {
        this.columnName = columnName;
        this.val = val;
    }

    public String getColumnName() {
        return columnName;
    }

    public T getVal() {
        return val;
    }

    public boolean isNull() { return val == null;}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Column column = (Column) o;

        if (columnName != null ? !columnName.equals(column.columnName) : column.columnName != null) return false;
        if (val != null ? !val.equals(column.val) : column.val != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = columnName != null ? columnName.hashCode() : 0;
        result = 31 * result + (val != null ? val.hashCode() : 0);
        return result;
    }

    public static Object[] getVals(List<Column> columns) {
        List<Object> vals = new ArrayList<>(columns.size());
        for(Column c : columns)
            vals.add(c.getVal());
        return vals.toArray();
    }
}
