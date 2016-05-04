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
package org.apache.storm.hbase.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a list of HBase columns.
 *
 * There are two types of columns, <i>standard</i> and <i>counter</i>.
 *
 * Standard columns have <i>column family</i> (required), <i>qualifier</i> (optional),
 * <i>timestamp</i> (optional), and a <i>value</i> (optional) values.
 *
 * Counter columns have <i>column family</i> (required), <i>qualifier</i> (optional),
 * and an <i>increment</i> (optional, but recommended) values.
 *
 * Inserts/Updates can be added via the <code>addColumn()</code> and <code>addCounter()</code>
 * methods.
 *
 */
public class ColumnList {

    public static abstract class AbstractColumn {
        byte[] family, qualifier;

        AbstractColumn(byte[] family, byte[] qualifier){
            this.family = family;
            this.qualifier = qualifier;
        }

        public byte[] getFamily() {
            return family;
        }

        public byte[] getQualifier() {
            return qualifier;
        }

    }

    public static class Column extends AbstractColumn {
        byte[] value;
        long ts = -1;
        Column(byte[] family, byte[] qualifier, long ts, byte[] value){
            super(family, qualifier);
            this.value = value;
            this.ts = ts;
        }

        public byte[] getValue() {
            return value;
        }

        public long getTs() {
            return ts;
        }
    }

    public static class Counter extends AbstractColumn {
        long incr = 0;
        Counter(byte[] family, byte[] qualifier, long incr){
            super(family, qualifier);
            this.incr = incr;
        }

        public long getIncrement() {
            return incr;
        }
    }


    private ArrayList<Column> columns;
    private ArrayList<Counter> counters;


    private ArrayList<Column> columns(){
        if(this.columns == null){
            this.columns = new ArrayList<Column>();
        }
        return this.columns;
    }

    private ArrayList<Counter> counters(){
        if(this.counters == null){
            this.counters = new ArrayList<Counter>();
        }
        return this.counters;
    }

    /**
     * Add a standard HBase column.
     *
     * @param family
     * @param qualifier
     * @param ts
     * @param value
     * @return
     */
    public ColumnList addColumn(byte[] family, byte[] qualifier, long ts, byte[] value){
        columns().add(new Column(family, qualifier, ts, value));
        return this;
    }

    /**
     * Add a standard HBase column
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public ColumnList addColumn(byte[] family, byte[] qualifier, byte[] value){
        columns().add(new Column(family, qualifier, -1, value));
        return this;
    }

    /**
     * Add a standard HBase column given an instance of a class that implements
     * the <code>IColumn</code> interface.
     * @param column
     * @return
     */
    public ColumnList addColumn(IColumn column){
        return this.addColumn(column.family(), column.qualifier(), column.timestamp(), column.value());
    }

    /**
     * Add an HBase counter column.
     *
     * @param family
     * @param qualifier
     * @param incr
     * @return
     */
    public ColumnList addCounter(byte[] family, byte[] qualifier, long incr){
        counters().add(new Counter(family, qualifier, incr));
        return this;
    }

    /**
     * Add an HBase counter column given an instance of a class that implements the
     * <code>ICounter</code> interface.
     * @param counter
     * @return
     */
    public ColumnList addCounter(ICounter counter){
        return this.addCounter(counter.family(), counter.qualifier(), counter.increment());
    }


    /**
     * Query to determine if we have column definitions.
     *
     * @return
     */
    public boolean hasColumns(){
        return this.columns != null;
    }

    /**
     * Query to determine if we have counter definitions.
     *
     * @return
     */
    public boolean hasCounters(){
        return this.counters != null;
    }

    /**
     * Get the list of column definitions.
     *
     * @return
     */
    public List<Column> getColumns(){
        return this.columns;
    }

    /**
     * Get the list of counter definitions.
     * @return
     */
    public List<Counter> getCounters(){
        return this.counters;
    }

}
